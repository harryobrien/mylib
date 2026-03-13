#!/usr/bin/env python3
"""
Open Library dump loader.

Imports Open Library data dumps into PostgreSQL using COPY for bulk loading.

Usage:
    python loader.py --authors FILE --works FILE --editions FILE
    python loader.py --cover-metadata FILE
    python loader.py --rebuild-indexes
"""

import argparse
import gzip
import json
import os
import re
import sys
import threading
from pathlib import Path
from queue import Queue
from typing import Callable, Iterator

import psycopg
from psycopg import sql

# --- Configuration ---

DB_CONFIG = {
    "host": os.getenv("PGHOST", "localhost"),
    "port": int(os.getenv("PGPORT", 5432)),
    "dbname": os.getenv("PGDATABASE", "mylib"),
    "user": os.getenv("PGUSER", "mylib"),
    "password": os.getenv("PGPASSWORD", "mylib"),
}

BATCH_SIZE = 10000
NUM_WRITERS = 4

# --- Helpers ---

def extract_text_block(obj) -> str | None:
    if obj is None:
        return None
    if isinstance(obj, str):
        return obj
    if isinstance(obj, dict):
        return obj.get("value")
    return None


def extract_datetime(obj) -> str | None:
    if obj is None:
        return None
    if isinstance(obj, dict):
        return obj.get("value")
    return str(obj)


def normalize_isbn(raw: str) -> str:
    return raw.replace("-", "").replace(" ", "").upper()


def truncate(val: str | None, maxlen: int) -> str | None:
    if val is None:
        return None
    return val[:maxlen] if len(val) > maxlen else val


_COUNTRY_RE = re.compile(r'^[a-z]{2,3}$')

def clean_country(val: str | None) -> str | None:
    if not val:
        return None
    cleaned = val.strip().lower()[:3]
    return cleaned if _COUNTRY_RE.match(cleaned) else None


def extract_lang_code(lang_obj: dict) -> str | None:
    if not lang_obj:
        return None
    key = lang_obj.get("key", "")
    if key.startswith("/languages/"):
        return key[11:]
    return None


def parse_dump_line(line: str) -> tuple[str, str, int, str, dict] | None:
    parts = line.rstrip("\n").split("\t")
    if len(parts) != 5:
        return None
    try:
        return (parts[0], parts[1], int(parts[2]), parts[3], json.loads(parts[4]))
    except (json.JSONDecodeError, ValueError):
        return None


def stream_dump(filepath: Path) -> Iterator[tuple[str, str, int, str, dict]]:
    opener = gzip.open if filepath.suffix == ".gz" else open
    with opener(filepath, "rt", encoding="utf-8") as f:
        for line in f:
            parsed = parse_dump_line(line)
            if parsed:
                yield parsed

class ThreadedWriter:
    """Manages a pool of writer threads for parallel DB writes."""

    def __init__(self, flush_fn: Callable, num_writers: int = NUM_WRITERS):
        self.flush_fn = flush_fn
        self.queue: Queue = Queue(maxsize=num_writers * 2)
        self.error: Exception | None = None
        self.lock = threading.Lock()
        self.threads = [
            threading.Thread(target=self._worker, daemon=True)
            for _ in range(num_writers)
        ]
        for t in self.threads:
            t.start()

    def _worker(self):
        conn = psycopg.connect(**DB_CONFIG)
        try:
            while True:
                batch = self.queue.get()
                if batch is None:
                    self.queue.put(None)
                    break
                if self.error:
                    break
                try:
                    self.flush_fn(conn, batch)
                except Exception as e:
                    with self.lock:
                        if self.error is None:
                            self.error = e
                    break
        finally:
            conn.close()

    def submit(self, batch):
        if self.error:
            raise self.error
        self.queue.put(batch)

    def shutdown(self):
        self.queue.put(None)
        for t in self.threads:
            t.join()
        if self.error:
            raise self.error


# --- Generic Loader ---

def load_entity(
    conn: psycopg.Connection,
    filepath: Path,
    entity_type: str,
    type_key: str,
    parse_fn: Callable,
    flush_fn: Callable,
    preload_fn: Callable | None = None,
):
    """Generic loader for authors/works/editions."""
    print(f"Loading {entity_type} from {filepath}...")

    context = preload_fn(conn) if preload_fn else {}

    table = entity_type
    with conn.cursor() as cur:
        cur.execute(f"SELECT key FROM {table}")
        existing_keys = {row[0] for row in cur.fetchall()}
    print(f"  Found {len(existing_keys):,} existing {entity_type} (will skip)")

    writer = ThreadedWriter(flush_fn)
    batch = []
    count = 0
    skipped = 0
    processed = 0

    for _, key, revision, last_mod, data in stream_dump(filepath):
        if data.get("type", {}).get("key") != type_key:
            continue

        processed += 1
        if processed % 100 == 0:
            print(f"  {processed:,} processed, {count:,} loaded...", end="\r")

        if key in existing_keys:
            continue

        record = parse_fn(key, revision, data, context)
        if record is None:
            skipped += 1
            continue

        batch.append(record)
        count += 1

        if count % BATCH_SIZE == 0:
            writer.submit(batch)
            batch = []

    if batch:
        writer.submit(batch)

    writer.shutdown()
    print(f"  Processed {processed:,}, loaded {count:,} {entity_type} (skipped {skipped:,} invalid)")


# --- Author Loading ---

def preload_authors(conn):
    return {}


def parse_author(key, revision, data, context):
    if not data.get("name"):
        return None

    author = (
        key,
        data.get("name"),
        data.get("fuller_name"),
        data.get("personal_name"),
        truncate(data.get("title"), 100),
        extract_text_block(data.get("bio")),
        truncate(data.get("birth_date"), 50),
        truncate(data.get("death_date"), 50),
        truncate(data.get("date"), 50),
        data.get("entity_type") if data.get("entity_type") in ('person', 'org', 'event') else None,
        revision,
        data.get("latest_revision"),
        extract_datetime(data.get("created")),
        extract_datetime(data.get("last_modified")),
    )

    alt_names = set()
    for name in data.get("alternate_names", []):
        alt_names.add((key, name))

    photos = []
    for i, photo_id in enumerate(data.get("photos", [])):
        if isinstance(photo_id, (int, float)):
            photos.append((key, int(photo_id), i))

    links = {}
    for link in data.get("links", []):
        if link.get("url") and link.get("title"):
            url = truncate(link["url"], 2000)
            link_key = (key, url)
            if link_key not in links:
                links[link_key] = truncate(link["title"], 500)

    remote_ids = set()
    for source, identifier in data.get("remote_ids", {}).items():
        if identifier:
            remote_ids.add((key, source, str(identifier)))

    return (author, alt_names, photos, links, remote_ids)


def flush_authors(conn, batch):
    authors = [b[0] for b in batch]
    alt_names = set().union(*(b[1] for b in batch))
    photos = [p for b in batch for p in b[2]]
    links = {}
    for b in batch:
        links.update(b[3])
    remote_ids = set().union(*(b[4] for b in batch))

    with conn.cursor() as cur:
        with cur.copy("""
            COPY authors (key, name, fuller_name, personal_name, title, bio,
                         birth_date, death_date, date, entity_type,
                         revision, latest_revision, created_at, last_modified)
            FROM STDIN
        """) as copy:
            for row in authors:
                copy.write_row(row)

        if alt_names or photos or links or remote_ids:
            keys = [a[0] for a in authors]
            cur.execute("SELECT key, id FROM authors WHERE key = ANY(%s)", (keys,))
            key_to_id = dict(cur.fetchall())

            if alt_names:
                with cur.copy("COPY author_alternate_names (author_id, name) FROM STDIN") as copy:
                    for key, name in alt_names:
                        if key in key_to_id:
                            copy.write_row((key_to_id[key], name))

            if photos:
                with cur.copy("COPY author_photos (author_id, photo_id, position) FROM STDIN") as copy:
                    for key, photo_id, pos in photos:
                        if key in key_to_id:
                            copy.write_row((key_to_id[key], photo_id, pos))

            if links:
                with cur.copy("COPY author_links (author_id, url, title) FROM STDIN") as copy:
                    for (key, url), title in links.items():
                        if key in key_to_id:
                            copy.write_row((key_to_id[key], url, title))

            if remote_ids:
                with cur.copy("COPY author_remote_ids (author_id, source, identifier) FROM STDIN") as copy:
                    for key, source, ident in remote_ids:
                        if key in key_to_id:
                            copy.write_row((key_to_id[key], source, ident))

    conn.commit()


def load_authors(conn, filepath):
    load_entity(conn, filepath, "authors", "/type/author", parse_author, flush_authors, preload_authors)


# --- Works Loading ---

def preload_works(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT key, id FROM authors")
        author_map = dict(cur.fetchall())
    print(f"  Loaded {len(author_map):,} author mappings")
    return {"author_map": author_map}


def parse_work(key, revision, data, context):
    if not data.get("title"):
        return None

    author_map = context["author_map"]

    lc_class = data.get("lc_classifications")
    if lc_class:
        lc_class = [truncate(c, 50) for c in lc_class if c]

    work = (
        key,
        data.get("title"),
        data.get("subtitle"),
        truncate(data.get("first_publish_date"), 50),
        extract_text_block(data.get("description")),
        extract_text_block(data.get("notes")),
        revision,
        data.get("latest_revision"),
        extract_datetime(data.get("created")),
        extract_datetime(data.get("last_modified")),
        lc_class,
    )

    work_authors = []
    for i, author_role in enumerate(data.get("authors", [])):
        if not isinstance(author_role, dict):
            continue
        author_obj = author_role.get("author")
        if isinstance(author_obj, dict):
            author_key = author_obj.get("key")
        elif isinstance(author_obj, str):
            author_key = author_obj
        else:
            continue
        if author_key and author_key in author_map:
            work_authors.append((key, author_map[author_key], author_role.get("role"), author_role.get("as"), i))

    covers = []
    for i, cover_id in enumerate(data.get("covers", [])):
        if isinstance(cover_id, (int, float)):
            covers.append((key, int(cover_id), i))

    links = {}
    for link in data.get("links", []):
        if link.get("url") and link.get("title"):
            url = truncate(link["url"], 2000)
            link_key = (key, url)
            if link_key not in links:
                links[link_key] = truncate(link["title"], 500)

    subjects = set()
    for subject in data.get("subjects", []):
        if subject:
            subjects.add((key, truncate(subject, 500)))

    return (work, work_authors, covers, links, subjects)


def flush_works(conn, batch):
    works = [b[0] for b in batch]
    work_authors = [wa for b in batch for wa in b[1]]
    covers = [c for b in batch for c in b[2]]
    links = {}
    for b in batch:
        links.update(b[3])
    subjects = set().union(*(b[4] for b in batch))

    with conn.cursor() as cur:
        with cur.copy("""
            COPY works (key, title, subtitle, first_publish_date, description,
                       notes, revision, latest_revision, created_at, last_modified,
                       lc_classifications)
            FROM STDIN
        """) as copy:
            for row in works:
                copy.write_row(row)

        if work_authors or covers or links or subjects:
            keys = [w[0] for w in works]
            cur.execute("SELECT key, id FROM works WHERE key = ANY(%s)", (keys,))
            key_to_id = dict(cur.fetchall())

            if work_authors:
                with cur.copy("COPY work_authors (work_id, author_id, role, as_name, position) FROM STDIN") as copy:
                    for key, author_id, role, as_name, pos in work_authors:
                        if key in key_to_id:
                            copy.write_row((key_to_id[key], author_id, role, as_name, pos))

            if covers:
                with cur.copy("COPY work_covers (work_id, cover_id, position) FROM STDIN") as copy:
                    for key, cover_id, pos in covers:
                        if key in key_to_id:
                            copy.write_row((key_to_id[key], cover_id, pos))

            if links:
                with cur.copy("COPY work_links (work_id, url, title) FROM STDIN") as copy:
                    for (key, url), title in links.items():
                        if key in key_to_id:
                            copy.write_row((key_to_id[key], url, title))

            if subjects:
                with cur.copy("COPY work_subjects (work_id, subject) FROM STDIN") as copy:
                    for key, subject in subjects:
                        if key in key_to_id:
                            copy.write_row((key_to_id[key], subject))

    conn.commit()


def load_works(conn, filepath):
    load_entity(conn, filepath, "works", "/type/work", parse_work, flush_works, preload_works)


# --- Editions Loading ---

def preload_editions(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT key, id FROM works")
        work_map = dict(cur.fetchall())
        cur.execute("SELECT key, id FROM authors")
        author_map = dict(cur.fetchall())
        cur.execute("SELECT code FROM languages")
        languages = {row[0] for row in cur.fetchall()}
    print(f"  Loaded {len(work_map):,} work mappings")
    return {"work_map": work_map, "author_map": author_map, "languages": languages}


def parse_edition(key, revision, data, context):
    work_map = context["work_map"]
    author_map = context["author_map"]
    languages = context["languages"]

    works = data.get("works", [])
    if not works:
        return None
    work_key = works[0].get("key")
    if work_key not in work_map:
        return None
    if not data.get("title"):
        return None

    work_id = work_map[work_key]

    edition = (
        key, work_id, data.get("title"), data.get("subtitle"), data.get("ocaid"),
        data.get("weight"),
        (lambda p: p if p and p > 0 else None)(data.get("number_of_pages")),
        data.get("pagination"), data.get("physical_dimensions"), data.get("physical_format"),
        data.get("by_statement"), data.get("edition_name"), data.get("copyright_date"),
        clean_country(data.get("publish_country")), data.get("publish_date"),
        data.get("translation_of"), extract_text_block(data.get("description")),
        extract_text_block(data.get("first_sentence")), extract_text_block(data.get("notes")),
        revision, data.get("latest_revision"), extract_datetime(data.get("created")),
        extract_datetime(data.get("last_modified")), data.get("contributions"),
        [truncate(d, 30) for d in data.get("dewey_decimal_class", []) if d] or None,
        [truncate(c, 50) for c in data.get("lc_classifications", []) if c] or None,
        data.get("other_titles"), data.get("work_titles"), data.get("source_records"),
        data.get("local_id"),
        json.dumps(data.get("table_of_contents")) if data.get("table_of_contents") else None,
    )

    edition_authors = []
    for i, author_obj in enumerate(data.get("authors", [])):
        if not isinstance(author_obj, dict):
            continue
        author_key = author_obj.get("key")
        if author_key and author_key in author_map:
            edition_authors.append((key, author_map[author_key], i))

    langs = set()
    for lang_obj in data.get("languages", []):
        code = extract_lang_code(lang_obj)
        if code and code in languages:
            langs.add((key, code))

    translated_from = set()
    for lang_obj in data.get("translated_from", []):
        code = extract_lang_code(lang_obj)
        if code and code in languages:
            translated_from.add((key, code))

    isbns = {}
    for isbn in data.get("isbn_10", []):
        normalized = normalize_isbn(isbn)
        if len(normalized) == 10:
            isbns[(key, normalized)] = (10, isbn)
    for isbn in data.get("isbn_13", []):
        normalized = normalize_isbn(isbn)
        if len(normalized) == 13:
            isbns[(key, normalized)] = (13, isbn)

    lccn = set((key, l) for l in data.get("lccn", []))
    oclc = set((key, o) for o in data.get("oclc_numbers", []))

    identifiers = set()
    for source, ids in data.get("identifiers", {}).items():
        if isinstance(ids, list):
            for ident in ids:
                identifiers.add((key, source, str(ident)))

    covers = [(key, int(c), i) for i, c in enumerate(data.get("covers", [])) if isinstance(c, (int, float))]

    links = {}
    for link in data.get("links", []):
        if link.get("url") and link.get("title"):
            links[(key, link["url"])] = link["title"]

    publishers = set((key, p[:500]) for p in data.get("publishers", []) if p)
    places = set((key, p[:500]) for p in data.get("publish_places", []) if p)
    subjects = set((key, truncate(s, 500)) for s in data.get("subjects", []) if s)
    genres = set((key, g[:500]) for g in data.get("genres", []) if g)
    series = set((key, s[:500]) for s in data.get("series", []) if s)

    return (edition, edition_authors, langs, translated_from, isbns, lccn, oclc,
            identifiers, covers, links, publishers, places, subjects, genres, series)


def flush_editions(conn, batch):
    editions = [b[0] for b in batch]
    edition_authors = [ea for b in batch for ea in b[1]]
    langs = set().union(*(b[2] for b in batch))
    translated_from = set().union(*(b[3] for b in batch))
    isbns = {}
    for b in batch:
        isbns.update(b[4])
    lccn = set().union(*(b[5] for b in batch))
    oclc = set().union(*(b[6] for b in batch))
    identifiers = set().union(*(b[7] for b in batch))
    covers = [c for b in batch for c in b[8]]
    links = {}
    for b in batch:
        links.update(b[9])
    publishers = set().union(*(b[10] for b in batch))
    places = set().union(*(b[11] for b in batch))
    subjects = set().union(*(b[12] for b in batch))
    genres = set().union(*(b[13] for b in batch))
    series = set().union(*(b[14] for b in batch))

    with conn.cursor() as cur:
        with cur.copy("""
            COPY editions (key, work_id, title, subtitle, ocaid, weight,
                          number_of_pages, pagination, physical_dimensions,
                          physical_format, by_statement, edition_name,
                          copyright_date, publish_country, publish_date,
                          translation_of, description, first_sentence, notes,
                          revision, latest_revision, created_at, last_modified,
                          contributions, dewey_decimal_class, lc_classifications,
                          other_titles, work_titles, source_records, local_ids,
                          table_of_contents)
            FROM STDIN
        """) as copy:
            for row in editions:
                copy.write_row(row)

        keys = [e[0] for e in editions]
        cur.execute("SELECT key, id FROM editions WHERE key = ANY(%s)", (keys,))
        key_to_id = dict(cur.fetchall())

        def copy_tuples(table, columns, data):
            if not data:
                return
            col_str = ", ".join(columns)
            with cur.copy(f"COPY {table} ({col_str}) FROM STDIN") as copy:
                for row in data:
                    if row[0] in key_to_id:
                        copy.write_row((key_to_id[row[0]],) + row[1:])

        copy_tuples("edition_authors", ["edition_id", "author_id", "position"], edition_authors)
        copy_tuples("edition_languages", ["edition_id", "language_code"], langs)
        copy_tuples("edition_translated_from", ["edition_id", "language_code"], translated_from)

        if isbns:
            with cur.copy("COPY edition_isbns (edition_id, isbn, isbn_type, raw_isbn) FROM STDIN") as copy:
                for (key, isbn), (isbn_type, raw_isbn) in isbns.items():
                    if key in key_to_id:
                        copy.write_row((key_to_id[key], isbn, isbn_type, raw_isbn))

        copy_tuples("edition_lccn", ["edition_id", "lccn"], lccn)
        copy_tuples("edition_oclc", ["edition_id", "oclc_number"], oclc)
        copy_tuples("edition_identifiers", ["edition_id", "source", "identifier"], identifiers)
        copy_tuples("edition_covers", ["edition_id", "cover_id", "position"], covers)

        if links:
            with cur.copy("COPY edition_links (edition_id, url, title) FROM STDIN") as copy:
                for (key, url), title in links.items():
                    if key in key_to_id:
                        copy.write_row((key_to_id[key], url, title))

        copy_tuples("edition_publishers", ["edition_id", "publisher"], publishers)
        copy_tuples("edition_publish_places", ["edition_id", "place"], places)
        copy_tuples("edition_subjects", ["edition_id", "subject"], subjects)
        copy_tuples("edition_genres", ["edition_id", "genre"], genres)
        copy_tuples("edition_series", ["edition_id", "series"], series)

    conn.commit()


def load_editions(conn, filepath):
    load_entity(conn, filepath, "editions", "/type/edition", parse_edition, flush_editions, preload_editions)


# --- Cover Metadata Loading ---

def load_cover_metadata(conn: psycopg.Connection, filepath: Path):
    """Load cover metadata from tab-separated dump file (cover_id, width, height, date)."""
    print(f"Loading cover metadata from {filepath}...")

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM cover_metadata")
        existing = cur.fetchone()[0]
    if existing > 0:
        print(f"  Table already has {existing:,} rows, skipping")
        return

    opener = gzip.open if filepath.suffix == ".gz" else open
    batch = []
    count = 0

    with opener(filepath, "rt", encoding="utf-8") as f:
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) != 4:
                continue
            try:
                cover_id = int(parts[0])
                width = int(parts[1])
                height = int(parts[2])
                created_at = parts[3] if parts[3] else None
                batch.append((cover_id, width, height, created_at))
                count += 1

                if count % 100000 == 0:
                    print(f"  {count:,} loaded...", end="\r")

                if len(batch) >= BATCH_SIZE:
                    with conn.cursor() as cur:
                        with cur.copy("COPY cover_metadata (id, width, height, created_at) FROM STDIN") as copy:
                            for row in batch:
                                copy.write_row(row)
                    conn.commit()
                    batch = []
            except ValueError:
                continue

    if batch:
        with conn.cursor() as cur:
            with cur.copy("COPY cover_metadata (id, width, height, created_at) FROM STDIN") as copy:
                for row in batch:
                    copy.write_row(row)
        conn.commit()

    print(f"  Loaded {count:,} cover metadata records")


# --- Index Management ---

INDEXES = [
    # Authors
    "CREATE INDEX idx_authors_name ON authors(name)",
    "CREATE INDEX idx_authors_last_modified ON authors(last_modified DESC)",
    "CREATE INDEX idx_author_alternate_names_name ON author_alternate_names(name)",
    "CREATE INDEX idx_author_alternate_names_author_id ON author_alternate_names(author_id)",
    "CREATE INDEX idx_author_remote_ids_lookup ON author_remote_ids(source, identifier)",
    # Works
    "CREATE INDEX idx_works_title ON works(title)",
    "CREATE INDEX idx_works_last_modified ON works(last_modified DESC)",
    "CREATE INDEX idx_works_first_publish ON works(first_publish_date)",
    "CREATE INDEX idx_work_authors_author ON work_authors(author_id)",
    "CREATE INDEX idx_work_authors_work ON work_authors(work_id)",
    "CREATE INDEX idx_work_subjects ON work_subjects(subject)",
    "CREATE INDEX idx_work_subjects_work ON work_subjects(work_id)",
    # Editions
    "CREATE INDEX idx_editions_work ON editions(work_id)",
    "CREATE INDEX idx_editions_title ON editions(title)",
    "CREATE INDEX idx_editions_publish_date ON editions(publish_date)",
    "CREATE INDEX idx_editions_last_modified ON editions(last_modified DESC)",
    "CREATE INDEX idx_editions_ocaid ON editions(ocaid) WHERE ocaid IS NOT NULL",
    "CREATE INDEX idx_editions_lc_class_gin ON editions USING GIN(lc_classifications)",
    "CREATE INDEX idx_edition_authors_author ON edition_authors(author_id)",
    "CREATE INDEX idx_edition_isbns ON edition_isbns(isbn)",
    "CREATE INDEX idx_edition_isbns_edition ON edition_isbns(edition_id)",
    "CREATE INDEX idx_edition_lccn ON edition_lccn(lccn)",
    "CREATE INDEX idx_edition_oclc ON edition_oclc(oclc_number)",
    "CREATE INDEX idx_edition_identifiers ON edition_identifiers(source, identifier)",
    "CREATE INDEX idx_edition_publishers ON edition_publishers(publisher)",
    "CREATE INDEX idx_edition_publishers_edition ON edition_publishers(edition_id)",
    "CREATE INDEX idx_edition_subjects ON edition_subjects(subject)",
    "CREATE INDEX idx_edition_series ON edition_series(series)",
    "CREATE INDEX idx_edition_covers_edition ON edition_covers(edition_id)",
    # Users & auth
    "CREATE INDEX idx_users_email ON users(email)",
    "CREATE INDEX idx_sessions_token ON sessions(token)",
    "CREATE INDEX idx_user_editions_user ON user_editions(user_id)",
]


def drop_indexes(conn):
    print("Dropping indexes...")
    with conn.cursor() as cur:
        cur.execute("""
            SELECT i.indexname FROM pg_indexes i
            LEFT JOIN pg_constraint c ON c.conname = i.indexname
            WHERE i.schemaname = 'public' AND c.conname IS NULL AND i.indexname LIKE 'idx_%'
        """)
        indexes = cur.fetchall()
        for (name,) in indexes:
            cur.execute(sql.SQL("DROP INDEX IF EXISTS {}").format(sql.Identifier(name)))
    conn.commit()
    print(f"  Dropped {len(indexes)} indexes")


def rebuild_indexes(conn):
    print("Rebuilding indexes...")
    with conn.cursor() as cur:
        for idx_sql in INDEXES:
            idx_name = idx_sql.split()[2]
            print(f"  {idx_name}...", end="\r")
            cur.execute(idx_sql)
    conn.commit()
    print(f"  Created {len(INDEXES)} indexes")


def refresh_materialized_views(conn):
    print("Refreshing materialized views...")
    with conn.cursor() as cur:
        cur.execute("REFRESH MATERIALIZED VIEW author_stats")
        cur.execute("REFRESH MATERIALIZED VIEW work_summary")
    conn.commit()
    print("  Done")


# --- Main ---

def load_popularity(conn: psycopg.Connection, ratings_path: Path | None, reading_log_path: Path | None):
    print("Loading popularity data...")

    with conn.cursor() as cur:
        # Create temp tables for raw events
        cur.execute("""
            CREATE TEMP TABLE tmp_ratings (
                work_key TEXT,
                edition_key TEXT,
                rating SMALLINT
            ) ON COMMIT DROP
        """)
        cur.execute("""
            CREATE TEMP TABLE tmp_reading_log (
                work_key TEXT,
                edition_key TEXT,
                status TEXT
            ) ON COMMIT DROP
        """)

    # Stream ratings into temp table
    if ratings_path:
        print(f"  Loading ratings from {ratings_path}...")
        opener = gzip.open if ratings_path.suffix == ".gz" else open
        count = 0
        with conn.cursor() as cur:
            with cur.copy("COPY tmp_ratings (work_key, edition_key, rating) FROM STDIN") as copy:
                with opener(ratings_path, "rt", encoding="utf-8") as f:
                    for line in f:
                        parts = line.rstrip("\n").split("\t")
                        if len(parts) < 3:
                            continue
                        work_key, edition_key, rating_str = parts[0], parts[1], parts[2]
                        try:
                            rating = int(rating_str)
                            if rating < 1 or rating > 5:
                                continue
                        except ValueError:
                            continue
                        copy.write_row((work_key, edition_key or None, rating))
                        count += 1
                        if count % 500000 == 0:
                            print(f"    {count:,} ratings loaded...", end="\r")
        print(f"    {count:,} ratings loaded")

    # Stream reading log into temp table
    if reading_log_path:
        print(f"  Loading reading log from {reading_log_path}...")
        opener = gzip.open if reading_log_path.suffix == ".gz" else open
        count = 0
        with conn.cursor() as cur:
            with cur.copy("COPY tmp_reading_log (work_key, edition_key, status) FROM STDIN") as copy:
                with opener(reading_log_path, "rt", encoding="utf-8") as f:
                    for line in f:
                        parts = line.rstrip("\n").split("\t")
                        if len(parts) < 3:
                            continue
                        work_key, edition_key, status = parts[0], parts[1], parts[2]
                        if status not in ("Already Read", "Currently Reading", "Want to Read"):
                            continue
                        copy.write_row((work_key, edition_key or None, status))
                        count += 1
                        if count % 500000 == 0:
                            print(f"    {count:,} reading log entries loaded...", end="\r")
        print(f"    {count:,} reading log entries loaded")

    # Aggregate and insert into work_popularity
    print("  Aggregating work popularity...")
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO work_popularity (work_id, ratings_count, ratings_sum,
                                         want_to_read, currently_reading, already_read, did_not_finish)
            SELECT
                w.id,
                COALESCE(r.cnt, 0),
                COALESCE(r.total, 0),
                COALESCE(rl.want_to_read, 0),
                COALESCE(rl.currently_reading, 0),
                COALESCE(rl.already_read, 0),
                0
            FROM works w
            LEFT JOIN (
                SELECT work_key, COUNT(*) as cnt, SUM(rating) as total
                FROM tmp_ratings
                GROUP BY work_key
            ) r ON r.work_key = w.key
            LEFT JOIN (
                SELECT work_key,
                    COUNT(*) FILTER (WHERE status = 'Want to Read') as want_to_read,
                    COUNT(*) FILTER (WHERE status = 'Currently Reading') as currently_reading,
                    COUNT(*) FILTER (WHERE status = 'Already Read') as already_read
                FROM tmp_reading_log
                GROUP BY work_key
            ) rl ON rl.work_key = w.key
            WHERE r.work_key IS NOT NULL OR rl.work_key IS NOT NULL
            ON CONFLICT (work_id) DO UPDATE SET
                ratings_count = EXCLUDED.ratings_count,
                ratings_sum = EXCLUDED.ratings_sum,
                want_to_read = EXCLUDED.want_to_read,
                currently_reading = EXCLUDED.currently_reading,
                already_read = EXCLUDED.already_read
        """)
        work_count = cur.rowcount
    print(f"    {work_count:,} work records")

    # Aggregate and insert into edition_popularity
    print("  Aggregating edition popularity...")
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO edition_popularity (edition_id, ratings_count, ratings_sum,
                                            want_to_read, currently_reading, already_read, did_not_finish)
            SELECT
                e.id,
                COALESCE(r.cnt, 0),
                COALESCE(r.total, 0),
                COALESCE(rl.want_to_read, 0),
                COALESCE(rl.currently_reading, 0),
                COALESCE(rl.already_read, 0),
                0
            FROM editions e
            LEFT JOIN (
                SELECT edition_key, COUNT(*) as cnt, SUM(rating) as total
                FROM tmp_ratings
                WHERE edition_key IS NOT NULL
                GROUP BY edition_key
            ) r ON r.edition_key = e.key
            LEFT JOIN (
                SELECT edition_key,
                    COUNT(*) FILTER (WHERE status = 'Want to Read') as want_to_read,
                    COUNT(*) FILTER (WHERE status = 'Currently Reading') as currently_reading,
                    COUNT(*) FILTER (WHERE status = 'Already Read') as already_read
                FROM tmp_reading_log
                WHERE edition_key IS NOT NULL
                GROUP BY edition_key
            ) rl ON rl.edition_key = e.key
            WHERE r.edition_key IS NOT NULL OR rl.edition_key IS NOT NULL
            ON CONFLICT (edition_id) DO UPDATE SET
                ratings_count = EXCLUDED.ratings_count,
                ratings_sum = EXCLUDED.ratings_sum,
                want_to_read = EXCLUDED.want_to_read,
                currently_reading = EXCLUDED.currently_reading,
                already_read = EXCLUDED.already_read
        """)
        edition_count = cur.rowcount

    # Aggregate author popularity from their works
    print("  Aggregating author popularity...")
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO author_popularity (author_id, popularity_score)
            SELECT wa.author_id,
                   SUM(compute_popularity_score(wp.ratings_count, wp.ratings_sum,
                       wp.want_to_read, wp.currently_reading, wp.already_read))
            FROM work_authors wa
            JOIN work_popularity wp ON wp.work_id = wa.work_id
            GROUP BY wa.author_id
            ON CONFLICT (author_id) DO UPDATE SET
                popularity_score = EXCLUDED.popularity_score
        """)
        author_count = cur.rowcount
    conn.commit()

    print(f"  Done: {work_count:,} works, {edition_count:,} editions, {author_count:,} authors")


def main():
    parser = argparse.ArgumentParser(description="Load Open Library dumps into PostgreSQL")
    parser.add_argument("--authors", type=Path, help="Path to authors dump file")
    parser.add_argument("--works", type=Path, help="Path to works dump file")
    parser.add_argument("--editions", type=Path, help="Path to editions dump file")
    parser.add_argument("--cover-metadata", type=Path, help="Path to covers metadata dump file")
    parser.add_argument("--ratings", type=Path, help="Path to OL ratings dump file")
    parser.add_argument("--reading-log", type=Path, help="Path to OL reading-log dump file")
    parser.add_argument("--skip-indexes", action="store_true", help="Don't drop/rebuild indexes")
    parser.add_argument("--rebuild-indexes", action="store_true", help="Only rebuild indexes")
    args = parser.parse_args()

    if args.rebuild_indexes:
        print(f"Connecting to {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}...")
        with psycopg.connect(**DB_CONFIG) as conn:
            drop_indexes(conn)
            rebuild_indexes(conn)
            refresh_materialized_views(conn)
        print("Done!")
        return

    if not any([args.authors, args.works, args.editions, args.cover_metadata, args.ratings, args.reading_log]):
        parser.print_help()
        sys.exit(1)

    print(f"Connecting to {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}...")

    with psycopg.connect(**DB_CONFIG) as conn:
        if not args.skip_indexes:
            drop_indexes(conn)

        if args.authors:
            load_authors(conn, args.authors)
        if args.works:
            load_works(conn, args.works)
        if args.editions:
            load_editions(conn, args.editions)
        if args.cover_metadata:
            load_cover_metadata(conn, args.cover_metadata)
        if args.ratings or args.reading_log:
            load_popularity(conn, args.ratings, args.reading_log)

        if not args.skip_indexes:
            rebuild_indexes(conn)
            refresh_materialized_views(conn)

    print("Done!")


if __name__ == "__main__":
    main()
