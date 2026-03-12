#!/usr/bin/env python3

import os
from pathlib import Path

import psycopg

DB_CONFIG = {
    "host": os.environ.get("PGHOST", "localhost"),
    "port": int(os.environ.get("PGPORT", 5432)),
    "dbname": os.environ.get("PGDATABASE", "mylib"),
    "user": os.environ.get("PGUSER", "mylib"),
    "password": os.environ.get("PGPASSWORD", "mylib"),
}

OUTPUT_DIR = Path("./sample")
AUTHOR_SAMPLE_SIZE = 10000


def export_table(cur, table: str, query: str):
    path = OUTPUT_DIR / f"{table}.csv"
    with open(path, "wb") as f:
        with cur.copy(f"COPY ({query}) TO STDOUT CSV HEADER") as copy:
            row_count = 0
            for data in copy:
                chunk = bytes(data)
                f.write(chunk)
                row_count += chunk.count(b'\n')
    row_count = max(0, row_count - 1)
    print(f"  {table}: {row_count:,} rows")


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Connecting to {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}...")

    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            print(f"Sampling {AUTHOR_SAMPLE_SIZE:,} authors...")
            cur.execute(f"""
                CREATE TEMP TABLE sample_authors AS
                SELECT id FROM authors ORDER BY random() LIMIT {AUTHOR_SAMPLE_SIZE}
            """)

            cur.execute("""
                CREATE TEMP TABLE sample_works AS
                SELECT DISTINCT w.id FROM works w
                JOIN work_authors wa ON w.id = wa.work_id
                WHERE wa.author_id IN (SELECT id FROM sample_authors)
            """)

            cur.execute("""
                CREATE TEMP TABLE sample_editions AS
                SELECT id FROM editions WHERE work_id IN (SELECT id FROM sample_works)
            """)

            cur.execute("""
                INSERT INTO sample_authors
                SELECT DISTINCT ea.author_id FROM edition_authors ea
                WHERE ea.edition_id IN (SELECT id FROM sample_editions)
                AND ea.author_id NOT IN (SELECT id FROM sample_authors)
            """)

            cur.execute("""
                CREATE TEMP TABLE sample_languages AS
                SELECT DISTINCT language_code FROM edition_languages
                WHERE edition_id IN (SELECT id FROM sample_editions)
                UNION
                SELECT DISTINCT language_code FROM edition_translated_from
                WHERE edition_id IN (SELECT id FROM sample_editions)
            """)

            print("Exporting tables...")

            export_table(cur, "languages",
                "SELECT * FROM languages WHERE code IN (SELECT language_code FROM sample_languages)")
            export_table(cur, "authors",
                "SELECT * FROM authors WHERE id IN (SELECT id FROM sample_authors)")
            export_table(cur, "author_alternate_names",
                "SELECT * FROM author_alternate_names WHERE author_id IN (SELECT id FROM sample_authors)")
            export_table(cur, "author_photos",
                "SELECT * FROM author_photos WHERE author_id IN (SELECT id FROM sample_authors)")
            export_table(cur, "author_links",
                "SELECT * FROM author_links WHERE author_id IN (SELECT id FROM sample_authors)")
            export_table(cur, "author_remote_ids",
                "SELECT * FROM author_remote_ids WHERE author_id IN (SELECT id FROM sample_authors)")
            export_table(cur, "works",
                "SELECT * FROM works WHERE id IN (SELECT id FROM sample_works)")
            export_table(cur, "work_authors",
                "SELECT * FROM work_authors WHERE work_id IN (SELECT id FROM sample_works)")
            export_table(cur, "work_covers",
                "SELECT * FROM work_covers WHERE work_id IN (SELECT id FROM sample_works)")
            export_table(cur, "work_links",
                "SELECT * FROM work_links WHERE work_id IN (SELECT id FROM sample_works)")
            export_table(cur, "work_subjects",
                "SELECT * FROM work_subjects WHERE work_id IN (SELECT id FROM sample_works)")
            export_table(cur, "editions",
                "SELECT * FROM editions WHERE id IN (SELECT id FROM sample_editions)")
            export_table(cur, "edition_authors",
                "SELECT * FROM edition_authors WHERE edition_id IN (SELECT id FROM sample_editions)")
            export_table(cur, "edition_languages",
                "SELECT * FROM edition_languages WHERE edition_id IN (SELECT id FROM sample_editions)")
            export_table(cur, "edition_translated_from",
                "SELECT * FROM edition_translated_from WHERE edition_id IN (SELECT id FROM sample_editions)")
            export_table(cur, "edition_isbns",
                "SELECT * FROM edition_isbns WHERE edition_id IN (SELECT id FROM sample_editions)")
            export_table(cur, "edition_lccn",
                "SELECT * FROM edition_lccn WHERE edition_id IN (SELECT id FROM sample_editions)")
            export_table(cur, "edition_oclc",
                "SELECT * FROM edition_oclc WHERE edition_id IN (SELECT id FROM sample_editions)")
            export_table(cur, "edition_identifiers",
                "SELECT * FROM edition_identifiers WHERE edition_id IN (SELECT id FROM sample_editions)")
            export_table(cur, "edition_covers",
                "SELECT * FROM edition_covers WHERE edition_id IN (SELECT id FROM sample_editions)")
            export_table(cur, "edition_links",
                "SELECT * FROM edition_links WHERE edition_id IN (SELECT id FROM sample_editions)")
            export_table(cur, "edition_publishers",
                "SELECT * FROM edition_publishers WHERE edition_id IN (SELECT id FROM sample_editions)")
            export_table(cur, "edition_publish_places",
                "SELECT * FROM edition_publish_places WHERE edition_id IN (SELECT id FROM sample_editions)")
            export_table(cur, "edition_subjects",
                "SELECT * FROM edition_subjects WHERE edition_id IN (SELECT id FROM sample_editions)")
            export_table(cur, "edition_genres",
                "SELECT * FROM edition_genres WHERE edition_id IN (SELECT id FROM sample_editions)")
            export_table(cur, "edition_series",
                "SELECT * FROM edition_series WHERE edition_id IN (SELECT id FROM sample_editions)")

    print(f"\nExported to {OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
