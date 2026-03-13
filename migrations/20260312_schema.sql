CREATE TABLE authors (
    id SERIAL PRIMARY KEY,
    key VARCHAR(25) UNIQUE NOT NULL CHECK (key ~ '^/authors/OL[0-9]+A$'),
    name TEXT NOT NULL,
    fuller_name TEXT,
    personal_name TEXT,
    title VARCHAR(100),
    bio TEXT,
    birth_date VARCHAR(50),
    death_date VARCHAR(50),
    date VARCHAR(50),
    entity_type VARCHAR(10) CHECK (entity_type IN ('person', 'org', 'event')),
    revision INTEGER NOT NULL,
    latest_revision INTEGER,
    created_at TIMESTAMPTZ,
    last_modified TIMESTAMPTZ NOT NULL
);

CREATE TABLE author_alternate_names (
    author_id INTEGER NOT NULL REFERENCES authors(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    PRIMARY KEY (author_id, name)
);

CREATE TABLE author_photos (
    author_id INTEGER NOT NULL REFERENCES authors(id) ON DELETE CASCADE,
    photo_id BIGINT NOT NULL,
    position SMALLINT NOT NULL,
    PRIMARY KEY (author_id, position)
);

CREATE TABLE author_links (
    author_id INTEGER NOT NULL REFERENCES authors(id) ON DELETE CASCADE,
    url TEXT NOT NULL,
    title VARCHAR(500) NOT NULL,
    PRIMARY KEY (author_id, url)
);

CREATE TABLE author_remote_ids (
    author_id INTEGER NOT NULL REFERENCES authors(id) ON DELETE CASCADE,
    source VARCHAR(50) NOT NULL,
    identifier TEXT NOT NULL,
    PRIMARY KEY (author_id, source)
);

-- WORKS

CREATE TABLE works (
    id SERIAL PRIMARY KEY,
    key VARCHAR(25) UNIQUE NOT NULL CHECK (key ~ '^/works/OL[0-9]+W$'),
    title TEXT NOT NULL,
    subtitle TEXT,
    first_publish_date VARCHAR(50),
    description TEXT,
    notes TEXT,
    revision INTEGER NOT NULL,
    latest_revision INTEGER,
    created_at TIMESTAMPTZ,
    last_modified TIMESTAMPTZ NOT NULL,
    lc_classifications VARCHAR(50)[]
);

CREATE TABLE work_authors (
    work_id INTEGER NOT NULL REFERENCES works(id) ON DELETE CASCADE,
    author_id INTEGER NOT NULL REFERENCES authors(id) ON DELETE CASCADE,
    role TEXT,
    as_name TEXT,
    position SMALLINT NOT NULL,
    PRIMARY KEY (work_id, position)
);

CREATE TABLE cover_metadata (
    id BIGINT PRIMARY KEY,
    width INTEGER NOT NULL,
    height INTEGER NOT NULL,
    created_at DATE
);

CREATE TABLE work_covers (
    work_id INTEGER NOT NULL REFERENCES works(id) ON DELETE CASCADE,
    cover_id BIGINT NOT NULL,
    position SMALLINT NOT NULL,
    PRIMARY KEY (work_id, position)
);

CREATE TABLE work_links (
    work_id INTEGER NOT NULL REFERENCES works(id) ON DELETE CASCADE,
    url TEXT NOT NULL,
    title VARCHAR(500) NOT NULL,
    PRIMARY KEY (work_id, url)
);

CREATE TABLE work_subjects (
    work_id INTEGER NOT NULL REFERENCES works(id) ON DELETE CASCADE,
    subject TEXT NOT NULL,
    PRIMARY KEY (work_id, subject)
);

-- LANGUAGES (lookup table)

CREATE TABLE languages (
    code VARCHAR(3) PRIMARY KEY CHECK (code ~ '^[a-z]{3}$'),
    name VARCHAR(100)
);

-- EDITIONS

CREATE TABLE editions (
    id SERIAL PRIMARY KEY,
    key VARCHAR(25) UNIQUE NOT NULL CHECK (key ~ '^/books/OL[0-9]+M$'),
    work_id INTEGER NOT NULL REFERENCES works(id),
    title TEXT NOT NULL,
    subtitle TEXT,
    ocaid TEXT,
    weight TEXT,
    number_of_pages INTEGER CHECK (number_of_pages IS NULL OR number_of_pages > 0),
    pagination TEXT,
    physical_dimensions TEXT,
    physical_format TEXT,
    by_statement TEXT,
    edition_name TEXT,
    copyright_date TEXT,
    publish_country VARCHAR(3) CHECK (publish_country IS NULL OR publish_country ~ '^[a-z]{2,3}$'),
    publish_date TEXT,
    translation_of TEXT,
    description TEXT,
    first_sentence TEXT,
    notes TEXT,
    revision INTEGER NOT NULL,
    latest_revision INTEGER,
    created_at TIMESTAMPTZ,
    last_modified TIMESTAMPTZ NOT NULL,
    contributions TEXT[],
    dewey_decimal_class VARCHAR(30)[],
    lc_classifications VARCHAR(50)[],
    other_titles TEXT[],
    work_titles TEXT[],
    source_records TEXT[],
    local_ids TEXT[],
    table_of_contents JSONB
);

CREATE TABLE edition_languages (
    edition_id INTEGER NOT NULL REFERENCES editions(id) ON DELETE CASCADE,
    language_code VARCHAR(3) NOT NULL REFERENCES languages(code),
    PRIMARY KEY (edition_id, language_code)
);

CREATE TABLE edition_translated_from (
    edition_id INTEGER NOT NULL REFERENCES editions(id) ON DELETE CASCADE,
    language_code VARCHAR(3) NOT NULL REFERENCES languages(code),
    PRIMARY KEY (edition_id, language_code)
);

CREATE TABLE edition_authors (
    edition_id INTEGER NOT NULL REFERENCES editions(id) ON DELETE CASCADE,
    author_id INTEGER NOT NULL REFERENCES authors(id) ON DELETE CASCADE,
    position SMALLINT NOT NULL,
    PRIMARY KEY (edition_id, position)
);

CREATE TABLE edition_isbns (
    edition_id INTEGER NOT NULL REFERENCES editions(id) ON DELETE CASCADE,
    isbn VARCHAR(13) NOT NULL,
    isbn_type SMALLINT NOT NULL CHECK (isbn_type IN (10, 13)),
    raw_isbn TEXT,
    PRIMARY KEY (edition_id, isbn)
);

CREATE TABLE edition_lccn (
    edition_id INTEGER NOT NULL REFERENCES editions(id) ON DELETE CASCADE,
    lccn VARCHAR(20) NOT NULL,
    PRIMARY KEY (edition_id, lccn)
);

CREATE TABLE edition_oclc (
    edition_id INTEGER NOT NULL REFERENCES editions(id) ON DELETE CASCADE,
    oclc_number TEXT NOT NULL,
    PRIMARY KEY (edition_id, oclc_number)
);

CREATE TABLE edition_identifiers (
    edition_id INTEGER NOT NULL REFERENCES editions(id) ON DELETE CASCADE,
    source TEXT NOT NULL,
    identifier TEXT NOT NULL,
    PRIMARY KEY (edition_id, source, identifier)
);

CREATE TABLE edition_covers (
    edition_id INTEGER NOT NULL REFERENCES editions(id) ON DELETE CASCADE,
    cover_id BIGINT NOT NULL,
    position SMALLINT NOT NULL,
    PRIMARY KEY (edition_id, position)
);

CREATE TABLE edition_links (
    edition_id INTEGER NOT NULL REFERENCES editions(id) ON DELETE CASCADE,
    url TEXT NOT NULL,
    title VARCHAR(500) NOT NULL,
    PRIMARY KEY (edition_id, url)
);

CREATE TABLE edition_publishers (
    edition_id INTEGER NOT NULL REFERENCES editions(id) ON DELETE CASCADE,
    publisher TEXT NOT NULL,
    PRIMARY KEY (edition_id, publisher)
);

CREATE TABLE edition_publish_places (
    edition_id INTEGER NOT NULL REFERENCES editions(id) ON DELETE CASCADE,
    place TEXT NOT NULL,
    PRIMARY KEY (edition_id, place)
);

CREATE TABLE edition_subjects (
    edition_id INTEGER NOT NULL REFERENCES editions(id) ON DELETE CASCADE,
    subject TEXT NOT NULL,
    PRIMARY KEY (edition_id, subject)
);

CREATE TABLE edition_genres (
    edition_id INTEGER NOT NULL REFERENCES editions(id) ON DELETE CASCADE,
    genre TEXT NOT NULL,
    PRIMARY KEY (edition_id, genre)
);

CREATE TABLE edition_series (
    edition_id INTEGER NOT NULL REFERENCES editions(id) ON DELETE CASCADE,
    series TEXT NOT NULL,
    PRIMARY KEY (edition_id, series)
);

-- INDEXES

-- Authors
CREATE INDEX idx_authors_name ON authors(name);
CREATE INDEX idx_authors_last_modified ON authors(last_modified DESC);
CREATE INDEX idx_author_alternate_names_name ON author_alternate_names(name);
CREATE INDEX idx_author_remote_ids_lookup ON author_remote_ids(source, identifier);

-- Works
CREATE INDEX idx_works_title ON works(title);
CREATE INDEX idx_works_last_modified ON works(last_modified DESC);
CREATE INDEX idx_works_first_publish ON works(first_publish_date);
CREATE INDEX idx_work_authors_author ON work_authors(author_id);
CREATE INDEX idx_work_subjects ON work_subjects(subject);

-- Editions
CREATE INDEX idx_editions_work ON editions(work_id);
CREATE INDEX idx_editions_title ON editions(title);
CREATE INDEX idx_editions_publish_date ON editions(publish_date);
CREATE INDEX idx_editions_last_modified ON editions(last_modified DESC);
CREATE INDEX idx_editions_ocaid ON editions(ocaid) WHERE ocaid IS NOT NULL;
CREATE INDEX idx_editions_lc_class_gin ON editions USING GIN(lc_classifications);

CREATE INDEX idx_edition_authors_author ON edition_authors(author_id);
CREATE INDEX idx_edition_isbns ON edition_isbns(isbn);
CREATE INDEX idx_edition_lccn ON edition_lccn(lccn);
CREATE INDEX idx_edition_oclc ON edition_oclc(oclc_number);
CREATE INDEX idx_edition_identifiers ON edition_identifiers(source, identifier);
CREATE INDEX idx_edition_publishers ON edition_publishers(publisher);
CREATE INDEX idx_edition_subjects ON edition_subjects(subject);
CREATE INDEX idx_edition_series ON edition_series(series);

-- HELPER FUNCTIONS

-- Normalize ISBN by removing dashes and spaces
CREATE OR REPLACE FUNCTION normalize_isbn(raw TEXT)
RETURNS TEXT AS $$
    SELECT upper(regexp_replace(raw, '[- ]', '', 'g'));
$$ LANGUAGE SQL IMMUTABLE STRICT;

-- Extract language code from Open Library language key
CREATE OR REPLACE FUNCTION extract_lang_code(lang_key TEXT)
RETURNS VARCHAR(3) AS $$
    SELECT substring(lang_key FROM '/languages/([a-z]{3})$');
$$ LANGUAGE SQL IMMUTABLE STRICT;

-- Extract numeric ID from Open Library key
CREATE OR REPLACE FUNCTION extract_ol_id(ol_key TEXT)
RETURNS BIGINT AS $$
    SELECT (regexp_match(ol_key, 'OL([0-9]+)[AMW]$'))[1]::BIGINT;
$$ LANGUAGE SQL IMMUTABLE STRICT;

-- MATERIALIZED VIEWS

-- Author statistics
CREATE MATERIALIZED VIEW author_stats AS
SELECT
    a.id,
    a.key,
    a.name,
    COUNT(DISTINCT wa.work_id) AS work_count,
    COUNT(DISTINCT e.id) AS edition_count
FROM authors a
LEFT JOIN work_authors wa ON a.id = wa.author_id
LEFT JOIN works w ON wa.work_id = w.id
LEFT JOIN editions e ON w.id = e.work_id
GROUP BY a.id, a.key, a.name;

CREATE UNIQUE INDEX idx_author_stats_id ON author_stats(id);

-- Work summary with edition count
CREATE MATERIALIZED VIEW work_summary AS
SELECT
    w.id,
    w.key,
    w.title,
    w.first_publish_date,
    COUNT(e.id) AS edition_count,
    MIN(ei.isbn) FILTER (WHERE ei.isbn_type = 13) AS primary_isbn13,
    array_agg(DISTINCT a.name) FILTER (WHERE a.name IS NOT NULL) AS author_names
FROM works w
LEFT JOIN editions e ON w.id = e.work_id
LEFT JOIN edition_isbns ei ON e.id = ei.edition_id
LEFT JOIN work_authors wa ON w.id = wa.work_id
LEFT JOIN authors a ON wa.author_id = a.id
GROUP BY w.id, w.key, w.title, w.first_publish_date;

CREATE UNIQUE INDEX idx_work_summary_id ON work_summary(id);

-- SEED COMMON LANGUAGES

INSERT INTO languages (code, name) VALUES
    ('eng', 'English'),
    ('spa', 'Spanish'),
    ('fre', 'French'),
    ('ger', 'German'),
    ('ita', 'Italian'),
    ('por', 'Portuguese'),
    ('rus', 'Russian'),
    ('chi', 'Chinese'),
    ('jpn', 'Japanese'),
    ('ara', 'Arabic'),
    ('hin', 'Hindi'),
    ('dut', 'Dutch'),
    ('pol', 'Polish'),
    ('swe', 'Swedish'),
    ('kor', 'Korean'),
    ('lat', 'Latin'),
    ('gre', 'Greek'),
    ('heb', 'Hebrew'),
    ('tur', 'Turkish'),
    ('vie', 'Vietnamese'),
    ('tha', 'Thai'),
    ('ind', 'Indonesian'),
    ('ukr', 'Ukrainian'),
    ('ces', 'Czech'),
    ('dan', 'Danish'),
    ('fin', 'Finnish'),
    ('nor', 'Norwegian'),
    ('hun', 'Hungarian'),
    ('rum', 'Romanian'),
    ('cat', 'Catalan')
ON CONFLICT (code) DO NOTHING;
