-- Essential indexes for query performance

-- Authors
CREATE INDEX IF NOT EXISTS idx_authors_name ON authors(name);
CREATE INDEX IF NOT EXISTS idx_authors_last_modified ON authors(last_modified DESC);
CREATE INDEX IF NOT EXISTS idx_author_alternate_names_name ON author_alternate_names(name);
CREATE INDEX IF NOT EXISTS idx_author_alternate_names_author_id ON author_alternate_names(author_id);
CREATE INDEX IF NOT EXISTS idx_author_remote_ids_lookup ON author_remote_ids(source, identifier);

-- Works
CREATE INDEX IF NOT EXISTS idx_works_title ON works(title);
CREATE INDEX IF NOT EXISTS idx_works_last_modified ON works(last_modified DESC);
CREATE INDEX IF NOT EXISTS idx_works_first_publish ON works(first_publish_date);
CREATE INDEX IF NOT EXISTS idx_work_authors_author ON work_authors(author_id);
CREATE INDEX IF NOT EXISTS idx_work_authors_work ON work_authors(work_id);
CREATE INDEX IF NOT EXISTS idx_work_subjects ON work_subjects(subject);
CREATE INDEX IF NOT EXISTS idx_work_subjects_work ON work_subjects(work_id);

-- Editions
CREATE INDEX IF NOT EXISTS idx_editions_work ON editions(work_id);
CREATE INDEX IF NOT EXISTS idx_editions_title ON editions(title);
CREATE INDEX IF NOT EXISTS idx_editions_publish_date ON editions(publish_date);
CREATE INDEX IF NOT EXISTS idx_editions_last_modified ON editions(last_modified DESC);
CREATE INDEX IF NOT EXISTS idx_editions_ocaid ON editions(ocaid) WHERE ocaid IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_editions_lc_class_gin ON editions USING GIN(lc_classifications);
CREATE INDEX IF NOT EXISTS idx_edition_authors_author ON edition_authors(author_id);
CREATE INDEX IF NOT EXISTS idx_edition_isbns ON edition_isbns(isbn);
CREATE INDEX IF NOT EXISTS idx_edition_isbns_edition ON edition_isbns(edition_id);
CREATE INDEX IF NOT EXISTS idx_edition_lccn ON edition_lccn(lccn);
CREATE INDEX IF NOT EXISTS idx_edition_oclc ON edition_oclc(oclc_number);
CREATE INDEX IF NOT EXISTS idx_edition_identifiers ON edition_identifiers(source, identifier);
CREATE INDEX IF NOT EXISTS idx_edition_publishers ON edition_publishers(publisher);
CREATE INDEX IF NOT EXISTS idx_edition_publishers_edition ON edition_publishers(edition_id);
CREATE INDEX IF NOT EXISTS idx_edition_subjects ON edition_subjects(subject);
CREATE INDEX IF NOT EXISTS idx_edition_series ON edition_series(series);
CREATE INDEX IF NOT EXISTS idx_edition_covers_edition ON edition_covers(edition_id);

-- Users & auth
CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
CREATE INDEX IF NOT EXISTS idx_sessions_token ON sessions(token);
CREATE INDEX IF NOT EXISTS idx_user_editions_user ON user_editions(user_id);
