CREATE TABLE authors (
    id INT AUTO_INCREMENT PRIMARY KEY,
    `key` VARCHAR(25) UNIQUE NOT NULL CHECK (`key` REGEXP '^/authors/OL[0-9]+A$'),
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
    created_at DATETIME,
    last_modified DATETIME NOT NULL
);

CREATE TABLE author_alternate_names (
    author_id INTEGER NOT NULL REFERENCES authors(id) ON DELETE CASCADE,
    name VARCHAR(500) NOT NULL,
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
    url VARCHAR(2048) NOT NULL,
    title VARCHAR(500) NOT NULL,
    PRIMARY KEY (author_id, url(255))
);

CREATE TABLE author_remote_ids (
    author_id INTEGER NOT NULL REFERENCES authors(id) ON DELETE CASCADE,
    source VARCHAR(50) NOT NULL,
    identifier TEXT NOT NULL,
    PRIMARY KEY (author_id, source)
);

CREATE TABLE works (
    id INT AUTO_INCREMENT PRIMARY KEY,
    `key` VARCHAR(25) UNIQUE NOT NULL CHECK (`key` REGEXP '^/works/OL[0-9]+W$'),
    title TEXT NOT NULL,
    subtitle TEXT,
    first_publish_date VARCHAR(50),
    description TEXT,
    notes TEXT,
    revision INTEGER NOT NULL,
    latest_revision INTEGER,
    created_at DATETIME,
    last_modified DATETIME NOT NULL,
    lc_classifications JSON
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
    url VARCHAR(2048) NOT NULL,
    title VARCHAR(500) NOT NULL,
    PRIMARY KEY (work_id, url(255))
);

CREATE TABLE work_subjects (
    work_id INTEGER NOT NULL REFERENCES works(id) ON DELETE CASCADE,
    subject VARCHAR(500) NOT NULL,
    PRIMARY KEY (work_id, subject)
);

CREATE TABLE languages (
    code VARCHAR(3) PRIMARY KEY CHECK (code REGEXP '^[a-z]{3}$'),
    name VARCHAR(100)
);

CREATE TABLE editions (
    id INT AUTO_INCREMENT PRIMARY KEY,
    `key` VARCHAR(25) UNIQUE NOT NULL CHECK (`key` REGEXP '^/books/OL[0-9]+M$'),
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
    publish_country VARCHAR(3) CHECK (publish_country IS NULL OR publish_country REGEXP '^[a-z]{2,3}$'),
    publish_date TEXT,
    translation_of TEXT,
    description TEXT,
    first_sentence TEXT,
    notes TEXT,
    revision INTEGER NOT NULL,
    latest_revision INTEGER,
    created_at DATETIME,
    last_modified DATETIME NOT NULL,
    contributions JSON,
    dewey_decimal_class JSON,
    lc_classifications JSON,
    other_titles JSON,
    work_titles JSON,
    source_records JSON,
    local_ids JSON,
    table_of_contents JSON
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
    oclc_number VARCHAR(100) NOT NULL,
    PRIMARY KEY (edition_id, oclc_number)
);

CREATE TABLE edition_identifiers (
    edition_id INTEGER NOT NULL REFERENCES editions(id) ON DELETE CASCADE,
    source VARCHAR(100) NOT NULL,
    identifier VARCHAR(500) NOT NULL,
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
    url VARCHAR(2048) NOT NULL,
    title VARCHAR(500) NOT NULL,
    PRIMARY KEY (edition_id, url(255))
);

CREATE TABLE edition_publishers (
    edition_id INTEGER NOT NULL REFERENCES editions(id) ON DELETE CASCADE,
    publisher VARCHAR(500) NOT NULL,
    PRIMARY KEY (edition_id, publisher)
);

CREATE TABLE edition_publish_places (
    edition_id INTEGER NOT NULL REFERENCES editions(id) ON DELETE CASCADE,
    place VARCHAR(500) NOT NULL,
    PRIMARY KEY (edition_id, place)
);

CREATE TABLE edition_subjects (
    edition_id INTEGER NOT NULL REFERENCES editions(id) ON DELETE CASCADE,
    subject VARCHAR(500) NOT NULL,
    PRIMARY KEY (edition_id, subject)
);

CREATE TABLE edition_genres (
    edition_id INTEGER NOT NULL REFERENCES editions(id) ON DELETE CASCADE,
    genre VARCHAR(500) NOT NULL,
    PRIMARY KEY (edition_id, genre)
);

CREATE TABLE edition_series (
    edition_id INTEGER NOT NULL REFERENCES editions(id) ON DELETE CASCADE,
    series VARCHAR(500) NOT NULL,
    PRIMARY KEY (edition_id, series)
);

CREATE TABLE users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    email_verified TINYINT(1) NOT NULL DEFAULT 0,
    created_at DATETIME NOT NULL DEFAULT NOW(),
    username VARCHAR(30) NOT NULL,
    display_name VARCHAR(100),
    bio TEXT,
    username_lower VARCHAR(30) GENERATED ALWAYS AS (LOWER(username)) STORED
);

CREATE TABLE email_verifications (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token VARCHAR(64) UNIQUE NOT NULL,
    expires_at DATETIME NOT NULL,
    created_at DATETIME NOT NULL DEFAULT NOW()
);

CREATE TABLE user_editions (
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    edition_id INTEGER NOT NULL REFERENCES editions(id) ON DELETE CASCADE,
    status VARCHAR(20) NOT NULL CHECK (status IN ('reading', 'want_to_read', 'finished', 'did_not_finish')),
    created_at DATETIME NOT NULL DEFAULT NOW(),
    started_at DATE,
    finished_at DATE,
    current_page INTEGER CHECK (current_page IS NULL OR current_page >= 0),
    PRIMARY KEY (user_id, edition_id)
);

CREATE TABLE user_reviews (
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    edition_id INTEGER NOT NULL REFERENCES editions(id) ON DELETE CASCADE,
    rating FLOAT NOT NULL,
    review_text TEXT,
    created_at DATETIME NOT NULL DEFAULT NOW(),
    updated_at DATETIME NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, edition_id),
    CONSTRAINT user_reviews_rating_check
        CHECK (rating >= 1.0 AND rating <= 5.0 AND (rating * 4) = FLOOR(rating * 4))
);

CREATE TABLE user_follows (
    follower_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    following_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at DATETIME NOT NULL DEFAULT NOW(),
    PRIMARY KEY (follower_id, following_id),
    CHECK (follower_id != following_id)
);

CREATE TABLE user_lists (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    title VARCHAR(200) NOT NULL,
    description TEXT,
    created_at DATETIME NOT NULL DEFAULT NOW(),
    updated_at DATETIME NOT NULL DEFAULT NOW()
);

CREATE TABLE user_list_works (
    list_id INTEGER NOT NULL REFERENCES user_lists(id) ON DELETE CASCADE,
    work_id INTEGER NOT NULL REFERENCES works(id) ON DELETE CASCADE,
    position INTEGER NOT NULL DEFAULT 0,
    added_at DATETIME NOT NULL DEFAULT NOW(),
    PRIMARY KEY (list_id, work_id)
);

CREATE TABLE revisions (
    id INT AUTO_INCREMENT PRIMARY KEY,
    entity_type ENUM('work', 'edition', 'author') NOT NULL,
    entity_id INTEGER NOT NULL,
    user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
    old_values JSON NOT NULL,
    new_values JSON NOT NULL,
    created_at DATETIME NOT NULL DEFAULT NOW()
);

CREATE TABLE work_popularity (
    work_id INTEGER PRIMARY KEY REFERENCES works(id) ON DELETE CASCADE,
    ratings_count INTEGER NOT NULL DEFAULT 0,
    ratings_sum FLOAT NOT NULL DEFAULT 0,
    want_to_read INTEGER NOT NULL DEFAULT 0,
    currently_reading INTEGER NOT NULL DEFAULT 0,
    already_read INTEGER NOT NULL DEFAULT 0,
    did_not_finish INTEGER NOT NULL DEFAULT 0,
    popularity_score FLOAT GENERATED ALWAYS AS (
        COALESCE(ratings_sum / NULLIF(ratings_count, 0) * LN(1 + ratings_count), 0)
        + LN(1 + already_read) * 2.0
        + LN(1 + want_to_read) * 0.5
        + LN(1 + currently_reading)
    ) STORED
);

CREATE TABLE edition_popularity (
    edition_id INTEGER PRIMARY KEY REFERENCES editions(id) ON DELETE CASCADE,
    ratings_count INTEGER NOT NULL DEFAULT 0,
    ratings_sum FLOAT NOT NULL DEFAULT 0,
    want_to_read INTEGER NOT NULL DEFAULT 0,
    currently_reading INTEGER NOT NULL DEFAULT 0,
    already_read INTEGER NOT NULL DEFAULT 0,
    did_not_finish INTEGER NOT NULL DEFAULT 0,
    popularity_score FLOAT GENERATED ALWAYS AS (
        COALESCE(ratings_sum / NULLIF(ratings_count, 0) * LN(1 + ratings_count), 0)
        + LN(1 + already_read) * 2.0
        + LN(1 + want_to_read) * 0.5
        + LN(1 + currently_reading)
    ) STORED
);

CREATE TABLE author_popularity (
    author_id INTEGER PRIMARY KEY REFERENCES authors(id) ON DELETE CASCADE,
    popularity_score FLOAT NOT NULL DEFAULT 0
);

CREATE INDEX idx_authors_name ON authors(name(255));
CREATE INDEX idx_authors_last_modified ON authors(last_modified DESC);
CREATE INDEX idx_author_alternate_names_name ON author_alternate_names(name(255));
CREATE INDEX idx_author_alternate_names_author_id ON author_alternate_names(author_id);
CREATE INDEX idx_author_remote_ids_lookup ON author_remote_ids(source, identifier(255));

CREATE INDEX idx_works_title ON works(title(255));
CREATE INDEX idx_works_last_modified ON works(last_modified DESC);
CREATE INDEX idx_works_first_publish ON works(first_publish_date);
CREATE INDEX idx_work_authors_author ON work_authors(author_id);
CREATE INDEX idx_work_authors_work ON work_authors(work_id);
CREATE INDEX idx_work_subjects ON work_subjects(subject);
CREATE INDEX idx_work_subjects_work ON work_subjects(work_id);

CREATE INDEX idx_editions_work ON editions(work_id);
CREATE INDEX idx_editions_title ON editions(title(255));
CREATE INDEX idx_editions_publish_date ON editions(publish_date(255));
CREATE INDEX idx_editions_last_modified ON editions(last_modified DESC);
CREATE INDEX idx_editions_ocaid ON editions(ocaid(255));
CREATE INDEX idx_edition_authors_author ON edition_authors(author_id);
CREATE INDEX idx_edition_isbns ON edition_isbns(isbn);
CREATE INDEX idx_edition_isbns_edition ON edition_isbns(edition_id);
CREATE INDEX idx_edition_lccn ON edition_lccn(lccn);
CREATE INDEX idx_edition_oclc ON edition_oclc(oclc_number);
CREATE INDEX idx_edition_identifiers ON edition_identifiers(source(50), identifier(255));
CREATE INDEX idx_edition_publishers ON edition_publishers(publisher);
CREATE INDEX idx_edition_publishers_edition ON edition_publishers(edition_id);
CREATE INDEX idx_edition_subjects ON edition_subjects(subject);
CREATE INDEX idx_edition_series ON edition_series(series);
CREATE INDEX idx_edition_covers_edition ON edition_covers(edition_id);

CREATE UNIQUE INDEX idx_users_username_lower ON users(username_lower);
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_email_verifications_token ON email_verifications(token);
CREATE INDEX idx_email_verifications_user ON email_verifications(user_id);
CREATE INDEX idx_user_editions_user ON user_editions(user_id);
CREATE INDEX idx_user_editions_status ON user_editions(user_id, status);
CREATE INDEX idx_user_reviews_edition ON user_reviews(edition_id);
CREATE INDEX idx_user_follows_following ON user_follows(following_id);
CREATE INDEX idx_user_lists_user ON user_lists(user_id);
CREATE INDEX idx_user_list_works_work ON user_list_works(work_id);
CREATE INDEX idx_revisions_entity ON revisions(entity_type, entity_id);
CREATE INDEX idx_revisions_user ON revisions(user_id);
CREATE INDEX idx_revisions_created ON revisions(created_at);
CREATE INDEX idx_work_popularity_score ON work_popularity(popularity_score DESC);
CREATE INDEX idx_edition_popularity_score ON edition_popularity(popularity_score DESC);

DELIMITER //

CREATE FUNCTION compute_popularity_score(
    p_ratings_count INT,
    p_ratings_sum INT,
    p_want_to_read INT,
    p_currently_reading INT,
    p_already_read INT
) RETURNS FLOAT
DETERMINISTIC
BEGIN
    RETURN COALESCE(
        p_ratings_sum / NULLIF(p_ratings_count, 0) * LN(1 + p_ratings_count), 0
    ) + LN(1 + p_already_read) * 2.0
      + LN(1 + p_want_to_read) * 0.5
      + LN(1 + p_currently_reading);
END //

CREATE FUNCTION normalize_isbn(raw TEXT)
RETURNS TEXT DETERMINISTIC
RETURN UPPER(REGEXP_REPLACE(raw, '[- ]', ''))//

CREATE FUNCTION extract_lang_code(lang_key TEXT)
RETURNS VARCHAR(3) DETERMINISTIC
RETURN REGEXP_SUBSTR(lang_key, '[a-z]{3}$')//

CREATE FUNCTION extract_ol_id(ol_key TEXT)
RETURNS BIGINT DETERMINISTIC
RETURN CAST(REGEXP_SUBSTR(ol_key, '[0-9]+(?=[AMW]$)') AS UNSIGNED)//

CREATE TRIGGER trg_user_editions_popularity_insert
AFTER INSERT ON user_editions
FOR EACH ROW
BEGIN
    DECLARE v_work_id INTEGER;
    SELECT work_id INTO v_work_id FROM editions WHERE id = NEW.edition_id;

    INSERT IGNORE INTO edition_popularity (edition_id) VALUES (NEW.edition_id);
    UPDATE edition_popularity SET
        want_to_read = want_to_read + CASE WHEN NEW.status = 'want_to_read' THEN 1 ELSE 0 END,
        currently_reading = currently_reading + CASE WHEN NEW.status = 'reading' THEN 1 ELSE 0 END,
        already_read = already_read + CASE WHEN NEW.status = 'finished' THEN 1 ELSE 0 END,
        did_not_finish = did_not_finish + CASE WHEN NEW.status = 'did_not_finish' THEN 1 ELSE 0 END
    WHERE edition_id = NEW.edition_id;

    INSERT IGNORE INTO work_popularity (work_id) VALUES (v_work_id);
    UPDATE work_popularity SET
        want_to_read = want_to_read + CASE WHEN NEW.status = 'want_to_read' THEN 1 ELSE 0 END,
        currently_reading = currently_reading + CASE WHEN NEW.status = 'reading' THEN 1 ELSE 0 END,
        already_read = already_read + CASE WHEN NEW.status = 'finished' THEN 1 ELSE 0 END,
        did_not_finish = did_not_finish + CASE WHEN NEW.status = 'did_not_finish' THEN 1 ELSE 0 END
    WHERE work_id = v_work_id;
END //

CREATE TRIGGER trg_user_editions_popularity_update
AFTER UPDATE ON user_editions
FOR EACH ROW
BEGIN
    DECLARE v_work_id INTEGER;
    SELECT work_id INTO v_work_id FROM editions WHERE id = NEW.edition_id;

    UPDATE edition_popularity SET
        want_to_read = want_to_read - CASE WHEN OLD.status = 'want_to_read' THEN 1 ELSE 0 END,
        currently_reading = currently_reading - CASE WHEN OLD.status = 'reading' THEN 1 ELSE 0 END,
        already_read = already_read - CASE WHEN OLD.status = 'finished' THEN 1 ELSE 0 END,
        did_not_finish = did_not_finish - CASE WHEN OLD.status = 'did_not_finish' THEN 1 ELSE 0 END
    WHERE edition_id = OLD.edition_id;
    UPDATE edition_popularity SET
        want_to_read = want_to_read + CASE WHEN NEW.status = 'want_to_read' THEN 1 ELSE 0 END,
        currently_reading = currently_reading + CASE WHEN NEW.status = 'reading' THEN 1 ELSE 0 END,
        already_read = already_read + CASE WHEN NEW.status = 'finished' THEN 1 ELSE 0 END,
        did_not_finish = did_not_finish + CASE WHEN NEW.status = 'did_not_finish' THEN 1 ELSE 0 END
    WHERE edition_id = NEW.edition_id;

    UPDATE work_popularity SET
        want_to_read = want_to_read - CASE WHEN OLD.status = 'want_to_read' THEN 1 ELSE 0 END,
        currently_reading = currently_reading - CASE WHEN OLD.status = 'reading' THEN 1 ELSE 0 END,
        already_read = already_read - CASE WHEN OLD.status = 'finished' THEN 1 ELSE 0 END,
        did_not_finish = did_not_finish - CASE WHEN OLD.status = 'did_not_finish' THEN 1 ELSE 0 END
    WHERE work_id = v_work_id;
    UPDATE work_popularity SET
        want_to_read = want_to_read + CASE WHEN NEW.status = 'want_to_read' THEN 1 ELSE 0 END,
        currently_reading = currently_reading + CASE WHEN NEW.status = 'reading' THEN 1 ELSE 0 END,
        already_read = already_read + CASE WHEN NEW.status = 'finished' THEN 1 ELSE 0 END,
        did_not_finish = did_not_finish + CASE WHEN NEW.status = 'did_not_finish' THEN 1 ELSE 0 END
    WHERE work_id = v_work_id;
END //

CREATE TRIGGER trg_user_editions_popularity_delete
AFTER DELETE ON user_editions
FOR EACH ROW
BEGIN
    DECLARE v_work_id INTEGER;
    SELECT work_id INTO v_work_id FROM editions WHERE id = OLD.edition_id;

    UPDATE edition_popularity SET
        want_to_read = want_to_read - CASE WHEN OLD.status = 'want_to_read' THEN 1 ELSE 0 END,
        currently_reading = currently_reading - CASE WHEN OLD.status = 'reading' THEN 1 ELSE 0 END,
        already_read = already_read - CASE WHEN OLD.status = 'finished' THEN 1 ELSE 0 END,
        did_not_finish = did_not_finish - CASE WHEN OLD.status = 'did_not_finish' THEN 1 ELSE 0 END
    WHERE edition_id = OLD.edition_id;

    UPDATE work_popularity SET
        want_to_read = want_to_read - CASE WHEN OLD.status = 'want_to_read' THEN 1 ELSE 0 END,
        currently_reading = currently_reading - CASE WHEN OLD.status = 'reading' THEN 1 ELSE 0 END,
        already_read = already_read - CASE WHEN OLD.status = 'finished' THEN 1 ELSE 0 END,
        did_not_finish = did_not_finish - CASE WHEN OLD.status = 'did_not_finish' THEN 1 ELSE 0 END
    WHERE work_id = v_work_id;
END //

CREATE TRIGGER trg_user_reviews_popularity_insert
AFTER INSERT ON user_reviews
FOR EACH ROW
BEGIN
    DECLARE v_work_id INTEGER;
    SELECT work_id INTO v_work_id FROM editions WHERE id = NEW.edition_id;

    INSERT IGNORE INTO edition_popularity (edition_id) VALUES (NEW.edition_id);
    INSERT IGNORE INTO work_popularity (work_id) VALUES (v_work_id);

    UPDATE edition_popularity SET
        ratings_count = ratings_count + 1, ratings_sum = ratings_sum + NEW.rating
    WHERE edition_id = NEW.edition_id;
    UPDATE work_popularity SET
        ratings_count = ratings_count + 1, ratings_sum = ratings_sum + NEW.rating
    WHERE work_id = v_work_id;
END //

CREATE TRIGGER trg_user_reviews_popularity_update
AFTER UPDATE ON user_reviews
FOR EACH ROW
BEGIN
    DECLARE v_work_id INTEGER;
    SELECT work_id INTO v_work_id FROM editions WHERE id = NEW.edition_id;

    UPDATE edition_popularity SET
        ratings_sum = ratings_sum - OLD.rating + NEW.rating
    WHERE edition_id = NEW.edition_id;
    UPDATE work_popularity SET
        ratings_sum = ratings_sum - OLD.rating + NEW.rating
    WHERE work_id = v_work_id;
END //

CREATE TRIGGER trg_user_reviews_popularity_delete
AFTER DELETE ON user_reviews
FOR EACH ROW
BEGIN
    DECLARE v_work_id INTEGER;
    SELECT work_id INTO v_work_id FROM editions WHERE id = OLD.edition_id;

    UPDATE edition_popularity SET
        ratings_count = ratings_count - 1, ratings_sum = ratings_sum - OLD.rating
    WHERE edition_id = OLD.edition_id;
    UPDATE work_popularity SET
        ratings_count = ratings_count - 1, ratings_sum = ratings_sum - OLD.rating
    WHERE work_id = v_work_id;
END //

CREATE TRIGGER trg_work_popularity_author_insert
AFTER INSERT ON work_popularity
FOR EACH ROW
BEGIN
    DECLARE v_author_id INTEGER;
    DECLARE done INT DEFAULT 0;
    DECLARE cur CURSOR FOR SELECT author_id FROM work_authors WHERE work_id = NEW.work_id;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

    OPEN cur;
    read_loop: LOOP
        FETCH cur INTO v_author_id;
        IF done THEN LEAVE read_loop; END IF;
        INSERT INTO author_popularity (author_id, popularity_score)
        SELECT v_author_id, COALESCE(SUM(compute_popularity_score(
            wp.ratings_count, wp.ratings_sum, wp.want_to_read, wp.currently_reading, wp.already_read)), 0)
        FROM work_authors wa JOIN work_popularity wp ON wp.work_id = wa.work_id
        WHERE wa.author_id = v_author_id
        ON DUPLICATE KEY UPDATE popularity_score = VALUES(popularity_score);
    END LOOP;
    CLOSE cur;
END //

CREATE TRIGGER trg_work_popularity_author_update
AFTER UPDATE ON work_popularity
FOR EACH ROW
BEGIN
    DECLARE v_author_id INTEGER;
    DECLARE done INT DEFAULT 0;
    DECLARE cur CURSOR FOR SELECT author_id FROM work_authors WHERE work_id = NEW.work_id;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

    OPEN cur;
    read_loop: LOOP
        FETCH cur INTO v_author_id;
        IF done THEN LEAVE read_loop; END IF;
        INSERT INTO author_popularity (author_id, popularity_score)
        SELECT v_author_id, COALESCE(SUM(compute_popularity_score(
            wp.ratings_count, wp.ratings_sum, wp.want_to_read, wp.currently_reading, wp.already_read)), 0)
        FROM work_authors wa JOIN work_popularity wp ON wp.work_id = wa.work_id
        WHERE wa.author_id = v_author_id
        ON DUPLICATE KEY UPDATE popularity_score = VALUES(popularity_score);
    END LOOP;
    CLOSE cur;
END //

CREATE TRIGGER trg_work_popularity_author_delete
AFTER DELETE ON work_popularity
FOR EACH ROW
BEGIN
    DECLARE v_author_id INTEGER;
    DECLARE done INT DEFAULT 0;
    DECLARE cur CURSOR FOR SELECT author_id FROM work_authors WHERE work_id = OLD.work_id;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

    OPEN cur;
    read_loop: LOOP
        FETCH cur INTO v_author_id;
        IF done THEN LEAVE read_loop; END IF;
        INSERT INTO author_popularity (author_id, popularity_score)
        SELECT v_author_id, COALESCE(SUM(compute_popularity_score(
            wp.ratings_count, wp.ratings_sum, wp.want_to_read, wp.currently_reading, wp.already_read)), 0)
        FROM work_authors wa JOIN work_popularity wp ON wp.work_id = wa.work_id
        WHERE wa.author_id = v_author_id
        ON DUPLICATE KEY UPDATE popularity_score = VALUES(popularity_score);
    END LOOP;
    CLOSE cur;
END //

DELIMITER ;

INSERT IGNORE INTO languages (code, name) VALUES
    ('eng', 'English'), ('spa', 'Spanish'), ('fre', 'French'),
    ('ger', 'German'), ('ita', 'Italian'), ('por', 'Portuguese'),
    ('rus', 'Russian'), ('chi', 'Chinese'), ('jpn', 'Japanese'),
    ('ara', 'Arabic'), ('hin', 'Hindi'), ('dut', 'Dutch'),
    ('pol', 'Polish'), ('swe', 'Swedish'), ('kor', 'Korean'),
    ('lat', 'Latin'), ('gre', 'Greek'), ('heb', 'Hebrew'),
    ('tur', 'Turkish'), ('vie', 'Vietnamese'), ('tha', 'Thai'),
    ('ind', 'Indonesian'), ('ukr', 'Ukrainian'), ('ces', 'Czech'),
    ('dan', 'Danish'), ('fin', 'Finnish'), ('nor', 'Norwegian'),
    ('hun', 'Hungarian'), ('rum', 'Romanian'), ('cat', 'Catalan');

INSERT INTO authors (id, `key`, name, personal_name, bio, birth_date, death_date, revision, last_modified) VALUES
(1858708, '/authors/OL33452A', 'Petronius', 'Petronius Arbiter.', 'Gaius Petronius Arbiter (c. 27 – c. 66 AD; sometimes Titus Petronius Niger) was a Roman courtier during the reign of Nero (r. 54–68). He is generally believed to be the author of the *Satyricon,* a satirical novel believed to have been written during the Neronian era.', 'ca. 27', '66', 18, '2025-10-30 20:44:18'),
(2915174, '/authors/OL232499A', 'C. S. Forester', 'C. S. Forester', 'Cecil Scott Forester, an Englishman, was born in Cairo in 1899, the son of a British army officer. He wrote many best-selling novels—African Queen and The General among them—before he wrote the first of his Hornblower stories in 1937.', '27 Aug 1899', '2 Apr 1966', 25, '2026-02-09 22:28:49'),
(7919213, '/authors/OL2826579A', 'John Booty', NULL, NULL, NULL, NULL, 1, '2008-04-29 15:03:11'),
(12840199, '/authors/OL123428A', 'John Donne', 'Donne, John', NULL, '1572', '1631', 8, '2025-10-30 17:33:44'),
(13225072, '/authors/OL553208A', 'Sarah Ruden', 'Sarah Ruden', NULL, NULL, NULL, 2, '2008-08-29 21:33:03'),
(13686704, '/authors/OL2826703A', 'P.G. Stanwood', NULL, NULL, NULL, NULL, 1, '2008-04-29 15:03:11');

INSERT INTO works (id, `key`, title, first_publish_date, description, revision, last_modified) VALUES
(2064355, '/works/OL15420303W', 'John Donne Poetry', NULL, 'This Norton Critical Edition presents a comprehensive collection of Donne\'s poetry, scrupulously edited from the Westmoreland manuscript where possible.', 30, '2025-02-01 18:13:36'),
(3194284, '/works/OL1937690W', 'The African Queen', 'January 1986', 'English missionary\'s sister enlists aid of mechanic in blowing up German gunboat on African lake.', 5, '2026-02-09 22:32:18'),
(14444199, '/works/OL503861W', 'Satyricon', '1711', 'An incomplete Roman novel believed to have been written by Gaius Petronius Arbiter. A picaresque story concerning the narrator''s battle to keep his lover, a handsome sixteen year old boy, away from his many rivals.', 11, '2025-03-20 23:56:18');

INSERT INTO work_authors VALUES
(2064355, 12840199, NULL, NULL, 0),
(2064355, 7919213, NULL, NULL, 1),
(2064355, 13686704, NULL, NULL, 2),
(3194284, 2915174, NULL, NULL, 0),
(14444199, 1858708, NULL, NULL, 0);

INSERT INTO work_subjects (work_id, subject) VALUES
(2064355, 'English poetry'), (2064355, 'Poetry'), (2064355, 'Sonnets'), (2064355, 'Religion'),
(3194284, 'Fiction'), (3194284, 'World War, 1914-1918'), (3194284, 'Man-woman relationships'), (3194284, 'Missionaries'),
(14444199, 'Latin literature'), (14444199, 'Fiction'), (14444199, 'Satire, latin'), (14444199, 'Translations into English'), (14444199, 'Classic Literature');

INSERT INTO work_covers VALUES
(2064355, 8238738, 0),
(3194284, 12960164, 0),
(14444199, 6060585, 0);

INSERT INTO cover_metadata VALUES
(501488, 336, 500, '2007-12-31'), (1364498, 317, 475, '2007-12-31'),
(4626944, 128, 191, '2008-06-01'), (5365091, 128, 191, '2008-06-01'),
(6060585, 1887, 3166, '2009-09-13'), (7948351, 2608, 4204, '2017-05-02'),
(8238738, 553, 868, '2018-09-05'), (10167625, 1734, 2704, '2020-06-11'),
(10445691, 299, 500, '2020-09-15'), (12960164, 1563, 2645, '2022-10-20'),
(13677940, 333, 500, '2023-03-19');

INSERT INTO editions (id, `key`, work_id, title, subtitle, number_of_pages, physical_format, by_statement, publish_date, description, notes, revision, created_at, last_modified, lc_classifications) VALUES
(2701517, '/books/OL30152983M', 14444199, 'Satyricon', NULL, 256, 'hardcover', NULL, 'Mar 01, 2000', NULL, 'Source title: Satyricon (Hackett Classics)', 10, '2020-09-15 07:12:00', '2026-01-14 22:21:46', '["PA6558.E5 R83 2000"]'),
(49682983, '/books/OL7634257M', 14444199, 'The Satyricon', NULL, NULL, 'Hardcover', NULL, 'January 1, 1981', NULL, NULL, 9, '2008-04-29 15:03:11', '2023-03-19 21:54:54', NULL),
(7452323, '/books/OL1089103M', 3194284, 'The African Queen', NULL, 260, NULL, 'C. S. Forester.', '1994', NULL, NULL, 12, '2008-04-01 03:28:50', '2026-02-09 22:32:18', '["PR6011.O56 A69 1994"]'),
(20791422, '/books/OL28236034M', 3194284, 'The African queen', NULL, 189, NULL, 'C.S. Forester', '2006', 'Thrown together at the outset of World War I, the spinster sister of a British missionary and derelict captain of the launch, The African Queen, determine to pilot the boat down an unchartered river in an effort to destroy a German gunboat.', 'Originally published: London: Heinemann, 1935.', 5, '2020-06-11 08:02:00', '2026-02-09 22:32:18', '["PR6011.O56 A37"]'),
(28306308, '/books/OL7988361M', 2064355, 'John Donne', 'Selected Poems (Phoenix Poetry)', 112, 'Hardcover', NULL, 'August 28, 2003', NULL, NULL, 9, '2008-04-29 15:03:11', '2023-06-17 03:06:42', NULL),
(36509272, '/books/OL17053313M', 2064355, 'John Donne', 'selected letters', 133, NULL, 'edited by P.M. Oliver', '2002', NULL, 'Includes bibliographical references (p. 129) and index', 10, '2008-09-27 07:01:28', '2024-08-30 05:48:24', '["PR2248 .A44 2002"]');

INSERT INTO edition_isbns VALUES
(2701517, '0872205118', 10, '0872205118'), (2701517, '9780872205116', 13, '9780872205116'),
(7452323, '0745129366', 10, '0745129366'), (7452323, '0816174598', 10, '0816174598'),
(20791422, '075382079X', 10, '075382079X'), (20791422, '9780753820797', 13, '9780753820797'),
(28306308, '0753816504', 10, '0753816504'), (28306308, '9780753816509', 13, '9780753816509'),
(36509272, '0415942276', 10, '0415942276'), (36509272, '0415942284', 10, '0415942284'),
(49682983, '0472729357', 10, '0472729357'), (49682983, '9780472729357', 13, '9780472729357');

INSERT INTO edition_covers VALUES
(2701517, 10445691, 0), (7452323, 7948351, 0), (20791422, 10167625, 0),
(28306308, 501488, 0), (36509272, 5365091, 0), (36509272, 4626944, 1),
(49682983, 13677940, 0);

INSERT INTO edition_publishers VALUES
(2701517, 'Hackett Publishing Company, Inc.'), (7452323, 'G. K. Hall'),
(20791422, 'Phoenix'), (28306308, 'Phoenix Press'),
(36509272, 'Routledge'), (49682983, 'University of Michigan Press');

INSERT INTO edition_publish_places VALUES
(7452323, 'Thorndike, Me., USA'), (20791422, 'London'), (36509272, 'New York');

INSERT INTO edition_languages VALUES
(2701517, 'eng'), (7452323, 'eng'), (20791422, 'eng'),
(28306308, 'eng'), (36509272, 'eng'), (49682983, 'eng');

INSERT INTO edition_authors VALUES
(2701517, 1858708, 0), (2701517, 13225072, 1),
(7452323, 2915174, 0), (20791422, 2915174, 0),
(28306308, 12840199, 0), (36509272, 12840199, 0),
(49682983, 1858708, 0);

INSERT INTO edition_subjects VALUES
(7452323, 'Large type books.'), (7452323, 'Man-woman relationships -- Fiction.'),
(20791422, 'Fiction'), (20791422, 'World War, 1914-1918'),
(28306308, 'English, Irish, Scottish, Welsh'), (28306308, 'Poetry'),
(36509272, 'Poets, English -- Correspondence'),
(49682983, 'Literary studies: classical, early & medieval');

INSERT INTO edition_lccn VALUES (2701517, '99055844'), (7452323, '94013169');
INSERT INTO edition_oclc VALUES (2701517, '42780676'), (20791422, '70765130'), (36509272, '50632593');

INSERT INTO edition_identifiers VALUES
(7452323, 'goodreads', '3465165'), (28306308, 'goodreads', '660441'),
(36509272, 'librarything', '4663549'), (49682983, 'goodreads', '3267615');

INSERT INTO users (id, email, password_hash, email_verified, username) VALUES
(1, 'demo@mylib.co', '$argon2id$v=19$m=19456,t=2,p=1$j5GOr6Uj4/8N1oPEPMopBQ$qss3wLhwhbjOAJuH/1lsVC/iCFfirZSq7KNNuibSyOs', 1, 'demo');

INSERT INTO user_editions (user_id, edition_id, status, started_at) VALUES
(1, 20791422, 'reading', '2026-05-10'),
(1, 2701517, 'finished', '2026-04-01');

UPDATE user_editions SET finished_at = '2026-04-15', current_page = 256
WHERE user_id = 1 AND edition_id = 2701517;

INSERT INTO user_reviews (user_id, edition_id, rating, review_text) VALUES
(1, 2701517, 4.5, 'Test review');

INSERT INTO user_lists (id, user_id, title, description) VALUES
(1, 1, 'Test list', NULL);

INSERT INTO user_list_works (list_id, work_id, position) VALUES
(1, 14444199, 0),
(1, 3194284, 1);
