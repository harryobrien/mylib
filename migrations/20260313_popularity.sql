-- Author-level popularity (aggregated from their works)
CREATE TABLE author_popularity (
    author_id INTEGER PRIMARY KEY REFERENCES authors(id) ON DELETE CASCADE,
    popularity_score REAL NOT NULL DEFAULT 0
);

-- Trigger: update author_popularity when work_popularity changes
CREATE OR REPLACE FUNCTION update_author_popularity_on_work() RETURNS TRIGGER AS $$
DECLARE
    v_work_id INTEGER;
    v_author_id INTEGER;
BEGIN
    v_work_id := COALESCE(NEW.work_id, OLD.work_id);

    -- Update popularity for all authors of this work
    FOR v_author_id IN SELECT author_id FROM work_authors WHERE work_id = v_work_id
    LOOP
        INSERT INTO author_popularity (author_id, popularity_score)
        SELECT v_author_id,
               COALESCE(SUM(compute_popularity_score(wp.ratings_count, wp.ratings_sum,
                   wp.want_to_read, wp.currently_reading, wp.already_read)), 0)
        FROM work_authors wa
        JOIN work_popularity wp ON wp.work_id = wa.work_id
        WHERE wa.author_id = v_author_id
        ON CONFLICT (author_id) DO UPDATE SET
            popularity_score = EXCLUDED.popularity_score;
    END LOOP;

    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_work_popularity_author
AFTER INSERT OR UPDATE OR DELETE ON work_popularity
FOR EACH ROW EXECUTE FUNCTION update_author_popularity_on_work();

-- Work-level popularity (aggregated from all editions)
CREATE TABLE work_popularity (
    work_id INTEGER PRIMARY KEY REFERENCES works(id) ON DELETE CASCADE,
    ratings_count INTEGER NOT NULL DEFAULT 0,
    ratings_sum INTEGER NOT NULL DEFAULT 0,
    want_to_read INTEGER NOT NULL DEFAULT 0,
    currently_reading INTEGER NOT NULL DEFAULT 0,
    already_read INTEGER NOT NULL DEFAULT 0,
    did_not_finish INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX idx_work_popularity_score ON work_popularity (
    (ratings_sum::real / NULLIF(ratings_count, 1) * ln(1 + ratings_count)
     + ln(1 + already_read) * 2.0
     + ln(1 + want_to_read) * 0.5
     + ln(1 + currently_reading))
);

-- Edition-level popularity (for showing "most popular edition")
CREATE TABLE edition_popularity (
    edition_id INTEGER PRIMARY KEY REFERENCES editions(id) ON DELETE CASCADE,
    ratings_count INTEGER NOT NULL DEFAULT 0,
    ratings_sum INTEGER NOT NULL DEFAULT 0,
    want_to_read INTEGER NOT NULL DEFAULT 0,
    currently_reading INTEGER NOT NULL DEFAULT 0,
    already_read INTEGER NOT NULL DEFAULT 0,
    did_not_finish INTEGER NOT NULL DEFAULT 0
);

-- Function to compute popularity score
CREATE OR REPLACE FUNCTION compute_popularity_score(
    ratings_count INTEGER,
    ratings_sum INTEGER,
    want_to_read INTEGER,
    currently_reading INTEGER,
    already_read INTEGER
) RETURNS REAL AS $$
BEGIN
    RETURN COALESCE(
        ratings_sum::real / NULLIF(ratings_count, 0) * ln(1 + ratings_count), 0
    ) + ln(1 + already_read) * 2.0
      + ln(1 + want_to_read) * 0.5
      + ln(1 + currently_reading);
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Trigger function: update edition + work popularity when user_editions changes
CREATE OR REPLACE FUNCTION update_popularity_on_user_edition() RETURNS TRIGGER AS $$
DECLARE
    v_work_id INTEGER;
    old_status VARCHAR(20);
    new_status VARCHAR(20);
BEGIN
    -- Get the work_id for this edition
    IF TG_OP = 'DELETE' THEN
        SELECT work_id INTO v_work_id FROM editions WHERE id = OLD.edition_id;
        old_status := OLD.status;
        new_status := NULL;
    ELSIF TG_OP = 'INSERT' THEN
        SELECT work_id INTO v_work_id FROM editions WHERE id = NEW.edition_id;
        old_status := NULL;
        new_status := NEW.status;
    ELSE -- UPDATE
        SELECT work_id INTO v_work_id FROM editions WHERE id = NEW.edition_id;
        old_status := OLD.status;
        new_status := NEW.status;
    END IF;

    -- Update edition_popularity
    INSERT INTO edition_popularity (edition_id)
    VALUES (COALESCE(NEW.edition_id, OLD.edition_id))
    ON CONFLICT (edition_id) DO NOTHING;

    -- Decrement old status
    IF old_status IS NOT NULL THEN
        UPDATE edition_popularity SET
            want_to_read = want_to_read - CASE WHEN old_status = 'want_to_read' THEN 1 ELSE 0 END,
            currently_reading = currently_reading - CASE WHEN old_status = 'reading' THEN 1 ELSE 0 END,
            already_read = already_read - CASE WHEN old_status = 'finished' THEN 1 ELSE 0 END,
            did_not_finish = did_not_finish - CASE WHEN old_status = 'did_not_finish' THEN 1 ELSE 0 END
        WHERE edition_id = OLD.edition_id;
    END IF;

    -- Increment new status
    IF new_status IS NOT NULL THEN
        UPDATE edition_popularity SET
            want_to_read = want_to_read + CASE WHEN new_status = 'want_to_read' THEN 1 ELSE 0 END,
            currently_reading = currently_reading + CASE WHEN new_status = 'reading' THEN 1 ELSE 0 END,
            already_read = already_read + CASE WHEN new_status = 'finished' THEN 1 ELSE 0 END,
            did_not_finish = did_not_finish + CASE WHEN new_status = 'did_not_finish' THEN 1 ELSE 0 END
        WHERE edition_id = NEW.edition_id;
    END IF;

    -- Update work_popularity (aggregate from all editions)
    INSERT INTO work_popularity (work_id) VALUES (v_work_id)
    ON CONFLICT (work_id) DO NOTHING;

    -- Decrement old status at work level
    IF old_status IS NOT NULL THEN
        UPDATE work_popularity SET
            want_to_read = want_to_read - CASE WHEN old_status = 'want_to_read' THEN 1 ELSE 0 END,
            currently_reading = currently_reading - CASE WHEN old_status = 'reading' THEN 1 ELSE 0 END,
            already_read = already_read - CASE WHEN old_status = 'finished' THEN 1 ELSE 0 END,
            did_not_finish = did_not_finish - CASE WHEN old_status = 'did_not_finish' THEN 1 ELSE 0 END
        WHERE work_id = v_work_id;
    END IF;

    -- Increment new status at work level
    IF new_status IS NOT NULL THEN
        UPDATE work_popularity SET
            want_to_read = want_to_read + CASE WHEN new_status = 'want_to_read' THEN 1 ELSE 0 END,
            currently_reading = currently_reading + CASE WHEN new_status = 'reading' THEN 1 ELSE 0 END,
            already_read = already_read + CASE WHEN new_status = 'finished' THEN 1 ELSE 0 END,
            did_not_finish = did_not_finish + CASE WHEN new_status = 'did_not_finish' THEN 1 ELSE 0 END
        WHERE work_id = v_work_id;
    END IF;

    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_user_editions_popularity
AFTER INSERT OR UPDATE OR DELETE ON user_editions
FOR EACH ROW EXECUTE FUNCTION update_popularity_on_user_edition();
