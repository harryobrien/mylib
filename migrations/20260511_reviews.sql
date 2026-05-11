DROP INDEX IF EXISTS idx_work_popularity_score;
CREATE INDEX idx_work_popularity_score ON work_popularity (
    (ratings_sum::real / NULLIF(ratings_count, 0) * ln(1 + ratings_count)
     + ln(1 + already_read) * 2.0
     + ln(1 + want_to_read) * 0.5
     + ln(1 + currently_reading))
);

CREATE TABLE user_reviews (
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    edition_id INTEGER NOT NULL REFERENCES editions(id) ON DELETE CASCADE,
    rating SMALLINT NOT NULL CHECK (rating BETWEEN 1 AND 5),
    review_text TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, edition_id)
);

CREATE INDEX idx_user_reviews_edition ON user_reviews(edition_id);

CREATE OR REPLACE FUNCTION update_popularity_on_user_review() RETURNS TRIGGER AS $$
DECLARE
    v_edition_id INTEGER;
    v_work_id INTEGER;
BEGIN
    v_edition_id := COALESCE(NEW.edition_id, OLD.edition_id);
    SELECT work_id INTO v_work_id FROM editions WHERE id = v_edition_id;

    INSERT INTO edition_popularity (edition_id)
    VALUES (v_edition_id)
    ON CONFLICT (edition_id) DO NOTHING;

    INSERT INTO work_popularity (work_id)
    VALUES (v_work_id)
    ON CONFLICT (work_id) DO NOTHING;

    IF TG_OP = 'DELETE' THEN
        UPDATE edition_popularity SET
            ratings_count = ratings_count - 1,
            ratings_sum = ratings_sum - OLD.rating
        WHERE edition_id = v_edition_id;

        UPDATE work_popularity SET
            ratings_count = ratings_count - 1,
            ratings_sum = ratings_sum - OLD.rating
        WHERE work_id = v_work_id;

    ELSIF TG_OP = 'INSERT' THEN
        UPDATE edition_popularity SET
            ratings_count = ratings_count + 1,
            ratings_sum = ratings_sum + NEW.rating
        WHERE edition_id = v_edition_id;

        UPDATE work_popularity SET
            ratings_count = ratings_count + 1,
            ratings_sum = ratings_sum + NEW.rating
        WHERE work_id = v_work_id;

    ELSE -- UPDATE (rating changed)
        UPDATE edition_popularity SET
            ratings_sum = ratings_sum - OLD.rating + NEW.rating
        WHERE edition_id = v_edition_id;

        UPDATE work_popularity SET
            ratings_sum = ratings_sum - OLD.rating + NEW.rating
        WHERE work_id = v_work_id;
    END IF;

    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_user_reviews_popularity
AFTER INSERT OR UPDATE OR DELETE ON user_reviews
FOR EACH ROW EXECUTE FUNCTION update_popularity_on_user_review();
