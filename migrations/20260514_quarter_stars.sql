ALTER TABLE user_reviews
    ALTER COLUMN rating TYPE REAL USING rating::real;

ALTER TABLE user_reviews
    DROP CONSTRAINT IF EXISTS user_reviews_rating_check;

ALTER TABLE user_reviews
    ADD CONSTRAINT user_reviews_rating_check
    CHECK (rating >= 1.0 AND rating <= 5.0 AND (rating * 4) = FLOOR(rating * 4));

ALTER TABLE edition_popularity ALTER COLUMN ratings_sum TYPE REAL USING ratings_sum::real;
ALTER TABLE work_popularity ALTER COLUMN ratings_sum TYPE REAL USING ratings_sum::real;
