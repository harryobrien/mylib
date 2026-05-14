DROP INDEX IF EXISTS idx_work_popularity_score;
CREATE INDEX idx_work_popularity_score ON work_popularity (
    (ratings_sum / NULLIF(ratings_count, 0)::double precision
     * ln((1 + ratings_count)::double precision)
     + ln((1 + already_read)::double precision) * 2.0
     + ln((1 + want_to_read)::double precision) * 0.5
     + ln((1 + currently_reading)::double precision))
    DESC
);
