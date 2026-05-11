ALTER TABLE user_editions
    ADD COLUMN started_at DATE,
    ADD COLUMN finished_at DATE,
    ADD COLUMN current_page INTEGER CHECK (current_page IS NULL OR current_page >= 0);
