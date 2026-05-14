ALTER TABLE users
    ADD COLUMN username VARCHAR(30) NOT NULL,
    ADD COLUMN display_name VARCHAR(100),
    ADD COLUMN bio TEXT;

CREATE UNIQUE INDEX idx_users_username_lower ON users (LOWER(username));
