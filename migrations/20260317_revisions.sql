-- Revision tracking for works, editions, and authors

CREATE TYPE entity_type AS ENUM ('work', 'edition', 'author');

CREATE TABLE revisions (
    id SERIAL PRIMARY KEY,
    entity_type entity_type NOT NULL,
    entity_id INTEGER NOT NULL,
    user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
    old_values JSONB NOT NULL,
    new_values JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_revisions_entity ON revisions(entity_type, entity_id);
CREATE INDEX idx_revisions_user ON revisions(user_id);
CREATE INDEX idx_revisions_created ON revisions(created_at);
