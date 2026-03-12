# Development commands for mylib

# Run both API and web with hot reloading
dev:
    just --justfile {{justfile()}} dev-api & just --justfile {{justfile()}} dev-web & wait

# Run API with hot reloading (requires cargo-watch: cargo install cargo-watch)
dev-api:
    cd api && cargo watch -x run

# Run web with hot reloading
dev-web:
    cd web && bun run dev

# Run only the database
db:
    docker compose up -d postgres

# Stop the database
db-stop:
    docker compose down

# Build for production
build:
    docker compose build

# Run production stack
prod:
    docker compose up -d

# View logs
logs service="":
    docker compose logs -f {{service}}

# Rebuild search index
reindex:
    curl -X GET http://localhost:3000/admin/reindex

# Install dependencies
install:
    cd web && bun install
    cargo fetch
