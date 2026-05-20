# mylib

A social book reading and reviewing platform. Users can track their reading, rate and review books, create curated lists, and follow other readers. The production site at https://mylib.co is populated with the full contents of the OpenLibrary dataset (millions of works, authors, and editions).

## Architecture

The project is split into two applications:

- **api/** — REST API built with Rust and Axum, backed by MySQL and Tantivy (in-process full-text search)
- **web/** — Web client built with Astro (SSR) and React (interactive islands)

The client communicates with the API exclusively over HTTP/JSON. It never accesses the database directly.

## Features

- Search works, authors, and editions with autocomplete (Tantivy full-text search with edge ngrams and diacritics normalisation)
- Browse works with editions, cover images, and popularity scores
- Register, log in, and verify email (JWT authentication, Argon2 password hashing)
- Track reading status (want to read, reading, finished, did not finish) with page progress and dates
- Rate editions on a quarter-star scale (1.0–5.0) and write reviews
- Create and manage curated book lists
- Follow other users and see their reviews in a personalised feed
- Edit book metadata (works, authors, editions) with revision tracking
- Public user profiles with reading stats, reviews, and lists
- Discover popular books, recent reviews, and recent lists
- PWA with offline caching (Workbox service worker)
- Responsive design with mobile support

## Running

### Prerequisites

- Docker and Docker Compose

### Production

```
docker compose up -d
```

This starts MySQL, the API, the web server, and an Nginx reverse proxy on port 80.

### Development

```
docker compose up -d mysql    # start database
cd web
npm run dev &
cd ../api
cargo run
```

The API runs on http://localhost:3000 and the web client on http://localhost:4321.

### Data Import

Book data is imported from OpenLibrary monthly dumps:

```
cd scripts
pip install -r requirements.txt
python loader.py
```

## Configuration

### API (`api/.env`)

| Variable | Description | Default |
|---|---|---|
| `DATABASE_URL` | MySQL connection string | `mysql://mylib:mylib@localhost:3306/mylib` |
| `INDEX_PATH` | Tantivy search index directory | `./index` |
| `LISTEN_ADDR` | Bind address | `0.0.0.0:3000` |
| `RUST_LOG` | Log level | `info` |
| `JWT_SECRET` | Secret for signing JWT tokens | `dev-secret-change-in-production` |
| `BASE_URL` | Frontend URL for email links | `http://localhost:4321` |
| `CORS_ORIGINS` | Allowed origins (comma-separated) | `http://localhost:4321` |
| `RESEND_API_KEY` | Resend API key for verification emails | Required for email |
| `RESEND_FROM_EMAIL` | Sender address | `noreply@example.com` |

### Web (`web/.env`)

| Variable | Description | Default |
|---|---|---|
| `API_URL` | API URL for server-side rendering | `http://localhost:3000` |
| `PUBLIC_API_URL` | API URL for client-side fetches | `http://localhost:3000` |

## Project Structure

```
mylib/
  api/src/
    main.rs          Server setup, database pool, search index
    auth.rs          Authentication and user endpoints
    routes.rs        Public API endpoints
    db.rs            Database queries
    search.rs        Tantivy search index operations
    indexer.rs       Background search index builder
    base36.rs        ID encoding for URL slugs
  web/src/
    pages/           Astro SSR routes
    components/      React interactive components
    stores/          Nanostores state management
    lib/             Shared fetchers and utilities
    layouts/         Layout with global styles
  migrations/
    schema.sql       Full MySQL schema with tables, indexes, and triggers
  docker-compose.yml
  nginx.conf
```

## External Services

- **OpenLibrary** — source of book data and cover images (covers.openlibrary.org)
- **Resend** — transactional email for account verification
