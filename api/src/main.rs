mod auth;
mod base36;
mod db;
mod indexer;
mod routes;
mod search;

use axum::Router;
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;
use axum::http::{header, Method};
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

pub struct AppState {
    pub db: sqlx::PgPool,
    pub search: search::SearchIndex,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let args: Vec<String> = std::env::args().collect();
    let backfill_covers = args.iter().any(|a| a == "--backfill-covers");
    let rebuild_index = args.iter().any(|a| a == "--rebuild-index");

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://mylib:mylib@localhost:5432/mylib".into());

    let index_path = std::env::var("INDEX_PATH").unwrap_or_else(|_| "./index".into());

    if rebuild_index {
        tracing::info!("Deleting existing indexes for rebuild...");
        std::fs::remove_dir_all(format!("{}/works", index_path)).ok();
        std::fs::remove_dir_all(format!("{}/authors", index_path)).ok();
        std::fs::remove_dir_all(format!("{}/editions", index_path)).ok();
    }

    tracing::info!("Connecting to database...");
    let db = PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url)
        .await?;

    tracing::info!("Loading search index from {index_path}...");
    let search = search::SearchIndex::open_or_create(&index_path)?;

    tracing::info!(
        "Search index loaded: {} works, {} authors, {} editions",
        search.works.doc_count(),
        search.authors.doc_count(),
        search.editions.doc_count()
    );

    if backfill_covers {
        tracing::info!("Backfilling covers...");
        indexer::backfill_covers(&db, &search).await?;
        tracing::info!("Cover backfill complete");
        return Ok(());
    }

    indexer::build_missing_indexes(&db, &search).await?;

    let state = Arc::new(AppState { db, search });

    let cors_origins = std::env::var("CORS_ORIGINS")
        .unwrap_or_else(|_| "http://localhost:4321".into())
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect::<Vec<_>>();

    let cors = CorsLayer::new()
        .allow_origin(cors_origins)
        .allow_methods([Method::GET, Method::POST, Method::PUT, Method::DELETE, Method::OPTIONS])
        .allow_headers([header::CONTENT_TYPE, header::COOKIE])
        .allow_credentials(true);

    let app = Router::new()
        .merge(routes::router())
        .merge(auth::router())
        .layer(TraceLayer::new_for_http())
        .layer(cors)
        .with_state(state);

    let addr = std::env::var("LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:3000".into());
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!("Listening on {addr}");

    axum::serve(listener, app).await?;
    Ok(())
}
