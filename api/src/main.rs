mod base36;
mod db;
mod indexer;
mod routes;
mod search;

use axum::Router;
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;
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

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://mylib:mylib@localhost:5432/mylib".into());

    let index_path = std::env::var("INDEX_PATH").unwrap_or_else(|_| "./index".into());

    tracing::info!("Connecting to database...");
    let db = PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url)
        .await?;

    tracing::info!("Loading search index from {index_path}...");
    let search = search::SearchIndex::open_or_create(&index_path)?;

    if search.is_empty() {
        tracing::info!("Search index is empty, building...");
        indexer::build_indexes(&db, &search).await?;
        tracing::info!("Search index built successfully");
    } else {
        tracing::info!(
            "Search index loaded: {} works, {} authors, {} editions",
            search.works.doc_count(),
            search.authors.doc_count(),
            search.editions.doc_count()
        );
    }

    let state = Arc::new(AppState { db, search });

    let app = Router::new()
        .merge(routes::router())
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
        .with_state(state);

    let addr = std::env::var("LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:3000".into());
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!("Listening on {addr}");

    axum::serve(listener, app).await?;
    Ok(())
}
