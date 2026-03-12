use std::sync::Arc;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};

use crate::{db, indexer, search, AppState};

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        // Search endpoints
        .route("/search", get(search_all))
        .route("/search/works", get(search_works))
        .route("/search/authors", get(search_authors))
        .route("/search/editions", get(search_editions))
        // Resource endpoints
        .route("/works/{key}", get(get_work))
        .route("/works/{key}/authors", get(get_work_authors))
        .route("/works/{key}/editions", get(get_work_editions))
        .route("/authors/{key}", get(get_author))
        .route("/authors/{key}/works", get(get_author_works))
        .route("/editions/{key}", get(get_edition))
        // Admin endpoints
        .route("/admin/reindex", get(reindex))
        .route("/health", get(health))
}

#[derive(Deserialize)]
pub struct SearchQuery {
    q: String,
    #[serde(default = "default_limit")]
    limit: usize,
}

fn default_limit() -> usize { 20 }

#[derive(Serialize)]
pub struct SearchResponse<T> {
    query: String,
    count: usize,
    results: Vec<T>,
}

#[derive(Serialize)]
pub struct UnifiedSearchResponse {
    query: String,
    works: Vec<search::WorkHit>,
    authors: Vec<search::AuthorHit>,
    editions: Vec<search::EditionHit>,
}

async fn search_all(
    State(state): State<Arc<AppState>>,
    Query(params): Query<SearchQuery>,
) -> Result<Json<UnifiedSearchResponse>, AppError> {
    let limit = params.limit.min(10); // Cap per-category for unified search

    let works = state.search.works.search(&params.q, limit)?;
    let authors = state.search.authors.search(&params.q, limit)?;
    let editions = state.search.editions.search(&params.q, limit)?;

    Ok(Json(UnifiedSearchResponse {
        query: params.q,
        works,
        authors,
        editions,
    }))
}

async fn search_works(
    State(state): State<Arc<AppState>>,
    Query(params): Query<SearchQuery>,
) -> Result<Json<SearchResponse<search::WorkHit>>, AppError> {
    let results = state.search.works.search(&params.q, params.limit)?;
    Ok(Json(SearchResponse {
        query: params.q,
        count: results.len(),
        results,
    }))
}

async fn search_authors(
    State(state): State<Arc<AppState>>,
    Query(params): Query<SearchQuery>,
) -> Result<Json<SearchResponse<search::AuthorHit>>, AppError> {
    let results = state.search.authors.search(&params.q, params.limit)?;
    Ok(Json(SearchResponse {
        query: params.q,
        count: results.len(),
        results,
    }))
}

async fn search_editions(
    State(state): State<Arc<AppState>>,
    Query(params): Query<SearchQuery>,
) -> Result<Json<SearchResponse<search::EditionHit>>, AppError> {
    let results = state.search.editions.search(&params.q, params.limit)?;
    Ok(Json(SearchResponse {
        query: params.q,
        count: results.len(),
        results,
    }))
}

#[derive(Serialize)]
pub struct WorkResponse {
    #[serde(flatten)]
    work: db::Work,
    authors: Vec<db::Author>,
}

async fn get_work(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
) -> Result<Json<WorkResponse>, AppError> {
    let full_key = if key.starts_with("/works/") { key } else { format!("/works/{key}") };

    let work = db::get_work_by_key(&state.db, &full_key)
        .await?
        .ok_or(AppError::NotFound)?;

    let authors = db::get_work_authors(&state.db, work.id).await?;

    Ok(Json(WorkResponse { work, authors }))
}

async fn get_work_authors(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
) -> Result<Json<Vec<db::Author>>, AppError> {
    let full_key = if key.starts_with("/works/") { key } else { format!("/works/{key}") };

    let work = db::get_work_by_key(&state.db, &full_key)
        .await?
        .ok_or(AppError::NotFound)?;

    let authors = db::get_work_authors(&state.db, work.id).await?;
    Ok(Json(authors))
}

async fn get_work_editions(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
) -> Result<Json<Vec<db::Edition>>, AppError> {
    let full_key = if key.starts_with("/works/") { key } else { format!("/works/{key}") };

    let work = db::get_work_by_key(&state.db, &full_key)
        .await?
        .ok_or(AppError::NotFound)?;

    let editions = db::get_work_editions(&state.db, work.id).await?;
    Ok(Json(editions))
}

async fn get_author(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
) -> Result<Json<db::Author>, AppError> {
    let full_key = if key.starts_with("/authors/") { key } else { format!("/authors/{key}") };

    let author = db::get_author_by_key(&state.db, &full_key)
        .await?
        .ok_or(AppError::NotFound)?;

    Ok(Json(author))
}

async fn get_author_works(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
) -> Result<Json<Vec<db::Work>>, AppError> {
    let full_key = if key.starts_with("/authors/") { key } else { format!("/authors/{key}") };

    let author = db::get_author_by_key(&state.db, &full_key)
        .await?
        .ok_or(AppError::NotFound)?;

    let works = db::get_author_works(&state.db, author.id).await?;
    Ok(Json(works))
}

#[derive(Serialize)]
pub struct EditionResponse {
    #[serde(flatten)]
    edition: db::Edition,
    isbns: Vec<String>,
}

async fn get_edition(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
) -> Result<Json<EditionResponse>, AppError> {
    let full_key = if key.starts_with("/books/") { key } else { format!("/books/{key}") };

    let edition = db::get_edition_by_key(&state.db, &full_key)
        .await?
        .ok_or(AppError::NotFound)?;

    let isbns = db::get_edition_isbns(&state.db, edition.id).await?;

    Ok(Json(EditionResponse { edition, isbns }))
}

#[derive(Serialize)]
pub struct ReindexResponse {
    works: i64,
    authors: i64,
    editions: i64,
}

async fn reindex(
    State(state): State<Arc<AppState>>,
) -> Result<Json<ReindexResponse>, AppError> {
    indexer::build_indexes(&state.db, &state.search).await?;

    Ok(Json(ReindexResponse {
        works: state.search.works.doc_count() as i64,
        authors: state.search.authors.doc_count() as i64,
        editions: state.search.editions.doc_count() as i64,
    }))
}

async fn health() -> &'static str {
    "ok"
}

pub enum AppError {
    NotFound,
    Internal(anyhow::Error),
}

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        match self {
            AppError::NotFound => (StatusCode::NOT_FOUND, "Not found").into_response(),
            AppError::Internal(e) => {
                tracing::error!("Internal error: {e:?}");
                (StatusCode::INTERNAL_SERVER_ERROR, "Internal error").into_response()
            }
        }
    }
}

impl From<sqlx::Error> for AppError {
    fn from(e: sqlx::Error) -> Self {
        AppError::Internal(e.into())
    }
}

impl From<anyhow::Error> for AppError {
    fn from(e: anyhow::Error) -> Self {
        AppError::Internal(e)
    }
}

impl From<tantivy::TantivyError> for AppError {
    fn from(e: tantivy::TantivyError) -> Self {
        AppError::Internal(e.into())
    }
}
