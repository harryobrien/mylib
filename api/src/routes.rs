use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::{base36, db, search, AppState};

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        // Search endpoints
        .route("/search", get(search_all))
        .route("/search/works", get(search_works))
        .route("/search/authors", get(search_authors))
        .route("/search/editions", get(search_editions))
        // Resource endpoints (slug = base36 encoded ID)
        .route("/works/{slug}", get(get_work))
        .route("/works/{slug}/authors", get(get_work_authors))
        .route("/works/{slug}/editions", get(get_work_editions))
        .route("/authors/{slug}", get(get_author))
        .route("/authors/{slug}/works", get(get_author_works))
        .route("/editions/{slug}", get(get_edition))
        .route("/health", get(health))
}

#[derive(Deserialize)]
pub struct SearchQuery {
    q: String,
    #[serde(default = "default_limit")]
    limit: usize,
}

fn default_limit() -> usize {
    20
}

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
    let limit = params.limit.min(10);
    let q = params.q.clone();

    let state_clone = state.clone();
    let q1 = q.clone();
    let works_handle = tokio::task::spawn_blocking(move || state_clone.search.works.search(&q1, limit));

    let state_clone = state.clone();
    let q2 = q.clone();
    let authors_handle = tokio::task::spawn_blocking(move || state_clone.search.authors.search(&q2, limit));

    let state_clone = state.clone();
    let q3 = q.clone();
    let editions_handle = tokio::task::spawn_blocking(move || state_clone.search.editions.search(&q3, limit));

    let (works_res, authors_res, editions_res) = tokio::join!(works_handle, authors_handle, editions_handle);

    let works = works_res.map_err(|e| AppError::Internal(e.into()))??;
    let authors = authors_res.map_err(|e| AppError::Internal(e.into()))??;
    let editions = editions_res.map_err(|e| AppError::Internal(e.into()))??;

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
    slug: String,
    #[serde(flatten)]
    work: db::Work,
    authors: Vec<AuthorSummary>,
    editions: Vec<EditionSummary>,
    #[serde(skip_serializing_if = "Option::is_none")]
    popularity: Option<db::WorkPopularity>,
}

#[derive(Serialize)]
pub struct AuthorSummary {
    slug: String,
    #[serde(flatten)]
    author: db::Author,
}

#[derive(Serialize)]
pub struct EditionSummary {
    slug: String,
    #[serde(flatten)]
    edition: db::Edition,
}

#[derive(Serialize)]
pub struct WorkSummary {
    slug: String,
    #[serde(flatten)]
    work: db::Work,
}

async fn get_work(
    State(state): State<Arc<AppState>>,
    Path(slug): Path<String>,
) -> Result<Json<WorkResponse>, AppError> {
    let id = base36::decode(&slug).ok_or(AppError::NotFound)? as i32;

    let work = db::get_work_by_id(&state.db, id)
        .await?
        .ok_or(AppError::NotFound)?;

    let (authors, editions, popularity) = tokio::join!(
        db::get_work_authors(&state.db, work.id),
        db::get_work_editions(&state.db, work.id),
        db::get_work_popularity(&state.db, work.id)
    );

    let authors = authors?
        .into_iter()
        .map(|a| AuthorSummary {
            slug: base36::encode(a.id as i64),
            author: a,
        })
        .collect();

    let editions = editions?
        .into_iter()
        .map(|e| EditionSummary {
            slug: base36::encode(e.id as i64),
            edition: e,
        })
        .collect();

    Ok(Json(WorkResponse {
        slug,
        work,
        authors,
        editions,
        popularity: popularity?,
    }))
}

async fn get_work_authors(
    State(state): State<Arc<AppState>>,
    Path(slug): Path<String>,
) -> Result<Json<Vec<db::Author>>, AppError> {
    let id = base36::decode(&slug).ok_or(AppError::NotFound)? as i32;

    let authors = db::get_work_authors(&state.db, id).await?;
    Ok(Json(authors))
}

async fn get_work_editions(
    State(state): State<Arc<AppState>>,
    Path(slug): Path<String>,
) -> Result<Json<Vec<db::Edition>>, AppError> {
    let id = base36::decode(&slug).ok_or(AppError::NotFound)? as i32;

    let editions = db::get_work_editions(&state.db, id).await?;
    Ok(Json(editions))
}

async fn get_author(
    State(state): State<Arc<AppState>>,
    Path(slug): Path<String>,
) -> Result<Json<db::Author>, AppError> {
    let id = base36::decode(&slug).ok_or(AppError::NotFound)? as i32;

    let author = db::get_author_by_id(&state.db, id)
        .await?
        .ok_or(AppError::NotFound)?;

    Ok(Json(author))
}

async fn get_author_works(
    State(state): State<Arc<AppState>>,
    Path(slug): Path<String>,
) -> Result<Json<Vec<WorkSummary>>, AppError> {
    let id = base36::decode(&slug).ok_or(AppError::NotFound)? as i32;

    let works = db::get_author_works(&state.db, id).await?;
    let works = works
        .into_iter()
        .map(|w| WorkSummary {
            slug: base36::encode(w.id as i64),
            work: w,
        })
        .collect();
    Ok(Json(works))
}

#[derive(Serialize)]
pub struct EditionResponse {
    #[serde(flatten)]
    edition: db::Edition,
    isbns: Vec<String>,
    covers: Vec<db::CoverMetadata>,
    #[serde(skip_serializing_if = "Option::is_none")]
    popularity: Option<db::EditionPopularity>,
}

async fn get_edition(
    State(state): State<Arc<AppState>>,
    Path(slug): Path<String>,
) -> Result<Json<EditionResponse>, AppError> {
    let id = base36::decode(&slug).ok_or(AppError::NotFound)? as i32;

    let edition = db::get_edition_by_id(&state.db, id)
        .await?
        .ok_or(AppError::NotFound)?;

    let (isbns, covers, popularity) = tokio::join!(
        db::get_edition_isbns(&state.db, edition.id),
        db::get_edition_covers(&state.db, edition.id),
        db::get_edition_popularity(&state.db, edition.id)
    );

    Ok(Json(EditionResponse {
        edition,
        isbns: isbns?,
        covers: covers?,
        popularity: popularity?,
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
