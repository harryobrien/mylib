use std::sync::Arc;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};

use crate::{db, search, AppState};

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
    const BATCH_SIZE: i64 = 10000;

    tracing::info!("Indexing works...");
    let mut writer = state.search.works.writer()?;
    let total_works = db::count_works(&state.db).await?;
    let mut offset = 0i64;

    while offset < total_works {
        let works = db::get_works_for_indexing(&state.db, offset, BATCH_SIZE).await?;
        for w in &works {
            let year = extract_year(&w.first_publish_date);
            let mut doc = tantivy::TantivyDocument::new();
            doc.add_i64(state.search.works.fields.id, w.id as i64);
            doc.add_text(state.search.works.fields.key, &w.key);
            doc.add_text(state.search.works.fields.title, &w.title);
            if let Some(ref s) = w.subtitle { doc.add_text(state.search.works.fields.subtitle, s); }
            if let Some(ref d) = w.description { doc.add_text(state.search.works.fields.description, d); }
            if let Some(ref s) = w.subjects { doc.add_text(state.search.works.fields.subjects, s); }
            if let Some(ref a) = w.author_names { doc.add_text(state.search.works.fields.author_names, a); }
            if let Some(y) = year { doc.add_i64(state.search.works.fields.first_publish_year, y); }
            writer.add_document(doc)?;
        }
        offset += BATCH_SIZE;
        tracing::info!("  Works: {offset}/{total_works}");
    }
    writer.commit()?;

    tracing::info!("Indexing authors...");
    let mut writer = state.search.authors.writer()?;
    let total_authors = db::count_authors(&state.db).await?;
    offset = 0;

    while offset < total_authors {
        let authors = db::get_authors_for_indexing(&state.db, offset, BATCH_SIZE).await?;
        for a in &authors {
            let mut doc = tantivy::TantivyDocument::new();
            doc.add_i64(state.search.authors.fields.id, a.id as i64);
            doc.add_text(state.search.authors.fields.key, &a.key);
            doc.add_text(state.search.authors.fields.name, &a.name);
            if let Some(ref alt) = a.alternate_names { doc.add_text(state.search.authors.fields.alternate_names, alt); }
            if let Some(ref bio) = a.bio { doc.add_text(state.search.authors.fields.bio, bio); }
            writer.add_document(doc)?;
        }
        offset += BATCH_SIZE;
        tracing::info!("  Authors: {offset}/{total_authors}");
    }
    writer.commit()?;

    tracing::info!("Indexing editions...");
    let mut writer = state.search.editions.writer()?;
    let total_editions = db::count_editions(&state.db).await?;
    offset = 0;

    while offset < total_editions {
        let editions = db::get_editions_for_indexing(&state.db, offset, BATCH_SIZE).await?;
        for e in &editions {
            let year = extract_year(&e.publish_date);
            let mut doc = tantivy::TantivyDocument::new();
            doc.add_i64(state.search.editions.fields.id, e.id as i64);
            doc.add_text(state.search.editions.fields.key, &e.key);
            doc.add_text(state.search.editions.fields.work_key, &e.work_key);
            doc.add_text(state.search.editions.fields.title, &e.title);
            if let Some(ref s) = e.subtitle { doc.add_text(state.search.editions.fields.subtitle, s); }
            if let Some(ref i) = e.isbns { doc.add_text(state.search.editions.fields.isbns, i); }
            if let Some(ref p) = e.publishers { doc.add_text(state.search.editions.fields.publishers, p); }
            if let Some(y) = year { doc.add_i64(state.search.editions.fields.publish_year, y); }
            writer.add_document(doc)?;
        }
        offset += BATCH_SIZE;
        tracing::info!("  Editions: {offset}/{total_editions}");
    }
    writer.commit()?;

    tracing::info!("Reindex complete");
    Ok(Json(ReindexResponse {
        works: total_works,
        authors: total_authors,
        editions: total_editions,
    }))
}

fn extract_year(date: &Option<String>) -> Option<i64> {
    date.as_ref().and_then(|d| {
        d.chars()
            .collect::<String>()
            .split(|c: char| !c.is_ascii_digit())
            .find(|s| s.len() == 4)
            .and_then(|y| y.parse().ok())
    })
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
