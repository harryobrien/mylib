use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::{auth, base36, db, indexer, search, AppState};

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        // Search endpoints
        .route("/search", get(search_all))
        .route("/search/works", get(search_works))
        .route("/search/authors", get(search_authors))
        .route("/search/editions", get(search_editions))
        // Resource endpoints (slug = base36 encoded ID)
        .route("/works/{slug}", get(get_work).patch(patch_work))
        .route("/works/{slug}/authors", get(get_work_authors))
        .route("/works/{slug}/editions", get(get_work_editions))
        .route("/authors/{slug}", get(get_author).patch(patch_author))
        .route("/authors/{slug}/works", get(get_author_works))
        .route("/editions/{slug}", get(get_edition).patch(patch_edition))
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
    let works_handle =
        tokio::task::spawn_blocking(move || state_clone.search.works.search(&q1, limit));

    let state_clone = state.clone();
    let q2 = q.clone();
    let authors_handle =
        tokio::task::spawn_blocking(move || state_clone.search.authors.search(&q2, limit));

    let state_clone = state.clone();
    let q3 = q.clone();
    let editions_handle =
        tokio::task::spawn_blocking(move || state_clone.search.editions.search(&q3, limit));

    let (works_res, authors_res, editions_res) =
        tokio::join!(works_handle, authors_handle, editions_handle);

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
    #[serde(skip_serializing_if = "Option::is_none")]
    popularity: Option<db::EditionPopularity>,
}

async fn get_work(
    State(state): State<Arc<AppState>>,
    Path(slug): Path<String>,
) -> Result<Json<WorkResponse>, AppError> {
    let id = base36::decode(&slug).ok_or(AppError::NotFound)? as i32;

    let work = db::get_work_by_id(&state.db, id)
        .await?
        .ok_or(AppError::NotFound)?;

    let (authors, editions, popularity, edition_pops) = tokio::join!(
        db::get_work_authors(&state.db, work.id),
        db::get_work_editions(&state.db, work.id),
        db::get_work_popularity(&state.db, work.id),
        db::get_edition_popularities_for_work(&state.db, work.id)
    );

    let authors = authors?
        .into_iter()
        .map(|a| AuthorSummary {
            slug: base36::encode(a.id as i64),
            author: a,
        })
        .collect();

    let edition_pops: std::collections::HashMap<i32, db::EditionPopularity> = edition_pops?
        .into_iter()
        .map(|ep| {
            (
                ep.edition_id,
                db::EditionPopularity {
                    ratings_count: ep.ratings_count,
                    rating_avg: ep.rating_avg,
                    want_to_read: ep.want_to_read,
                    currently_reading: ep.currently_reading,
                    already_read: ep.already_read,
                },
            )
        })
        .collect();

    let editions = editions?
        .into_iter()
        .map(|e| {
            let pop = edition_pops.get(&e.id).cloned();
            EditionSummary {
                slug: base36::encode(e.id as i64),
                edition: e,
                popularity: pop,
            }
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
) -> Result<Json<Vec<AuthorWorkSummary>>, AppError> {
    let id = base36::decode(&slug).ok_or(AppError::NotFound)? as i32;

    let works = db::get_author_works(&state.db, id).await?;
    let works = works
        .into_iter()
        .map(|w| AuthorWorkSummary {
            slug: base36::encode(w.id as i64),
            work: w,
        })
        .collect();
    Ok(Json(works))
}

#[derive(Serialize)]
pub struct AuthorWorkSummary {
    slug: String,
    #[serde(flatten)]
    work: db::WorkWithPopularity,
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

// --- PATCH endpoints ---

const MAX_TITLE: usize = 500;
const MAX_SUBTITLE: usize = 500;
const MAX_DESCRIPTION: usize = 50000;
const MAX_BIO: usize = 50000;
const MAX_NAME: usize = 200;
const MAX_DATE: usize = 50;
const MAX_FORMAT: usize = 50;
const MAX_PAGES: i32 = 50000;

fn validate_len(field: &str, value: &Option<String>, max: usize) -> Result<(), AppError> {
    if let Some(v) = value {
        if v.len() > max {
            return Err(AppError::Validation(format!(
                "{field} exceeds max length of {max}"
            )));
        }
    }
    Ok(())
}

fn validate_positive(field: &str, value: Option<i32>) -> Result<(), AppError> {
    if let Some(v) = value {
        if v <= 0 {
            return Err(AppError::Validation(format!("{field} must be positive")));
        }
        if v > MAX_PAGES {
            return Err(AppError::Validation(format!(
                "{field} exceeds max of {MAX_PAGES}"
            )));
        }
    }
    Ok(())
}

#[derive(Deserialize)]
pub struct PatchWork {
    title: Option<String>,
    subtitle: Option<String>,
    description: Option<String>,
    first_publish_date: Option<String>,
}

impl PatchWork {
    fn validate(&self) -> Result<(), AppError> {
        validate_len("title", &self.title, MAX_TITLE)?;
        validate_len("subtitle", &self.subtitle, MAX_SUBTITLE)?;
        validate_len("description", &self.description, MAX_DESCRIPTION)?;
        validate_len("first_publish_date", &self.first_publish_date, MAX_DATE)?;
        Ok(())
    }
}

#[derive(Deserialize)]
pub struct PatchAuthor {
    name: Option<String>,
    fuller_name: Option<String>,
    bio: Option<String>,
    birth_date: Option<String>,
    death_date: Option<String>,
}

impl PatchAuthor {
    fn validate(&self) -> Result<(), AppError> {
        validate_len("name", &self.name, MAX_NAME)?;
        validate_len("fuller_name", &self.fuller_name, MAX_NAME)?;
        validate_len("bio", &self.bio, MAX_BIO)?;
        validate_len("birth_date", &self.birth_date, MAX_DATE)?;
        validate_len("death_date", &self.death_date, MAX_DATE)?;
        Ok(())
    }
}

#[derive(Deserialize)]
pub struct PatchEdition {
    title: Option<String>,
    subtitle: Option<String>,
    publish_date: Option<String>,
    physical_format: Option<String>,
    number_of_pages: Option<i32>,
}

impl PatchEdition {
    fn validate(&self) -> Result<(), AppError> {
        validate_len("title", &self.title, MAX_TITLE)?;
        validate_len("subtitle", &self.subtitle, MAX_SUBTITLE)?;
        validate_len("publish_date", &self.publish_date, MAX_DATE)?;
        validate_len("physical_format", &self.physical_format, MAX_FORMAT)?;
        validate_positive("number_of_pages", self.number_of_pages)?;
        Ok(())
    }
}

async fn get_user_id_required(state: &AppState, headers: &HeaderMap) -> Result<i32, AppError> {
    let token = auth::extract_session_token(headers).ok_or(AppError::Unauthorized)?;
    sqlx::query_scalar::<_, i32>(
        "SELECT user_id FROM sessions WHERE token = $1 AND expires_at > NOW()",
    )
    .bind(&token)
    .fetch_optional(&state.db)
    .await?
    .ok_or(AppError::Unauthorized)
}

async fn patch_work(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(slug): Path<String>,
    Json(patch): Json<PatchWork>,
) -> Result<Json<serde_json::Value>, AppError> {
    patch.validate()?;
    let id = base36::decode(&slug).ok_or(AppError::NotFound)? as i32;
    let user_id = get_user_id_required(&state, &headers).await?;

    let current = sqlx::query_as::<_, (String, Option<String>, Option<String>, Option<String>)>(
        "SELECT title, subtitle, description, first_publish_date FROM works WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(&state.db)
    .await?
    .ok_or(AppError::NotFound)?;

    let old_values = serde_json::json!({
        "title": current.0,
        "subtitle": current.1,
        "description": current.2,
        "first_publish_date": current.3,
    });

    let new_title = patch.title.as_deref().unwrap_or(&current.0);
    let new_subtitle = patch.subtitle.as_ref().or(current.1.as_ref());
    let new_description = patch.description.as_ref().or(current.2.as_ref());
    let new_first_publish_date = patch.first_publish_date.as_ref().or(current.3.as_ref());

    sqlx::query(
        r#"UPDATE works SET title = $1, subtitle = $2, description = $3, first_publish_date = $4 WHERE id = $5"#,
    )
    .bind(new_title)
    .bind(new_subtitle)
    .bind(new_description)
    .bind(new_first_publish_date)
    .bind(id)
    .execute(&state.db)
    .await?;

    let new_values = serde_json::json!({
        "title": new_title,
        "subtitle": new_subtitle,
        "description": new_description,
        "first_publish_date": new_first_publish_date,
    });

    sqlx::query(
        r#"INSERT INTO revisions (entity_type, entity_id, user_id, old_values, new_values)
           VALUES ('work', $1, $2, $3, $4)"#,
    )
    .bind(id)
    .bind(user_id)
    .bind(&old_values)
    .bind(&new_values)
    .execute(&state.db)
    .await?;

    indexer::reindex_work(&state.db, &state.search, id).await?;

    Ok(Json(serde_json::json!({ "success": true, "slug": slug })))
}

async fn patch_author(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(slug): Path<String>,
    Json(patch): Json<PatchAuthor>,
) -> Result<Json<serde_json::Value>, AppError> {
    patch.validate()?;
    let id = base36::decode(&slug).ok_or(AppError::NotFound)? as i32;
    let user_id = get_user_id_required(&state, &headers).await?;

    let current = sqlx::query_as::<
        _,
        (
            String,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
        ),
    >(
        "SELECT name, fuller_name, bio, birth_date, death_date FROM authors WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(&state.db)
    .await?
    .ok_or(AppError::NotFound)?;

    let old_values = serde_json::json!({
        "name": current.0,
        "fuller_name": current.1,
        "bio": current.2,
        "birth_date": current.3,
        "death_date": current.4,
    });

    let new_name = patch.name.as_deref().unwrap_or(&current.0);
    let new_fuller_name = patch.fuller_name.as_ref().or(current.1.as_ref());
    let new_bio = patch.bio.as_ref().or(current.2.as_ref());
    let new_birth_date = patch.birth_date.as_ref().or(current.3.as_ref());
    let new_death_date = patch.death_date.as_ref().or(current.4.as_ref());

    sqlx::query(
        r#"UPDATE authors SET name = $1, fuller_name = $2, bio = $3, birth_date = $4, death_date = $5 WHERE id = $6"#,
    )
    .bind(new_name)
    .bind(new_fuller_name)
    .bind(new_bio)
    .bind(new_birth_date)
    .bind(new_death_date)
    .bind(id)
    .execute(&state.db)
    .await?;

    let new_values = serde_json::json!({
        "name": new_name,
        "fuller_name": new_fuller_name,
        "bio": new_bio,
        "birth_date": new_birth_date,
        "death_date": new_death_date,
    });

    sqlx::query(
        r#"INSERT INTO revisions (entity_type, entity_id, user_id, old_values, new_values)
           VALUES ('author', $1, $2, $3, $4)"#,
    )
    .bind(id)
    .bind(user_id)
    .bind(&old_values)
    .bind(&new_values)
    .execute(&state.db)
    .await?;

    indexer::reindex_author(&state.db, &state.search, id).await?;

    Ok(Json(serde_json::json!({ "success": true, "slug": slug })))
}

async fn patch_edition(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(slug): Path<String>,
    Json(patch): Json<PatchEdition>,
) -> Result<Json<serde_json::Value>, AppError> {
    patch.validate()?;
    let id = base36::decode(&slug).ok_or(AppError::NotFound)? as i32;
    let user_id = get_user_id_required(&state, &headers).await?;

    let current = sqlx::query_as::<_, (String, Option<String>, Option<String>, Option<String>, Option<i32>)>(
        "SELECT title, subtitle, publish_date, physical_format, number_of_pages FROM editions WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(&state.db)
    .await?
    .ok_or(AppError::NotFound)?;

    let old_values = serde_json::json!({
        "title": current.0,
        "subtitle": current.1,
        "publish_date": current.2,
        "physical_format": current.3,
        "number_of_pages": current.4,
    });

    let new_title = patch.title.as_deref().unwrap_or(&current.0);
    let new_subtitle = patch.subtitle.as_ref().or(current.1.as_ref());
    let new_publish_date = patch.publish_date.as_ref().or(current.2.as_ref());
    let new_physical_format = patch.physical_format.as_ref().or(current.3.as_ref());
    let new_number_of_pages = patch.number_of_pages.or(current.4);

    sqlx::query(
        r#"UPDATE editions SET title = $1, subtitle = $2, publish_date = $3, physical_format = $4, number_of_pages = $5 WHERE id = $6"#,
    )
    .bind(new_title)
    .bind(new_subtitle)
    .bind(new_publish_date)
    .bind(new_physical_format)
    .bind(new_number_of_pages)
    .bind(id)
    .execute(&state.db)
    .await?;

    let new_values = serde_json::json!({
        "title": new_title,
        "subtitle": new_subtitle,
        "publish_date": new_publish_date,
        "physical_format": new_physical_format,
        "number_of_pages": new_number_of_pages,
    });

    sqlx::query(
        r#"INSERT INTO revisions (entity_type, entity_id, user_id, old_values, new_values)
           VALUES ('edition', $1, $2, $3, $4)"#,
    )
    .bind(id)
    .bind(user_id)
    .bind(&old_values)
    .bind(&new_values)
    .execute(&state.db)
    .await?;

    indexer::reindex_edition(&state.db, &state.search, id).await?;

    Ok(Json(serde_json::json!({ "success": true, "slug": slug })))
}

pub enum AppError {
    NotFound,
    Unauthorized,
    Validation(String),
    Internal(anyhow::Error),
}

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        match self {
            AppError::NotFound => (StatusCode::NOT_FOUND, "Not found").into_response(),
            AppError::Unauthorized => (StatusCode::UNAUTHORIZED, "Unauthorized").into_response(),
            AppError::Validation(msg) => (StatusCode::BAD_REQUEST, msg).into_response(),
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
