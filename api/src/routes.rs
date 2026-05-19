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

#[derive(Serialize)]
struct PatchResponse {
    success: bool,
    slug: String,
}

#[derive(Serialize)]
struct ReviewItem {
    user_id: i32,
    username: String,
    display_name: Option<String>,
    rating: f32,
    review_text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    edition_slug: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    edition_title: Option<String>,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Serialize)]
struct ReviewsResponse {
    reviews: Vec<ReviewItem>,
}

#[derive(Serialize)]
struct ProfileReviewItem {
    edition_slug: String,
    work_slug: String,
    rating: f32,
    review_text: Option<String>,
    edition_title: String,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Serialize)]
struct ReadingStats {
    want_to_read: i64,
    reading: i64,
    finished: i64,
    did_not_finish: i64,
}

#[derive(Serialize)]
struct ProfileListItem {
    id: i32,
    title: String,
    work_count: i64,
}

#[derive(Serialize)]
struct UserProfileResponse {
    username: String,
    display_name: Option<String>,
    bio: Option<String>,
    created_at: chrono::DateTime<chrono::Utc>,
    reading_stats: ReadingStats,
    reviews: Vec<ProfileReviewItem>,
    lists: Vec<ProfileListItem>,
    followers_count: i64,
    following_count: i64,
}

#[derive(Serialize)]
struct ListWorkItem {
    slug: String,
    title: String,
    description: Option<String>,
    cover_id: Option<i64>,
    position: i32,
}

#[derive(Serialize)]
struct ListDetailResponse {
    id: i32,
    title: String,
    description: Option<String>,
    username: String,
    display_name: Option<String>,
    created_at: chrono::DateTime<chrono::Utc>,
    works: Vec<ListWorkItem>,
}

#[derive(Serialize)]
struct ListSummaryItem {
    id: i32,
    title: String,
    description: Option<String>,
    work_count: i64,
}

#[derive(Serialize)]
struct ListsResponse {
    lists: Vec<ListSummaryItem>,
}

#[derive(Serialize)]
struct WorkListItem {
    id: i32,
    title: String,
    username: String,
    display_name: Option<String>,
}

#[derive(Serialize)]
struct WorkListsResponse {
    lists: Vec<WorkListItem>,
}

#[derive(Serialize)]
struct PopularWorkItem {
    slug: String,
    title: String,
    cover_id: Option<i64>,
    rating_avg: Option<f32>,
    ratings_count: i32,
}

#[derive(Serialize)]
struct RecentReviewItem {
    username: String,
    display_name: Option<String>,
    rating: f32,
    review_text: Option<String>,
    edition_title: String,
    work_slug: String,
    edition_slug: String,
    cover_id: Option<i64>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Serialize)]
struct RecentListItem {
    id: i32,
    title: String,
    username: String,
    display_name: Option<String>,
    work_count: i64,
}

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
        .route("/editions/{slug}/reviews", get(get_edition_reviews))
        .route("/works/{slug}/reviews", get(get_work_reviews))
        .route("/users/{username}", get(get_user_profile))
        .route("/users/{username}/lists", get(get_user_lists))
        .route("/lists/{id}", get(get_list))
        .route("/works/{slug}/lists", get(get_work_lists))
        .route("/discover/popular", get(get_popular_works))
        .route("/discover/reviews", get(get_recent_reviews))
        .route("/discover/lists", get(get_recent_lists))
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

#[derive(sqlx::FromRow)]
struct WorkCurrentRow {
    title: String,
    subtitle: Option<String>,
    description: Option<String>,
    first_publish_date: Option<String>,
}

#[derive(sqlx::FromRow)]
struct AuthorCurrentRow {
    name: String,
    fuller_name: Option<String>,
    bio: Option<String>,
    birth_date: Option<String>,
    death_date: Option<String>,
}

#[derive(sqlx::FromRow)]
struct EditionCurrentRow {
    title: String,
    subtitle: Option<String>,
    publish_date: Option<String>,
    physical_format: Option<String>,
    number_of_pages: Option<i32>,
}

#[derive(sqlx::FromRow)]
struct EditionReviewRow {
    user_id: i32,
    username: String,
    display_name: Option<String>,
    rating: f32,
    review_text: Option<String>,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(sqlx::FromRow)]
struct WorkReviewRow {
    user_id: i32,
    username: String,
    display_name: Option<String>,
    edition_id: i32,
    rating: f32,
    review_text: Option<String>,
    title: String,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(sqlx::FromRow)]
struct UserRow {
    id: i32,
    username: String,
    display_name: Option<String>,
    bio: Option<String>,
    created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(sqlx::FromRow)]
struct StatusCountRow {
    status: String,
    count: i64,
}

#[derive(sqlx::FromRow)]
struct ProfileReviewRow {
    edition_id: i32,
    work_id: i32,
    rating: f32,
    review_text: Option<String>,
    title: String,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(sqlx::FromRow)]
struct ProfileListRow {
    id: i32,
    title: String,
    work_count: i64,
}

#[derive(sqlx::FromRow)]
struct ListHeaderRow {
    id: i32,
    title: String,
    description: Option<String>,
    username: String,
    display_name: Option<String>,
    created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(sqlx::FromRow)]
struct ListWorkRow {
    id: i32,
    title: String,
    description: Option<String>,
    cover_id: Option<i64>,
    position: i32,
}

#[derive(sqlx::FromRow)]
struct UserListRow {
    id: i32,
    title: String,
    description: Option<String>,
    work_count: i64,
}

#[derive(sqlx::FromRow)]
#[allow(dead_code)]
struct WorkListRow {
    id: i32,
    title: String,
    username: String,
    display_name: Option<String>,
    follower_count: i64,
}

#[derive(sqlx::FromRow)]
struct PopularWorkRow {
    id: i32,
    title: String,
    cover_id: Option<i64>,
    rating_avg: Option<f32>,
    ratings_count: i32,
}

#[derive(sqlx::FromRow)]
struct RecentReviewRow {
    username: String,
    display_name: Option<String>,
    rating: f32,
    review_text: Option<String>,
    title: String,
    id: i32,
    work_id: i32,
    cover_id: Option<i64>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(sqlx::FromRow)]
struct RecentListRow {
    id: i32,
    title: String,
    username: String,
    display_name: Option<String>,
    work_count: i64,
}

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

fn get_user_id_required(headers: &HeaderMap) -> Result<i32, AppError> {
    let token = auth::extract_bearer_token(headers).ok_or(AppError::Unauthorized)?;
    let claims = jsonwebtoken::decode::<serde_json::Value>(
        &token,
        &jsonwebtoken::DecodingKey::from_secret(
            &std::env::var("JWT_SECRET")
                .unwrap_or_else(|_| "dev-secret-change-in-production".into())
                .into_bytes(),
        ),
        &jsonwebtoken::Validation::default(),
    )
    .map_err(|_| AppError::Unauthorized)?;
    claims
        .claims
        .get("sub")
        .and_then(|v| v.as_i64())
        .map(|id| id as i32)
        .ok_or(AppError::Unauthorized)
}

async fn patch_work(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(slug): Path<String>,
    Json(patch): Json<PatchWork>,
) -> Result<Json<PatchResponse>, AppError> {
    patch.validate()?;
    let id = base36::decode(&slug).ok_or(AppError::NotFound)? as i32;
    let user_id = get_user_id_required(&headers)?;

    let current = sqlx::query_as::<_, WorkCurrentRow>(
        "SELECT title, subtitle, description, first_publish_date FROM works WHERE id = ?",
    )
    .bind(id)
    .fetch_optional(&state.db)
    .await?
    .ok_or(AppError::NotFound)?;

    let old_values = serde_json::json!({
        "title": current.title,
        "subtitle": current.subtitle,
        "description": current.description,
        "first_publish_date": current.first_publish_date,
    });

    let new_title = patch.title.as_deref().unwrap_or(&current.title);
    let new_subtitle = patch.subtitle.as_ref().or(current.subtitle.as_ref());
    let new_description = patch.description.as_ref().or(current.description.as_ref());
    let new_first_publish_date = patch
        .first_publish_date
        .as_ref()
        .or(current.first_publish_date.as_ref());

    sqlx::query(
        r#"UPDATE works SET title = ?, subtitle = ?, description = ?, first_publish_date = ? WHERE id = ?"#,
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
           VALUES ('work', ?, ?, ?, ?)"#,
    )
    .bind(id)
    .bind(user_id)
    .bind(&old_values)
    .bind(&new_values)
    .execute(&state.db)
    .await?;

    indexer::reindex_work(&state.db, &state.search, id).await?;

    Ok(Json(PatchResponse {
        success: true,
        slug,
    }))
}

async fn patch_author(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(slug): Path<String>,
    Json(patch): Json<PatchAuthor>,
) -> Result<Json<PatchResponse>, AppError> {
    patch.validate()?;
    let id = base36::decode(&slug).ok_or(AppError::NotFound)? as i32;
    let user_id = get_user_id_required(&headers)?;

    let current = sqlx::query_as::<_, AuthorCurrentRow>(
        "SELECT name, fuller_name, bio, birth_date, death_date FROM authors WHERE id = ?",
    )
    .bind(id)
    .fetch_optional(&state.db)
    .await?
    .ok_or(AppError::NotFound)?;

    let old_values = serde_json::json!({
        "name": current.name,
        "fuller_name": current.fuller_name,
        "bio": current.bio,
        "birth_date": current.birth_date,
        "death_date": current.death_date,
    });

    let new_name = patch.name.as_deref().unwrap_or(&current.name);
    let new_fuller_name = patch.fuller_name.as_ref().or(current.fuller_name.as_ref());
    let new_bio = patch.bio.as_ref().or(current.bio.as_ref());
    let new_birth_date = patch.birth_date.as_ref().or(current.birth_date.as_ref());
    let new_death_date = patch.death_date.as_ref().or(current.death_date.as_ref());

    sqlx::query(
        r#"UPDATE authors SET name = ?, fuller_name = ?, bio = ?, birth_date = ?, death_date = ? WHERE id = ?"#,
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
           VALUES ('author', ?, ?, ?, ?)"#,
    )
    .bind(id)
    .bind(user_id)
    .bind(&old_values)
    .bind(&new_values)
    .execute(&state.db)
    .await?;

    indexer::reindex_author(&state.db, &state.search, id).await?;

    Ok(Json(PatchResponse {
        success: true,
        slug,
    }))
}

async fn patch_edition(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(slug): Path<String>,
    Json(patch): Json<PatchEdition>,
) -> Result<Json<PatchResponse>, AppError> {
    patch.validate()?;
    let id = base36::decode(&slug).ok_or(AppError::NotFound)? as i32;
    let user_id = get_user_id_required(&headers)?;

    let current = sqlx::query_as::<_, EditionCurrentRow>(
        "SELECT title, subtitle, publish_date, physical_format, number_of_pages FROM editions WHERE id = ?",
    )
    .bind(id)
    .fetch_optional(&state.db)
    .await?
    .ok_or(AppError::NotFound)?;

    let old_values = serde_json::json!({
        "title": current.title,
        "subtitle": current.subtitle,
        "publish_date": current.publish_date,
        "physical_format": current.physical_format,
        "number_of_pages": current.number_of_pages,
    });

    let new_title = patch.title.as_deref().unwrap_or(&current.title);
    let new_subtitle = patch.subtitle.as_ref().or(current.subtitle.as_ref());
    let new_publish_date = patch
        .publish_date
        .as_ref()
        .or(current.publish_date.as_ref());
    let new_physical_format = patch
        .physical_format
        .as_ref()
        .or(current.physical_format.as_ref());
    let new_number_of_pages = patch.number_of_pages.or(current.number_of_pages);

    sqlx::query(
        r#"UPDATE editions SET title = ?, subtitle = ?, publish_date = ?, physical_format = ?, number_of_pages = ? WHERE id = ?"#,
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
           VALUES ('edition', ?, ?, ?, ?)"#,
    )
    .bind(id)
    .bind(user_id)
    .bind(&old_values)
    .bind(&new_values)
    .execute(&state.db)
    .await?;

    indexer::reindex_edition(&state.db, &state.search, id).await?;

    Ok(Json(PatchResponse {
        success: true,
        slug,
    }))
}

async fn get_edition_reviews(
    State(state): State<Arc<AppState>>,
    Path(slug): Path<String>,
) -> Result<Json<ReviewsResponse>, AppError> {
    let edition_id = base36::decode(&slug).ok_or(AppError::NotFound)? as i32;

    let rows = sqlx::query_as::<_, EditionReviewRow>(
        r#"
        SELECT ur.user_id, u.username, u.display_name, ur.rating, ur.review_text,
               ur.created_at, ur.updated_at
        FROM user_reviews ur
        JOIN users u ON ur.user_id = u.id
        WHERE ur.edition_id = ?
        ORDER BY ur.updated_at DESC
        "#,
    )
    .bind(edition_id)
    .fetch_all(&state.db)
    .await?;

    let reviews = rows
        .into_iter()
        .map(|row| ReviewItem {
            user_id: row.user_id,
            username: row.username,
            display_name: row.display_name,
            rating: row.rating,
            review_text: row.review_text,
            edition_slug: None,
            edition_title: None,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
        .collect();

    Ok(Json(ReviewsResponse { reviews }))
}

async fn get_work_reviews(
    State(state): State<Arc<AppState>>,
    Path(slug): Path<String>,
) -> Result<Json<ReviewsResponse>, AppError> {
    let work_id = base36::decode(&slug).ok_or(AppError::NotFound)? as i32;

    let rows = sqlx::query_as::<_, WorkReviewRow>(
        r#"
        SELECT ur.user_id, u.username, u.display_name, ur.edition_id, ur.rating,
               ur.review_text, e.title, ur.created_at, ur.updated_at
        FROM user_reviews ur
        JOIN users u ON ur.user_id = u.id
        JOIN editions e ON ur.edition_id = e.id
        WHERE e.work_id = ?
        ORDER BY ur.updated_at DESC
        "#,
    )
    .bind(work_id)
    .fetch_all(&state.db)
    .await?;

    let reviews = rows
        .into_iter()
        .map(|row| ReviewItem {
            user_id: row.user_id,
            username: row.username,
            display_name: row.display_name,
            rating: row.rating,
            review_text: row.review_text,
            edition_slug: Some(base36::encode(row.edition_id as i64)),
            edition_title: Some(row.title),
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
        .collect();

    Ok(Json(ReviewsResponse { reviews }))
}

async fn get_user_profile(
    State(state): State<Arc<AppState>>,
    Path(username): Path<String>,
) -> Result<Json<UserProfileResponse>, AppError> {
    let user = sqlx::query_as::<_, UserRow>(
        "SELECT id, username, display_name, bio, created_at FROM users WHERE LOWER(username) = LOWER(?)",
    )
    .bind(&username)
    .fetch_optional(&state.db)
    .await?
    .ok_or(AppError::NotFound)?;

    let (user_id, username, display_name, bio, created_at) = (
        user.id,
        user.username,
        user.display_name,
        user.bio,
        user.created_at,
    );

    let stats = sqlx::query_as::<_, StatusCountRow>(
        r#"
        SELECT status, COUNT(*) as count
        FROM user_editions
        WHERE user_id = ?
        GROUP BY status
        "#,
    )
    .bind(user_id)
    .fetch_all(&state.db)
    .await?;

    let mut reading_stats = ReadingStats {
        want_to_read: 0,
        reading: 0,
        finished: 0,
        did_not_finish: 0,
    };
    for row in &stats {
        match row.status.as_str() {
            "want_to_read" => reading_stats.want_to_read = row.count,
            "reading" => reading_stats.reading = row.count,
            "finished" => reading_stats.finished = row.count,
            "did_not_finish" => reading_stats.did_not_finish = row.count,
            _ => {}
        }
    }

    let rows = sqlx::query_as::<_, ProfileReviewRow>(
        r#"
        SELECT ur.edition_id, e.work_id, ur.rating, ur.review_text, e.title,
               ur.created_at, ur.updated_at
        FROM user_reviews ur
        JOIN editions e ON ur.edition_id = e.id
        WHERE ur.user_id = ?
        ORDER BY ur.updated_at DESC
        "#,
    )
    .bind(user_id)
    .fetch_all(&state.db)
    .await?;

    let reviews = rows
        .into_iter()
        .map(|row| ProfileReviewItem {
            edition_slug: base36::encode(row.edition_id as i64),
            work_slug: base36::encode(row.work_id as i64),
            rating: row.rating,
            review_text: row.review_text,
            edition_title: row.title,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
        .collect();

    let followers_count =
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM user_follows WHERE following_id = ?")
            .bind(user_id)
            .fetch_one(&state.db)
            .await?;

    let following_count =
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM user_follows WHERE follower_id = ?")
            .bind(user_id)
            .fetch_one(&state.db)
            .await?;

    let lists = sqlx::query_as::<_, ProfileListRow>(
        r#"
        SELECT ul.id, ul.title,
               (SELECT COUNT(*) FROM user_list_works WHERE list_id = ul.id) as work_count
        FROM user_lists ul
        WHERE ul.user_id = ?
        ORDER BY ul.updated_at DESC
        "#,
    )
    .bind(user_id)
    .fetch_all(&state.db)
    .await?;

    let lists = lists
        .into_iter()
        .map(|row| ProfileListItem {
            id: row.id,
            title: row.title,
            work_count: row.work_count,
        })
        .collect();

    Ok(Json(UserProfileResponse {
        username,
        display_name,
        bio,
        created_at,
        reading_stats,
        reviews,
        lists,
        followers_count,
        following_count,
    }))
}

async fn get_list(
    State(state): State<Arc<AppState>>,
    Path(list_id): Path<i32>,
) -> Result<Json<ListDetailResponse>, AppError> {
    let list = sqlx::query_as::<_, ListHeaderRow>(
        r#"
        SELECT ul.id, ul.title, ul.description, u.username, u.display_name, ul.created_at
        FROM user_lists ul
        JOIN users u ON ul.user_id = u.id
        WHERE ul.id = ?
        "#,
    )
    .bind(list_id)
    .fetch_optional(&state.db)
    .await?
    .ok_or(AppError::NotFound)?;

    let (id, title, description, username, display_name, created_at) = (
        list.id,
        list.title,
        list.description,
        list.username,
        list.display_name,
        list.created_at,
    );

    let works = sqlx::query_as::<_, ListWorkRow>(
        r#"
        SELECT w.id, w.title, w.description, wc.cover_id, ulw.position
        FROM user_list_works ulw
        JOIN works w ON ulw.work_id = w.id
        LEFT JOIN work_covers wc ON w.id = wc.work_id AND wc.position = 0
        WHERE ulw.list_id = ?
        ORDER BY ulw.position ASC
        "#,
    )
    .bind(list_id)
    .fetch_all(&state.db)
    .await?;

    let works = works
        .into_iter()
        .map(|row| ListWorkItem {
            slug: base36::encode(row.id as i64),
            title: row.title,
            description: row.description.map(|d| {
                if d.len() > 200 {
                    format!("{}...", &d[..200])
                } else {
                    d
                }
            }),
            cover_id: row.cover_id,
            position: row.position,
        })
        .collect();

    Ok(Json(ListDetailResponse {
        id,
        title,
        description,
        username,
        display_name,
        created_at,
        works,
    }))
}

async fn get_user_lists(
    State(state): State<Arc<AppState>>,
    Path(username): Path<String>,
) -> Result<Json<ListsResponse>, AppError> {
    let user_id =
        sqlx::query_scalar::<_, i32>("SELECT id FROM users WHERE LOWER(username) = LOWER(?)")
            .bind(&username)
            .fetch_optional(&state.db)
            .await?
            .ok_or(AppError::NotFound)?;

    let lists = sqlx::query_as::<_, UserListRow>(
        r#"
        SELECT ul.id, ul.title, ul.description,
               (SELECT COUNT(*) FROM user_list_works WHERE list_id = ul.id) as work_count
        FROM user_lists ul
        WHERE ul.user_id = ?
        ORDER BY ul.updated_at DESC
        "#,
    )
    .bind(user_id)
    .fetch_all(&state.db)
    .await?;

    let lists = lists
        .into_iter()
        .map(|row| ListSummaryItem {
            id: row.id,
            title: row.title,
            description: row.description,
            work_count: row.work_count,
        })
        .collect();

    Ok(Json(ListsResponse { lists }))
}

async fn get_work_lists(
    State(state): State<Arc<AppState>>,
    Path(slug): Path<String>,
) -> Result<Json<WorkListsResponse>, AppError> {
    let work_id = base36::decode(&slug).ok_or(AppError::NotFound)? as i32;

    let lists = sqlx::query_as::<_, WorkListRow>(
        r#"
        SELECT ul.id, ul.title, u.username, u.display_name,
               (SELECT COUNT(*) FROM user_follows WHERE following_id = u.id) as follower_count
        FROM user_list_works ulw
        JOIN user_lists ul ON ulw.list_id = ul.id
        JOIN users u ON ul.user_id = u.id
        WHERE ulw.work_id = ?
        ORDER BY follower_count DESC, ul.created_at DESC
        "#,
    )
    .bind(work_id)
    .fetch_all(&state.db)
    .await?;

    let lists = lists
        .into_iter()
        .map(|row| WorkListItem {
            id: row.id,
            title: row.title,
            username: row.username,
            display_name: row.display_name,
        })
        .collect();

    Ok(Json(WorkListsResponse { lists }))
}

async fn get_popular_works(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<PopularWorkItem>>, AppError> {
    let works = sqlx::query_as::<_, PopularWorkRow>(
        r#"
        SELECT w.id, w.title,
               (SELECT ec2.cover_id FROM editions e2
                JOIN edition_covers ec2 ON e2.id = ec2.edition_id AND ec2.position = 0
                WHERE e2.work_id = w.id LIMIT 1) as cover_id,
               (wp.ratings_sum / NULLIF(wp.ratings_count, 0)) as rating_avg,
               wp.ratings_count
        FROM work_popularity wp
        JOIN works w ON wp.work_id = w.id
        WHERE wp.ratings_count > 0
        ORDER BY wp.popularity_score DESC
        LIMIT 20
        "#,
    )
    .fetch_all(&state.db)
    .await?;

    let works = works
        .into_iter()
        .map(|row| PopularWorkItem {
            slug: base36::encode(row.id as i64),
            title: row.title,
            cover_id: row.cover_id,
            rating_avg: row.rating_avg,
            ratings_count: row.ratings_count,
        })
        .collect();

    Ok(Json(works))
}

async fn get_recent_reviews(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<RecentReviewItem>>, AppError> {
    let reviews = sqlx::query_as::<_, RecentReviewRow>(
        r#"
        SELECT u.username, u.display_name, ur.rating, ur.review_text,
               e.title, e.id, e.work_id, ec.cover_id, ur.updated_at
        FROM user_reviews ur
        JOIN users u ON ur.user_id = u.id
        JOIN editions e ON ur.edition_id = e.id
        LEFT JOIN edition_covers ec ON e.id = ec.edition_id AND ec.position = 0
        WHERE ur.review_text IS NOT NULL
        ORDER BY ur.updated_at DESC
        LIMIT 10
        "#,
    )
    .fetch_all(&state.db)
    .await?;

    let reviews = reviews
        .into_iter()
        .map(|row| RecentReviewItem {
            username: row.username,
            display_name: row.display_name,
            rating: row.rating,
            review_text: row.review_text,
            edition_title: row.title,
            work_slug: base36::encode(row.work_id as i64),
            edition_slug: base36::encode(row.id as i64),
            cover_id: row.cover_id,
            updated_at: row.updated_at,
        })
        .collect();

    Ok(Json(reviews))
}

async fn get_recent_lists(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<RecentListItem>>, AppError> {
    let lists = sqlx::query_as::<_, RecentListRow>(
        r#"
        SELECT ul.id, ul.title, u.username, u.display_name,
               (SELECT COUNT(*) FROM user_list_works WHERE list_id = ul.id) as work_count
        FROM user_lists ul
        JOIN users u ON ul.user_id = u.id
        WHERE (SELECT COUNT(*) FROM user_list_works WHERE list_id = ul.id) > 0
        ORDER BY ul.updated_at DESC
        LIMIT 10
        "#,
    )
    .fetch_all(&state.db)
    .await?;

    let lists = lists
        .into_iter()
        .map(|row| RecentListItem {
            id: row.id,
            title: row.title,
            username: row.username,
            display_name: row.display_name,
            work_count: row.work_count,
        })
        .collect();

    Ok(Json(lists))
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
