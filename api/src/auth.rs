use std::sync::Arc;

use argon2::{
    password_hash::{rand_core::OsRng, PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};
use axum::{
    extract::{Path, Query, State},
    http::{header::SET_COOKIE, HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{delete, get, patch, post, put},
    Json, Router,
};
use chrono::{Duration, Utc};
use rand::Rng;
use serde::{Deserialize, Serialize};

use crate::{base36, AppState};

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/auth/register", post(register))
        .route("/auth/login", post(login))
        .route("/auth/logout", post(logout))
        .route("/auth/me", get(me))
        .route("/auth/verify-email", get(verify_email))
        .route("/auth/editions", get(list_user_editions))
        .route("/auth/editions/{slug}", put(set_edition_status))
        .route("/auth/editions/{slug}", delete(remove_edition))
        .route(
            "/auth/editions/{slug}/review",
            get(get_user_review).put(upsert_review).delete(delete_review),
        )
        .route(
            "/auth/editions/{slug}/progress",
            patch(update_progress),
        )
        .route("/auth/profile", patch(update_profile))
        .route("/auth/following", get(list_following))
        .route(
            "/auth/following/{username}",
            get(check_following).put(follow_user).delete(unfollow_user),
        )
        .route("/auth/feed", get(get_feed))
        .route("/auth/lists", get(list_my_lists).post(create_list))
        .route(
            "/auth/lists/{id}",
            patch(update_list).delete(delete_list),
        )
        .route(
            "/auth/lists/{id}/works/{slug}",
            put(add_work_to_list).delete(remove_work_from_list),
        )
}

fn generate_token() -> String {
    let bytes: [u8; 32] = rand::thread_rng().gen();
    hex::encode(bytes)
}

#[derive(Deserialize)]
pub struct RegisterRequest {
    email: String,
    password: String,
    username: String,
}

#[derive(Serialize)]
pub struct AuthResponse {
    success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    user: Option<UserInfo>,
}

#[derive(Serialize)]
pub struct UserInfo {
    id: i32,
    email: String,
    email_verified: bool,
    username: String,
    display_name: Option<String>,
}

#[derive(Serialize)]
pub struct SuccessResponse {
    success: bool,
}

#[derive(Serialize)]
pub struct StatusChangeResponse {
    success: bool,
    status: String,
}

#[derive(Serialize)]
pub struct RatingChangeResponse {
    success: bool,
    rating: i16,
}

#[derive(Serialize)]
pub struct UserEditionItem {
    slug: String,
    edition_id: i32,
    work_slug: String,
    title: String,
    status: String,
    cover_id: Option<i64>,
    started_at: Option<chrono::NaiveDate>,
    finished_at: Option<chrono::NaiveDate>,
    current_page: Option<i32>,
    number_of_pages: Option<i32>,
}

#[derive(Serialize)]
pub struct UserEditionsResponse {
    success: bool,
    editions: Vec<UserEditionItem>,
}

#[derive(Serialize)]
pub struct ReviewDetail {
    rating: i16,
    review_text: Option<String>,
    created_at: chrono::DateTime<Utc>,
    updated_at: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
pub struct UserReviewResponse {
    success: bool,
    review: Option<ReviewDetail>,
}

#[derive(Serialize)]
pub struct FollowStateResponse {
    following: bool,
}

#[derive(Serialize)]
pub struct FollowingUser {
    username: String,
    display_name: Option<String>,
}

#[derive(Serialize)]
pub struct FollowingListResponse {
    following: Vec<FollowingUser>,
}

#[derive(Serialize)]
pub struct FeedItem {
    username: String,
    display_name: Option<String>,
    rating: i16,
    review_text: Option<String>,
    edition_slug: String,
    edition_title: String,
    work_slug: String,
    cover_id: Option<i64>,
    updated_at: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
pub struct FeedResponse {
    feed: Vec<FeedItem>,
}

fn validate_username(username: &str) -> Result<(), AuthError> {
    if username.len() < 3 || username.len() > 30 {
        return Err(AuthError::Validation("Username must be 3-30 characters".into()));
    }
    if !username.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(AuthError::Validation("Username must be alphanumeric or underscores".into()));
    }
    Ok(())
}

async fn register(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RegisterRequest>,
) -> Result<impl IntoResponse, AuthError> {
    if !req.email.contains('@') || req.email.len() < 5 {
        return Err(AuthError::InvalidEmail);
    }

    if req.password.len() < 8 {
        return Err(AuthError::WeakPassword);
    }

    validate_username(&req.username)?;

    let existing = sqlx::query_scalar::<_, i32>("SELECT id FROM users WHERE email = $1")
        .bind(&req.email)
        .fetch_optional(&state.db)
        .await?;

    if existing.is_some() {
        return Err(AuthError::EmailTaken);
    }

    let username_taken = sqlx::query_scalar::<_, i32>(
        "SELECT id FROM users WHERE LOWER(username) = LOWER($1)",
    )
    .bind(&req.username)
    .fetch_optional(&state.db)
    .await?
    .is_some();

    if username_taken {
        return Err(AuthError::Validation("Username already taken".into()));
    }

    let salt = SaltString::generate(&mut OsRng);
    let argon2 = Argon2::default();
    let password_hash = argon2
        .hash_password(req.password.as_bytes(), &salt)
        .map_err(|_| AuthError::Internal)?
        .to_string();

    let user_id = sqlx::query_scalar::<_, i32>(
        "INSERT INTO users (email, password_hash, username) VALUES ($1, $2, $3) RETURNING id",
    )
    .bind(&req.email)
    .bind(&password_hash)
    .bind(&req.username)
    .fetch_one(&state.db)
    .await?;

    let token = generate_token();
    let expires_at = Utc::now() + Duration::hours(24);

    sqlx::query("INSERT INTO email_verifications (user_id, token, expires_at) VALUES ($1, $2, $3)")
        .bind(user_id)
        .bind(&token)
        .bind(expires_at)
        .execute(&state.db)
        .await?;

    if let Err(e) = send_verification_email(&state, &req.email, &token).await {
        tracing::error!("Failed to send verification email: {e}");
    }

    let session_token = generate_token();
    let session_expires = Utc::now() + Duration::days(30);

    sqlx::query("INSERT INTO sessions (user_id, token, expires_at) VALUES ($1, $2, $3)")
        .bind(user_id)
        .bind(&session_token)
        .bind(session_expires)
        .execute(&state.db)
        .await?;

    let mut headers = HeaderMap::new();
    headers.insert(
        SET_COOKIE,
        format!(
            "session={}; Path=/; HttpOnly; SameSite=Lax; Max-Age={}",
            session_token,
            30 * 24 * 60 * 60
        )
        .parse()
        .unwrap(),
    );

    Ok((
        headers,
        Json(AuthResponse {
            success: true,
            message: Some(
                "Registration successful. Please check your email to verify your account.".into(),
            ),
            user: Some(UserInfo {
                id: user_id,
                email: req.email,
                email_verified: false,
                username: req.username,
                display_name: None,
            }),
        }),
    ))
}

#[derive(Deserialize)]
pub struct LoginRequest {
    email: String,
    password: String,
}

async fn login(
    State(state): State<Arc<AppState>>,
    Json(req): Json<LoginRequest>,
) -> Result<impl IntoResponse, AuthError> {
    let user = sqlx::query_as::<_, (i32, String, bool, String, Option<String>)>(
        "SELECT id, password_hash, email_verified, username, display_name FROM users WHERE email = $1",
    )
    .bind(&req.email)
    .fetch_optional(&state.db)
    .await?
    .ok_or(AuthError::InvalidCredentials)?;

    let (user_id, password_hash, email_verified, username, display_name) = user;

    let parsed_hash = PasswordHash::new(&password_hash).map_err(|_| AuthError::Internal)?;
    Argon2::default()
        .verify_password(req.password.as_bytes(), &parsed_hash)
        .map_err(|_| AuthError::InvalidCredentials)?;

    let session_token = generate_token();
    let session_expires = Utc::now() + Duration::days(30);

    sqlx::query("INSERT INTO sessions (user_id, token, expires_at) VALUES ($1, $2, $3)")
        .bind(user_id)
        .bind(&session_token)
        .bind(session_expires)
        .execute(&state.db)
        .await?;

    let mut headers = HeaderMap::new();
    headers.insert(
        SET_COOKIE,
        format!(
            "session={}; Path=/; HttpOnly; SameSite=Lax; Max-Age={}",
            session_token,
            30 * 24 * 60 * 60
        )
        .parse()
        .unwrap(),
    );

    Ok((
        headers,
        Json(AuthResponse {
            success: true,
            message: None,
            user: Some(UserInfo {
                id: user_id,
                email: req.email,
                email_verified,
                username,
                display_name,
            }),
        }),
    ))
}

async fn logout(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, AuthError> {
    if let Some(session_token) = extract_session_token(&headers) {
        sqlx::query("DELETE FROM sessions WHERE token = $1")
            .bind(&session_token)
            .execute(&state.db)
            .await?;
    }

    let mut response_headers = HeaderMap::new();
    response_headers.insert(
        SET_COOKIE,
        "session=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0"
            .parse()
            .unwrap(),
    );

    Ok((
        response_headers,
        Json(AuthResponse {
            success: true,
            message: Some("Logged out".into()),
            user: None,
        }),
    ))
}

async fn me(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Json<AuthResponse> {
    let user = match extract_session_token(&headers) {
        Some(token) => sqlx::query_as::<_, (i32, String, bool, String, Option<String>)>(
            r#"
            SELECT u.id, u.email, u.email_verified, u.username, u.display_name
            FROM users u
            JOIN sessions s ON u.id = s.user_id
            WHERE s.token = $1 AND s.expires_at > NOW()
            "#,
        )
        .bind(&token)
        .fetch_optional(&state.db)
        .await
        .ok()
        .flatten()
        .map(|(id, email, email_verified, username, display_name)| UserInfo {
            id,
            email,
            email_verified,
            username,
            display_name,
        }),
        None => None,
    };

    Json(AuthResponse {
        success: true,
        message: None,
        user,
    })
}

#[derive(Deserialize)]
pub struct VerifyEmailQuery {
    token: String,
}

async fn verify_email(
    State(state): State<Arc<AppState>>,
    Query(query): Query<VerifyEmailQuery>,
) -> Result<impl IntoResponse, AuthError> {
    let verification = sqlx::query_as::<_, (i32, i32)>(
        "SELECT id, user_id FROM email_verifications WHERE token = $1 AND expires_at > NOW()",
    )
    .bind(&query.token)
    .fetch_optional(&state.db)
    .await?
    .ok_or(AuthError::InvalidToken)?;

    let (verification_id, user_id) = verification;

    // Mark email as verified
    sqlx::query("UPDATE users SET email_verified = TRUE WHERE id = $1")
        .bind(user_id)
        .execute(&state.db)
        .await?;

    // Delete the verification token
    sqlx::query("DELETE FROM email_verifications WHERE id = $1")
        .bind(verification_id)
        .execute(&state.db)
        .await?;

    // Redirect to home with success message
    let base_url = std::env::var("BASE_URL").unwrap_or_else(|_| "http://localhost:4321".into());
    let redirect_url = format!("{}/?verified=1", base_url);
    Ok((
        StatusCode::FOUND,
        [(axum::http::header::LOCATION, redirect_url)],
    ))
}

// Helper to get user_id from session
async fn get_user_id(state: &AppState, headers: &HeaderMap) -> Result<i32, AuthError> {
    let session_token = extract_session_token(headers).ok_or(AuthError::Unauthorized)?;

    let user_id = sqlx::query_scalar::<_, i32>(
        "SELECT user_id FROM sessions WHERE token = $1 AND expires_at > NOW()",
    )
    .bind(&session_token)
    .fetch_optional(&state.db)
    .await?
    .ok_or(AuthError::Unauthorized)?;

    Ok(user_id)
}

#[derive(Deserialize)]
pub struct SetEditionStatusRequest {
    status: String,
    started_at: Option<chrono::NaiveDate>,
    finished_at: Option<chrono::NaiveDate>,
    current_page: Option<i32>,
}

#[derive(Serialize)]
pub struct EditionStatusResponse {
    slug: String,
    edition_id: i32,
    title: String,
    status: String,
}

async fn set_edition_status(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(slug): Path<String>,
    Json(req): Json<SetEditionStatusRequest>,
) -> Result<Json<StatusChangeResponse>, AuthError> {
    let user_id = get_user_id(&state, &headers).await?;
    let edition_id = base36::decode(&slug).ok_or(AuthError::InvalidToken)? as i32;

    if !["reading", "want_to_read", "finished", "did_not_finish"].contains(&req.status.as_str()) {
        return Err(AuthError::InvalidToken);
    }

    let edition = sqlx::query_as::<_, (i32, Option<i32>)>(
        "SELECT id, number_of_pages FROM editions WHERE id = $1",
    )
    .bind(edition_id)
    .fetch_optional(&state.db)
    .await?
    .ok_or(AuthError::InvalidToken)?;

    let today = Utc::now().date_naive();
    let number_of_pages = edition.1;

    let started_at = match req.status.as_str() {
        "reading" | "finished" => req.started_at.or(Some(today)),
        _ => req.started_at,
    };

    let finished_at = match req.status.as_str() {
        "finished" => req.finished_at.or(Some(today)),
        _ => req.finished_at,
    };

    let current_page = match req.status.as_str() {
        "finished" => req.current_page.or(number_of_pages),
        _ => req.current_page,
    };

    sqlx::query(
        r#"
        INSERT INTO user_editions (user_id, edition_id, status, started_at, finished_at, current_page)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (user_id, edition_id) DO UPDATE SET
            status = $3,
            started_at = COALESCE($4, user_editions.started_at),
            finished_at = $5,
            current_page = COALESCE($6, user_editions.current_page),
            created_at = NOW()
        "#,
    )
    .bind(user_id)
    .bind(edition_id)
    .bind(&req.status)
    .bind(started_at)
    .bind(finished_at)
    .bind(current_page)
    .execute(&state.db)
    .await?;

    Ok(Json(StatusChangeResponse {
        success: true,
        status: req.status,
    }))
}

async fn remove_edition(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(slug): Path<String>,
) -> Result<Json<SuccessResponse>, AuthError> {
    let user_id = get_user_id(&state, &headers).await?;
    let edition_id = base36::decode(&slug).ok_or(AuthError::InvalidToken)? as i32;

    sqlx::query("DELETE FROM user_editions WHERE user_id = $1 AND edition_id = $2")
        .bind(user_id)
        .bind(edition_id)
        .execute(&state.db)
        .await?;

    Ok(Json(SuccessResponse { success: true }))
}

async fn list_user_editions(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<UserEditionsResponse>, AuthError> {
    let user_id = get_user_id(&state, &headers).await?;

    let rows = sqlx::query_as::<_, (i32, String, String, i32, Option<i64>, Option<chrono::NaiveDate>, Option<chrono::NaiveDate>, Option<i32>, Option<i32>)>(
        r#"
        SELECT e.id, e.title, ue.status, e.work_id, ec.cover_id,
               ue.started_at, ue.finished_at, ue.current_page, e.number_of_pages
        FROM user_editions ue
        JOIN editions e ON ue.edition_id = e.id
        LEFT JOIN edition_covers ec ON e.id = ec.edition_id AND ec.position = 0
        WHERE ue.user_id = $1
        ORDER BY ue.created_at DESC
        "#,
    )
    .bind(user_id)
    .fetch_all(&state.db)
    .await?;

    let editions = rows
        .into_iter()
        .map(|(id, title, status, work_id, cover_id, started_at, finished_at, current_page, number_of_pages)| {
            UserEditionItem {
                slug: base36::encode(id as i64),
                edition_id: id,
                work_slug: base36::encode(work_id as i64),
                title,
                status,
                cover_id,
                started_at,
                finished_at,
                current_page,
                number_of_pages,
            }
        })
        .collect();

    Ok(Json(UserEditionsResponse { success: true, editions }))
}

#[derive(Deserialize)]
pub struct UpdateProgressRequest {
    started_at: Option<chrono::NaiveDate>,
    finished_at: Option<chrono::NaiveDate>,
    current_page: Option<i32>,
}

async fn update_progress(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(slug): Path<String>,
    Json(req): Json<UpdateProgressRequest>,
) -> Result<Json<SuccessResponse>, AuthError> {
    let user_id = get_user_id(&state, &headers).await?;
    let edition_id = base36::decode(&slug).ok_or(AuthError::InvalidToken)? as i32;

    if let Some(page) = req.current_page {
        if page < 0 {
            return Err(AuthError::InvalidToken);
        }
    }

    let rows = sqlx::query(
        r#"
        UPDATE user_editions SET
            started_at = COALESCE($3, started_at),
            finished_at = COALESCE($4, finished_at),
            current_page = COALESCE($5, current_page)
        WHERE user_id = $1 AND edition_id = $2
        "#,
    )
    .bind(user_id)
    .bind(edition_id)
    .bind(req.started_at)
    .bind(req.finished_at)
    .bind(req.current_page)
    .execute(&state.db)
    .await?;

    if rows.rows_affected() == 0 {
        return Err(AuthError::InvalidToken);
    }

    Ok(Json(SuccessResponse { success: true }))
}

const MAX_REVIEW_TEXT: usize = 10000;

#[derive(Deserialize)]
pub struct UpsertReviewRequest {
    rating: i16,
    review_text: Option<String>,
}

async fn get_user_review(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(slug): Path<String>,
) -> Result<Json<UserReviewResponse>, AuthError> {
    let user_id = get_user_id(&state, &headers).await?;
    let edition_id = base36::decode(&slug).ok_or(AuthError::InvalidToken)? as i32;

    let review = sqlx::query_as::<_, (i16, Option<String>, chrono::DateTime<Utc>, chrono::DateTime<Utc>)>(
        "SELECT rating, review_text, created_at, updated_at FROM user_reviews WHERE user_id = $1 AND edition_id = $2",
    )
    .bind(user_id)
    .bind(edition_id)
    .fetch_optional(&state.db)
    .await?;

    let review = review.map(|(rating, review_text, created_at, updated_at)| ReviewDetail {
        rating,
        review_text,
        created_at,
        updated_at,
    });

    Ok(Json(UserReviewResponse { success: true, review }))
}

async fn upsert_review(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(slug): Path<String>,
    Json(req): Json<UpsertReviewRequest>,
) -> Result<Json<RatingChangeResponse>, AuthError> {
    let user_id = get_user_id(&state, &headers).await?;
    let edition_id = base36::decode(&slug).ok_or(AuthError::InvalidToken)? as i32;

    if req.rating < 1 || req.rating > 5 {
        return Err(AuthError::InvalidToken);
    }

    if let Some(ref text) = req.review_text {
        if text.len() > MAX_REVIEW_TEXT {
            return Err(AuthError::InvalidToken);
        }
    }

    let exists = sqlx::query_scalar::<_, i32>("SELECT id FROM editions WHERE id = $1")
        .bind(edition_id)
        .fetch_optional(&state.db)
        .await?
        .is_some();

    if !exists {
        return Err(AuthError::InvalidToken);
    }

    sqlx::query(
        r#"
        INSERT INTO user_reviews (user_id, edition_id, rating, review_text)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (user_id, edition_id) DO UPDATE SET
            rating = $3, review_text = $4, updated_at = NOW()
        "#,
    )
    .bind(user_id)
    .bind(edition_id)
    .bind(req.rating)
    .bind(&req.review_text)
    .execute(&state.db)
    .await?;

    Ok(Json(RatingChangeResponse {
        success: true,
        rating: req.rating,
    }))
}

async fn delete_review(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(slug): Path<String>,
) -> Result<Json<SuccessResponse>, AuthError> {
    let user_id = get_user_id(&state, &headers).await?;
    let edition_id = base36::decode(&slug).ok_or(AuthError::InvalidToken)? as i32;

    sqlx::query("DELETE FROM user_reviews WHERE user_id = $1 AND edition_id = $2")
        .bind(user_id)
        .bind(edition_id)
        .execute(&state.db)
        .await?;

    Ok(Json(SuccessResponse { success: true }))
}

#[derive(Deserialize)]
pub struct UpdateProfileRequest {
    username: Option<String>,
    display_name: Option<String>,
    bio: Option<String>,
}

async fn update_profile(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<UpdateProfileRequest>,
) -> Result<Json<SuccessResponse>, AuthError> {
    let user_id = get_user_id(&state, &headers).await?;

    if let Some(ref username) = req.username {
        validate_username(username)?;

        let taken = sqlx::query_scalar::<_, i32>(
            "SELECT id FROM users WHERE LOWER(username) = LOWER($1) AND id != $2",
        )
        .bind(username)
        .bind(user_id)
        .fetch_optional(&state.db)
        .await?
        .is_some();

        if taken {
            return Err(AuthError::Validation("Username already taken".into()));
        }
    }

    if let Some(ref display_name) = req.display_name {
        if display_name.len() > 100 {
            return Err(AuthError::Validation("Display name too long".into()));
        }
    }

    if let Some(ref bio) = req.bio {
        if bio.len() > 5000 {
            return Err(AuthError::Validation("Bio too long".into()));
        }
    }

    sqlx::query(
        r#"
        UPDATE users SET
            username = COALESCE($2, username),
            display_name = COALESCE($3, display_name),
            bio = COALESCE($4, bio)
        WHERE id = $1
        "#,
    )
    .bind(user_id)
    .bind(&req.username)
    .bind(&req.display_name)
    .bind(&req.bio)
    .execute(&state.db)
    .await?;

    Ok(Json(SuccessResponse { success: true }))
}

#[derive(Deserialize)]
pub struct CreateListRequest {
    title: String,
    description: Option<String>,
}

async fn create_list(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<CreateListRequest>,
) -> Result<Json<serde_json::Value>, AuthError> {
    let user_id = get_user_id(&state, &headers).await?;

    if req.title.is_empty() || req.title.len() > 200 {
        return Err(AuthError::Validation("Title must be 1-200 characters".into()));
    }

    let list_id = sqlx::query_scalar::<_, i32>(
        "INSERT INTO user_lists (user_id, title, description) VALUES ($1, $2, $3) RETURNING id",
    )
    .bind(user_id)
    .bind(&req.title)
    .bind(&req.description)
    .fetch_one(&state.db)
    .await?;

    Ok(Json(serde_json::json!({ "success": true, "id": list_id })))
}

async fn update_list(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(list_id): Path<i32>,
    Json(req): Json<CreateListRequest>,
) -> Result<Json<serde_json::Value>, AuthError> {
    let user_id = get_user_id(&state, &headers).await?;

    if req.title.is_empty() || req.title.len() > 200 {
        return Err(AuthError::Validation("Title must be 1-200 characters".into()));
    }

    let rows = sqlx::query(
        "UPDATE user_lists SET title = $3, description = $4, updated_at = NOW() WHERE id = $1 AND user_id = $2",
    )
    .bind(list_id)
    .bind(user_id)
    .bind(&req.title)
    .bind(&req.description)
    .execute(&state.db)
    .await?;

    if rows.rows_affected() == 0 {
        return Err(AuthError::InvalidToken);
    }

    Ok(Json(serde_json::json!({ "success": true })))
}

async fn delete_list(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(list_id): Path<i32>,
) -> Result<Json<serde_json::Value>, AuthError> {
    let user_id = get_user_id(&state, &headers).await?;

    sqlx::query("DELETE FROM user_lists WHERE id = $1 AND user_id = $2")
        .bind(list_id)
        .bind(user_id)
        .execute(&state.db)
        .await?;

    Ok(Json(serde_json::json!({ "success": true })))
}

async fn add_work_to_list(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((list_id, slug)): Path<(i32, String)>,
) -> Result<Json<serde_json::Value>, AuthError> {
    let user_id = get_user_id(&state, &headers).await?;
    let work_id = base36::decode(&slug).ok_or(AuthError::InvalidToken)? as i32;

    let owns = sqlx::query_scalar::<_, i32>(
        "SELECT id FROM user_lists WHERE id = $1 AND user_id = $2",
    )
    .bind(list_id)
    .bind(user_id)
    .fetch_optional(&state.db)
    .await?
    .is_some();

    if !owns {
        return Err(AuthError::Unauthorized);
    }

    let max_pos = sqlx::query_scalar::<_, Option<i32>>(
        "SELECT MAX(position) FROM user_list_works WHERE list_id = $1",
    )
    .bind(list_id)
    .fetch_one(&state.db)
    .await?
    .unwrap_or(-1);

    sqlx::query(
        r#"
        INSERT INTO user_list_works (list_id, work_id, position)
        VALUES ($1, $2, $3)
        ON CONFLICT (list_id, work_id) DO NOTHING
        "#,
    )
    .bind(list_id)
    .bind(work_id)
    .bind(max_pos + 1)
    .execute(&state.db)
    .await?;

    Ok(Json(serde_json::json!({ "success": true })))
}

async fn remove_work_from_list(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((list_id, slug)): Path<(i32, String)>,
) -> Result<Json<serde_json::Value>, AuthError> {
    let user_id = get_user_id(&state, &headers).await?;
    let work_id = base36::decode(&slug).ok_or(AuthError::InvalidToken)? as i32;

    let owns = sqlx::query_scalar::<_, i32>(
        "SELECT id FROM user_lists WHERE id = $1 AND user_id = $2",
    )
    .bind(list_id)
    .bind(user_id)
    .fetch_optional(&state.db)
    .await?
    .is_some();

    if !owns {
        return Err(AuthError::Unauthorized);
    }

    sqlx::query("DELETE FROM user_list_works WHERE list_id = $1 AND work_id = $2")
        .bind(list_id)
        .bind(work_id)
        .execute(&state.db)
        .await?;

    Ok(Json(serde_json::json!({ "success": true })))
}

async fn list_my_lists(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, AuthError> {
    let user_id = get_user_id(&state, &headers).await?;

    let lists = sqlx::query_as::<_, (i32, String, Option<String>, i64)>(
        r#"
        SELECT ul.id, ul.title, ul.description,
               (SELECT COUNT(*) FROM user_list_works WHERE list_id = ul.id) as work_count
        FROM user_lists ul
        WHERE ul.user_id = $1
        ORDER BY ul.updated_at DESC
        "#,
    )
    .bind(user_id)
    .fetch_all(&state.db)
    .await?;

    // Also get all work_ids per list for the AddToList component
    let work_ids = sqlx::query_as::<_, (i32, i32)>(
        r#"
        SELECT ulw.list_id, ulw.work_id
        FROM user_list_works ulw
        JOIN user_lists ul ON ulw.list_id = ul.id
        WHERE ul.user_id = $1
        "#,
    )
    .bind(user_id)
    .fetch_all(&state.db)
    .await?;

    let lists: Vec<_> = lists
        .into_iter()
        .map(|(id, title, description, work_count)| {
            let works: Vec<String> = work_ids
                .iter()
                .filter(|(lid, _)| *lid == id)
                .map(|(_, wid)| base36::encode(*wid as i64))
                .collect();
            serde_json::json!({
                "id": id,
                "title": title,
                "description": description,
                "work_count": work_count,
                "work_slugs": works,
            })
        })
        .collect();

    Ok(Json(serde_json::json!({ "lists": lists })))
}

async fn resolve_username_to_id(state: &AppState, username: &str) -> Result<i32, AuthError> {
    sqlx::query_scalar::<_, i32>("SELECT id FROM users WHERE LOWER(username) = LOWER($1)")
        .bind(username)
        .fetch_optional(&state.db)
        .await?
        .ok_or(AuthError::InvalidToken)
}

async fn follow_user(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(username): Path<String>,
) -> Result<Json<SuccessResponse>, AuthError> {
    let user_id = get_user_id(&state, &headers).await?;
    let target_id = resolve_username_to_id(&state, &username).await?;

    if user_id == target_id {
        return Err(AuthError::Validation("Cannot follow yourself".into()));
    }

    sqlx::query(
        r#"
        INSERT INTO user_follows (follower_id, following_id)
        VALUES ($1, $2)
        ON CONFLICT (follower_id, following_id) DO NOTHING
        "#,
    )
    .bind(user_id)
    .bind(target_id)
    .execute(&state.db)
    .await?;

    Ok(Json(SuccessResponse { success: true }))
}

async fn unfollow_user(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(username): Path<String>,
) -> Result<Json<SuccessResponse>, AuthError> {
    let user_id = get_user_id(&state, &headers).await?;
    let target_id = resolve_username_to_id(&state, &username).await?;

    sqlx::query("DELETE FROM user_follows WHERE follower_id = $1 AND following_id = $2")
        .bind(user_id)
        .bind(target_id)
        .execute(&state.db)
        .await?;

    Ok(Json(SuccessResponse { success: true }))
}

async fn check_following(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(username): Path<String>,
) -> Result<Json<FollowStateResponse>, AuthError> {
    let user_id = get_user_id(&state, &headers).await?;
    let target_id = resolve_username_to_id(&state, &username).await?;

    let following = sqlx::query_scalar::<_, i32>(
        "SELECT follower_id FROM user_follows WHERE follower_id = $1 AND following_id = $2",
    )
    .bind(user_id)
    .bind(target_id)
    .fetch_optional(&state.db)
    .await?
    .is_some();

    Ok(Json(FollowStateResponse { following }))
}

async fn list_following(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<FollowingListResponse>, AuthError> {
    let user_id = get_user_id(&state, &headers).await?;

    let rows = sqlx::query_as::<_, (String, Option<String>)>(
        r#"
        SELECT u.username, u.display_name
        FROM user_follows uf
        JOIN users u ON uf.following_id = u.id
        WHERE uf.follower_id = $1
        ORDER BY uf.created_at DESC
        "#,
    )
    .bind(user_id)
    .fetch_all(&state.db)
    .await?;

    let following = rows
        .into_iter()
        .map(|(username, display_name)| FollowingUser { username, display_name })
        .collect();

    Ok(Json(FollowingListResponse { following }))
}

async fn get_feed(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<FeedResponse>, AuthError> {
    let user_id = get_user_id(&state, &headers).await?;

    let rows = sqlx::query_as::<_, (String, Option<String>, i16, Option<String>, i32, String, i32, Option<i64>, chrono::DateTime<Utc>)>(
        r#"
        SELECT u.username, u.display_name, ur.rating, ur.review_text,
               ur.edition_id, e.title, e.work_id, ec.cover_id, ur.updated_at
        FROM user_follows uf
        JOIN user_reviews ur ON uf.following_id = ur.user_id
        JOIN users u ON ur.user_id = u.id
        JOIN editions e ON ur.edition_id = e.id
        LEFT JOIN edition_covers ec ON e.id = ec.edition_id AND ec.position = 0
        WHERE uf.follower_id = $1
        ORDER BY ur.updated_at DESC
        LIMIT 20
        "#,
    )
    .bind(user_id)
    .fetch_all(&state.db)
    .await?;

    let feed = rows
        .into_iter()
        .map(|(username, display_name, rating, review_text, edition_id, edition_title, work_id, cover_id, updated_at)| {
            FeedItem {
                username,
                display_name,
                rating,
                review_text,
                edition_slug: base36::encode(edition_id as i64),
                edition_title,
                work_slug: base36::encode(work_id as i64),
                cover_id,
                updated_at,
            }
        })
        .collect();

    Ok(Json(FeedResponse { feed }))
}

pub fn extract_session_token(headers: &HeaderMap) -> Option<String> {
    let cookie_header = headers.get("cookie")?.to_str().ok()?;
    for part in cookie_header.split(';') {
        let part = part.trim();
        if let Some(value) = part.strip_prefix("session=") {
            if !value.is_empty() {
                return Some(value.to_string());
            }
        }
    }
    None
}

async fn send_verification_email(
    _state: &AppState,
    email: &str,
    token: &str,
) -> Result<(), anyhow::Error> {
    let base_url = std::env::var("BASE_URL").unwrap_or_else(|_| "http://localhost:4321".into());
    let verify_url = format!("{}/api/auth/verify-email?token={}", base_url, token);

    let api_key = std::env::var("RESEND_API_KEY")?;
    let from_email =
        std::env::var("RESEND_FROM_EMAIL").unwrap_or_else(|_| "noreply@example.com".into());

    let client = reqwest::Client::new();

    client
        .post("https://api.resend.com/emails")
        .header("Authorization", format!("Bearer {}", api_key))
        .json(&serde_json::json!({
            "from": from_email,
            "to": [email],
            "subject": "Verify your mylib account",
            "html": format!(
                r#"<p>Welcome to mylib!</p>
                <p>Please click the link below to verify your email address:</p>
                <p><a href="{}">Verify Email</a></p>
                <p>This link will expire in 24 hours.</p>"#,
                verify_url
            ),
            "text": format!(
                "Welcome to mylib!\n\nPlease verify your email by visiting: {}\n\nThis link will expire in 24 hours.",
                verify_url
            )
        }))
        .send()
        .await?
        .error_for_status()?;

    Ok(())
}

pub enum AuthError {
    InvalidEmail,
    WeakPassword,
    EmailTaken,
    InvalidCredentials,
    InvalidToken,
    Unauthorized,
    Validation(String),
    Internal,
    Database(sqlx::Error),
}

impl From<sqlx::Error> for AuthError {
    fn from(e: sqlx::Error) -> Self {
        AuthError::Database(e)
    }
}

impl IntoResponse for AuthError {
    fn into_response(self) -> axum::response::Response {
        let (status, message) = match self {
            AuthError::InvalidEmail => (StatusCode::BAD_REQUEST, "Invalid email address"),
            AuthError::WeakPassword => (
                StatusCode::BAD_REQUEST,
                "Password must be at least 8 characters",
            ),
            AuthError::EmailTaken => (StatusCode::CONFLICT, "Email already registered"),
            AuthError::InvalidCredentials => {
                (StatusCode::UNAUTHORIZED, "Invalid email or password")
            }
            AuthError::InvalidToken => (StatusCode::BAD_REQUEST, "Invalid or expired token"),
            AuthError::Validation(ref msg) => (StatusCode::BAD_REQUEST, msg.as_str()),
            AuthError::Unauthorized => (StatusCode::UNAUTHORIZED, "Not authenticated"),
            AuthError::Internal => (StatusCode::INTERNAL_SERVER_ERROR, "Internal error"),
            AuthError::Database(e) => {
                tracing::error!("Database error: {e:?}");
                (StatusCode::INTERNAL_SERVER_ERROR, "Internal error")
            }
        };

        (
            status,
            Json(AuthResponse {
                success: false,
                message: Some(message.into()),
                user: None,
            }),
        )
            .into_response()
    }
}
