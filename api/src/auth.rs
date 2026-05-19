use std::sync::Arc;

use argon2::{
    password_hash::{rand_core::OsRng, PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};
use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{delete, get, patch, post, put},
    Json, Router,
};
use chrono::{Duration, Utc};
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use rand::Rng;
use serde::{Deserialize, Serialize};

use crate::{base36, AppState};

#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    sub: i32,
    email: String,
    username: String,
    display_name: Option<String>,
    email_verified: bool,
    exp: i64,
}

fn jwt_secret() -> Vec<u8> {
    std::env::var("JWT_SECRET")
        .unwrap_or_else(|_| "dev-secret-change-in-production".into())
        .into_bytes()
}

fn create_jwt(
    user_id: i32,
    email: &str,
    username: &str,
    display_name: Option<&str>,
    email_verified: bool,
) -> Result<String, AuthError> {
    let exp = (Utc::now() + Duration::days(30)).timestamp();
    let claims = Claims {
        sub: user_id,
        email: email.to_string(),
        username: username.to_string(),
        display_name: display_name.map(|s| s.to_string()),
        email_verified,
        exp,
    };
    encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(&jwt_secret()),
    )
    .map_err(|_| AuthError::Internal)
}

fn decode_jwt(token: &str) -> Result<Claims, AuthError> {
    decode::<Claims>(
        token,
        &DecodingKey::from_secret(&jwt_secret()),
        &Validation::default(),
    )
    .map(|data| data.claims)
    .map_err(|_| AuthError::Unauthorized)
}

#[derive(sqlx::FromRow)]
struct UserRow {
    id: i32,
    password_hash: String,
    email_verified: bool,
    username: String,
    display_name: Option<String>,
}

#[derive(sqlx::FromRow)]
struct VerificationRow {
    id: i32,
    user_id: i32,
}

#[derive(sqlx::FromRow)]
struct EditionLookupRow {
    #[allow(unused)]
    id: i32,
    number_of_pages: Option<i32>,
}

#[derive(sqlx::FromRow)]
struct UserEditionRow {
    id: i32,
    title: String,
    status: String,
    work_id: i32,
    cover_id: Option<i64>,
    started_at: Option<chrono::NaiveDate>,
    finished_at: Option<chrono::NaiveDate>,
    current_page: Option<i32>,
    number_of_pages: Option<i32>,
}

#[derive(sqlx::FromRow)]
struct ReviewRow {
    rating: f32,
    review_text: Option<String>,
    created_at: chrono::DateTime<Utc>,
    updated_at: chrono::DateTime<Utc>,
}

#[derive(sqlx::FromRow)]
struct ListRow {
    id: i32,
    title: String,
    description: Option<String>,
    work_count: i64,
}

#[derive(sqlx::FromRow)]
struct ListWorkRow {
    list_id: i32,
    work_id: i32,
}

#[derive(sqlx::FromRow)]
struct FollowingRow {
    username: String,
    display_name: Option<String>,
}

#[derive(sqlx::FromRow)]
struct FeedRow {
    username: String,
    display_name: Option<String>,
    rating: f32,
    review_text: Option<String>,
    edition_id: i32,
    title: String,
    work_id: i32,
    cover_id: Option<i64>,
    updated_at: chrono::DateTime<Utc>,
}

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/auth/register", post(register))
        .route("/auth/login", post(login))
        .route("/auth/logout", post(logout))
        .route("/auth/verify-email", get(verify_email))
        .route("/auth/editions", get(list_user_editions))
        .route("/auth/editions/{slug}", put(set_edition_status))
        .route("/auth/editions/{slug}", delete(remove_edition))
        .route(
            "/auth/editions/{slug}/review",
            get(get_user_review)
                .put(upsert_review)
                .delete(delete_review),
        )
        .route("/auth/editions/{slug}/progress", patch(update_progress))
        .route("/auth/profile", patch(update_profile))
        .route("/auth/following", get(list_following))
        .route(
            "/auth/following/{username}",
            get(check_following).put(follow_user).delete(unfollow_user),
        )
        .route("/auth/feed", get(get_feed))
        .route("/auth/lists", get(list_my_lists).post(create_list))
        .route("/auth/lists/{id}", patch(update_list).delete(delete_list))
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
    token: Option<String>,
}

#[derive(Serialize)]
pub struct SuccessResponse {
    success: bool,
}

#[derive(Serialize)]
pub struct TokenResponse {
    success: bool,
    token: String,
}

#[derive(Serialize)]
pub struct CreateListResponse {
    success: bool,
    id: i32,
}

#[derive(Serialize)]
pub struct ListSummaryItem {
    id: i32,
    title: String,
    description: Option<String>,
    work_count: i64,
    work_slugs: Vec<String>,
}

#[derive(Serialize)]
pub struct MyListsResponse {
    lists: Vec<ListSummaryItem>,
}

#[derive(Serialize)]
struct ResendEmail {
    from: String,
    to: Vec<String>,
    subject: String,
    html: String,
    text: String,
}

#[derive(Serialize)]
pub struct StatusChangeResponse {
    success: bool,
    status: String,
}

#[derive(Serialize)]
pub struct RatingChangeResponse {
    success: bool,
    rating: f32,
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
    rating: f32,
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
    rating: f32,
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
        return Err(AuthError::Validation(
            "Username must be 3-30 characters".into(),
        ));
    }
    if !username
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Err(AuthError::Validation(
            "Username must be alphanumeric or underscores".into(),
        ));
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

    let existing = sqlx::query_scalar::<_, i32>("SELECT id FROM users WHERE email = ?")
        .bind(&req.email)
        .fetch_optional(&state.db)
        .await?;

    if existing.is_some() {
        return Err(AuthError::EmailTaken);
    }

    let username_taken =
        sqlx::query_scalar::<_, i32>("SELECT id FROM users WHERE LOWER(username) = LOWER(?)")
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

    let result = sqlx::query(
        "INSERT INTO users (email, password_hash, username) VALUES (?, ?, ?)",
    )
    .bind(&req.email)
    .bind(&password_hash)
    .bind(&req.username)
    .execute(&state.db)
    .await?;
    let user_id = result.last_insert_id() as i32;

    let token = generate_token();
    let expires_at = Utc::now() + Duration::hours(24);

    sqlx::query("INSERT INTO email_verifications (user_id, token, expires_at) VALUES (?, ?, ?)")
        .bind(user_id)
        .bind(&token)
        .bind(expires_at)
        .execute(&state.db)
        .await?;

    if let Err(e) = send_verification_email(&state, &req.email, &token).await {
        tracing::error!("Failed to send verification email: {e}");
    }

    let jwt = create_jwt(user_id, &req.email, &req.username, None, false)?;

    Ok(Json(AuthResponse {
        success: true,
        message: Some(
            "Registration successful. Please check your email to verify your account.".into(),
        ),
        token: Some(jwt),
    }))
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
    let user = sqlx::query_as::<_, UserRow>(
        "SELECT id, password_hash, email_verified, username, display_name FROM users WHERE email = ?",
    )
    .bind(&req.email)
    .fetch_optional(&state.db)
    .await?
    .ok_or(AuthError::InvalidCredentials)?;

    let user_id = user.id;
    let password_hash = user.password_hash;
    let email_verified = user.email_verified;
    let username = user.username;
    let display_name = user.display_name;

    let parsed_hash = PasswordHash::new(&password_hash).map_err(|_| AuthError::Internal)?;
    Argon2::default()
        .verify_password(req.password.as_bytes(), &parsed_hash)
        .map_err(|_| AuthError::InvalidCredentials)?;

    let jwt = create_jwt(
        user_id,
        &req.email,
        &username,
        display_name.as_deref(),
        email_verified,
    )?;

    Ok(Json(AuthResponse {
        success: true,
        message: None,
        token: Some(jwt),
    }))
}

async fn logout() -> Json<AuthResponse> {
    Json(AuthResponse {
        success: true,
        message: Some("Logged out".into()),
        token: None,
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
    let verification = sqlx::query_as::<_, VerificationRow>(
        "SELECT id, user_id FROM email_verifications WHERE token = ? AND expires_at > NOW()",
    )
    .bind(&query.token)
    .fetch_optional(&state.db)
    .await?
    .ok_or(AuthError::InvalidToken)?;

    let verification_id = verification.id;
    let user_id = verification.user_id;

    // Mark email as verified
    sqlx::query("UPDATE users SET email_verified = TRUE WHERE id = ?")
        .bind(user_id)
        .execute(&state.db)
        .await?;

    // Delete the verification token
    sqlx::query("DELETE FROM email_verifications WHERE id = ?")
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

fn get_user_id(headers: &HeaderMap) -> Result<i32, AuthError> {
    let token = extract_bearer_token(headers).ok_or(AuthError::Unauthorized)?;
    let claims = decode_jwt(&token)?;
    Ok(claims.sub)
}

#[derive(Deserialize)]
pub struct SetEditionStatusRequest {
    status: String,
    started_at: Option<chrono::NaiveDate>,
    finished_at: Option<chrono::NaiveDate>,
    current_page: Option<i32>,
}

async fn set_edition_status(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(slug): Path<String>,
    Json(req): Json<SetEditionStatusRequest>,
) -> Result<Json<StatusChangeResponse>, AuthError> {
    let user_id = get_user_id(&headers)?;
    let edition_id = base36::decode(&slug).ok_or(AuthError::InvalidToken)? as i32;

    if !["reading", "want_to_read", "finished", "did_not_finish"].contains(&req.status.as_str()) {
        return Err(AuthError::InvalidToken);
    }

    let edition = sqlx::query_as::<_, EditionLookupRow>(
        "SELECT id, number_of_pages FROM editions WHERE id = ?",
    )
    .bind(edition_id)
    .fetch_optional(&state.db)
    .await?
    .ok_or(AuthError::InvalidToken)?;

    let today = Utc::now().date_naive();
    let number_of_pages = edition.number_of_pages;

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
        VALUES (?, ?, ?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE
            status = VALUES(status),
            started_at = COALESCE(VALUES(started_at), started_at),
            finished_at = VALUES(finished_at),
            current_page = COALESCE(VALUES(current_page), current_page),
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
    let user_id = get_user_id(&headers)?;
    let edition_id = base36::decode(&slug).ok_or(AuthError::InvalidToken)? as i32;

    sqlx::query("DELETE FROM user_editions WHERE user_id = ? AND edition_id = ?")
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
    let user_id = get_user_id(&headers)?;

    let rows = sqlx::query_as::<_, UserEditionRow>(
        r#"
        SELECT e.id, e.title, ue.status, e.work_id, ec.cover_id,
               ue.started_at, ue.finished_at, ue.current_page, e.number_of_pages
        FROM user_editions ue
        JOIN editions e ON ue.edition_id = e.id
        LEFT JOIN edition_covers ec ON e.id = ec.edition_id AND ec.position = 0
        WHERE ue.user_id = ?
        ORDER BY ue.created_at DESC
        "#,
    )
    .bind(user_id)
    .fetch_all(&state.db)
    .await?;

    let editions = rows
        .into_iter()
        .map(|row| UserEditionItem {
            slug: base36::encode(row.id as i64),
            edition_id: row.id,
            work_slug: base36::encode(row.work_id as i64),
            title: row.title,
            status: row.status,
            cover_id: row.cover_id,
            started_at: row.started_at,
            finished_at: row.finished_at,
            current_page: row.current_page,
            number_of_pages: row.number_of_pages,
        })
        .collect();

    Ok(Json(UserEditionsResponse {
        success: true,
        editions,
    }))
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
    let user_id = get_user_id(&headers)?;
    let edition_id = base36::decode(&slug).ok_or(AuthError::InvalidToken)? as i32;

    if let Some(page) = req.current_page {
        if page < 0 {
            return Err(AuthError::InvalidToken);
        }
    }

    let rows = sqlx::query(
        r#"
        UPDATE user_editions SET
            started_at = COALESCE(?, started_at),
            finished_at = COALESCE(?, finished_at),
            current_page = COALESCE(?, current_page)
        WHERE user_id = ? AND edition_id = ?
        "#,
    )
    .bind(req.started_at)
    .bind(req.finished_at)
    .bind(req.current_page)
    .bind(user_id)
    .bind(edition_id)
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
    rating: f32,
    review_text: Option<String>,
}

async fn get_user_review(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(slug): Path<String>,
) -> Result<Json<UserReviewResponse>, AuthError> {
    let user_id = get_user_id(&headers)?;
    let edition_id = base36::decode(&slug).ok_or(AuthError::InvalidToken)? as i32;

    let review = sqlx::query_as::<_, ReviewRow>(
        "SELECT rating, review_text, created_at, updated_at FROM user_reviews WHERE user_id = ? AND edition_id = ?",
    )
    .bind(user_id)
    .bind(edition_id)
    .fetch_optional(&state.db)
    .await?;

    let review = review.map(|row| ReviewDetail {
        rating: row.rating,
        review_text: row.review_text,
        created_at: row.created_at,
        updated_at: row.updated_at,
    });

    Ok(Json(UserReviewResponse {
        success: true,
        review,
    }))
}

async fn upsert_review(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(slug): Path<String>,
    Json(req): Json<UpsertReviewRequest>,
) -> Result<Json<RatingChangeResponse>, AuthError> {
    let user_id = get_user_id(&headers)?;
    let edition_id = base36::decode(&slug).ok_or(AuthError::InvalidToken)? as i32;

    if req.rating < 1.0 || req.rating > 5.0 || (req.rating * 4.0).fract().abs() > f32::EPSILON {
        return Err(AuthError::InvalidToken);
    }

    if let Some(ref text) = req.review_text {
        if text.len() > MAX_REVIEW_TEXT {
            return Err(AuthError::InvalidToken);
        }
    }

    let exists = sqlx::query_scalar::<_, i32>("SELECT id FROM editions WHERE id = ?")
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
        VALUES (?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE
            rating = VALUES(rating), review_text = VALUES(review_text), updated_at = NOW()
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
    let user_id = get_user_id(&headers)?;
    let edition_id = base36::decode(&slug).ok_or(AuthError::InvalidToken)? as i32;

    sqlx::query("DELETE FROM user_reviews WHERE user_id = ? AND edition_id = ?")
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
) -> Result<Json<TokenResponse>, AuthError> {
    let user_id = get_user_id(&headers)?;

    if let Some(ref username) = req.username {
        validate_username(username)?;

        let taken = sqlx::query_scalar::<_, i32>(
            "SELECT id FROM users WHERE LOWER(username) = LOWER(?) AND id != ?",
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
            username = COALESCE(?, username),
            display_name = COALESCE(?, display_name),
            bio = COALESCE(?, bio)
        WHERE id = ?
        "#,
    )
    .bind(&req.username)
    .bind(&req.display_name)
    .bind(&req.bio)
    .bind(user_id)
    .execute(&state.db)
    .await?;

    let updated = sqlx::query_as::<_, UserRow>(
        "SELECT id, password_hash, email_verified, username, display_name FROM users WHERE id = ?",
    )
    .bind(user_id)
    .fetch_one(&state.db)
    .await?;

    let email = extract_bearer_token(&headers)
        .and_then(|t| decode_jwt(&t).ok())
        .map(|c| c.email)
        .unwrap_or_default();

    let jwt = create_jwt(
        updated.id,
        &email,
        &updated.username,
        updated.display_name.as_deref(),
        updated.email_verified,
    )?;

    Ok(Json(TokenResponse { success: true, token: jwt }))
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
) -> Result<Json<CreateListResponse>, AuthError> {
    let user_id = get_user_id(&headers)?;

    if req.title.is_empty() || req.title.len() > 200 {
        return Err(AuthError::Validation(
            "Title must be 1-200 characters".into(),
        ));
    }

    let result = sqlx::query(
        "INSERT INTO user_lists (user_id, title, description) VALUES (?, ?, ?)",
    )
    .bind(user_id)
    .bind(&req.title)
    .bind(&req.description)
    .execute(&state.db)
    .await?;
    let list_id = result.last_insert_id() as i32;

    Ok(Json(CreateListResponse { success: true, id: list_id }))
}

async fn update_list(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(list_id): Path<i32>,
    Json(req): Json<CreateListRequest>,
) -> Result<Json<SuccessResponse>, AuthError> {
    let user_id = get_user_id(&headers)?;

    if req.title.is_empty() || req.title.len() > 200 {
        return Err(AuthError::Validation(
            "Title must be 1-200 characters".into(),
        ));
    }

    let rows = sqlx::query(
        "UPDATE user_lists SET title = ?, description = ?, updated_at = NOW() WHERE id = ? AND user_id = ?",
    )
    .bind(&req.title)
    .bind(&req.description)
    .bind(list_id)
    .bind(user_id)
    .execute(&state.db)
    .await?;

    if rows.rows_affected() == 0 {
        return Err(AuthError::InvalidToken);
    }

    Ok(Json(SuccessResponse { success: true }))
}

async fn delete_list(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(list_id): Path<i32>,
) -> Result<Json<SuccessResponse>, AuthError> {
    let user_id = get_user_id(&headers)?;

    sqlx::query("DELETE FROM user_lists WHERE id = ? AND user_id = ?")
        .bind(list_id)
        .bind(user_id)
        .execute(&state.db)
        .await?;

    Ok(Json(SuccessResponse { success: true }))
}

async fn add_work_to_list(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((list_id, slug)): Path<(i32, String)>,
) -> Result<Json<SuccessResponse>, AuthError> {
    let user_id = get_user_id(&headers)?;
    let work_id = base36::decode(&slug).ok_or(AuthError::InvalidToken)? as i32;

    let owns =
        sqlx::query_scalar::<_, i32>("SELECT id FROM user_lists WHERE id = ? AND user_id = ?")
            .bind(list_id)
            .bind(user_id)
            .fetch_optional(&state.db)
            .await?
            .is_some();

    if !owns {
        return Err(AuthError::Unauthorized);
    }

    let max_pos = sqlx::query_scalar::<_, Option<i32>>(
        "SELECT MAX(position) FROM user_list_works WHERE list_id = ?",
    )
    .bind(list_id)
    .fetch_one(&state.db)
    .await?
    .unwrap_or(-1);

    sqlx::query(
        r#"
        INSERT IGNORE INTO user_list_works (list_id, work_id, position)
        VALUES (?, ?, ?)
        "#,
    )
    .bind(list_id)
    .bind(work_id)
    .bind(max_pos + 1)
    .execute(&state.db)
    .await?;

    Ok(Json(SuccessResponse { success: true }))
}

async fn remove_work_from_list(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((list_id, slug)): Path<(i32, String)>,
) -> Result<Json<SuccessResponse>, AuthError> {
    let user_id = get_user_id(&headers)?;
    let work_id = base36::decode(&slug).ok_or(AuthError::InvalidToken)? as i32;

    let owns =
        sqlx::query_scalar::<_, i32>("SELECT id FROM user_lists WHERE id = ? AND user_id = ?")
            .bind(list_id)
            .bind(user_id)
            .fetch_optional(&state.db)
            .await?
            .is_some();

    if !owns {
        return Err(AuthError::Unauthorized);
    }

    sqlx::query("DELETE FROM user_list_works WHERE list_id = ? AND work_id = ?")
        .bind(list_id)
        .bind(work_id)
        .execute(&state.db)
        .await?;

    Ok(Json(SuccessResponse { success: true }))
}

async fn list_my_lists(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<MyListsResponse>, AuthError> {
    let user_id = get_user_id(&headers)?;

    let lists = sqlx::query_as::<_, ListRow>(
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

    let work_ids = sqlx::query_as::<_, ListWorkRow>(
        r#"
        SELECT ulw.list_id, ulw.work_id
        FROM user_list_works ulw
        JOIN user_lists ul ON ulw.list_id = ul.id
        WHERE ul.user_id = ?
        "#,
    )
    .bind(user_id)
    .fetch_all(&state.db)
    .await?;

    let lists = lists
        .into_iter()
        .map(|list| {
            let work_slugs = work_ids
                .iter()
                .filter(|lw| lw.list_id == list.id)
                .map(|lw| base36::encode(lw.work_id as i64))
                .collect();
            ListSummaryItem {
                id: list.id,
                title: list.title,
                description: list.description,
                work_count: list.work_count,
                work_slugs,
            }
        })
        .collect();

    Ok(Json(MyListsResponse { lists }))
}

async fn resolve_username_to_id(state: &AppState, username: &str) -> Result<i32, AuthError> {
    sqlx::query_scalar::<_, i32>("SELECT id FROM users WHERE LOWER(username) = LOWER(?)")
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
    let user_id = get_user_id(&headers)?;
    let target_id = resolve_username_to_id(&state, &username).await?;

    if user_id == target_id {
        return Err(AuthError::Validation("Cannot follow yourself".into()));
    }

    sqlx::query(
        r#"
        INSERT IGNORE INTO user_follows (follower_id, following_id)
        VALUES (?, ?)
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
    let user_id = get_user_id(&headers)?;
    let target_id = resolve_username_to_id(&state, &username).await?;

    sqlx::query("DELETE FROM user_follows WHERE follower_id = ? AND following_id = ?")
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
    let user_id = get_user_id(&headers)?;
    let target_id = resolve_username_to_id(&state, &username).await?;

    let following = sqlx::query_scalar::<_, i32>(
        "SELECT follower_id FROM user_follows WHERE follower_id = ? AND following_id = ?",
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
    let user_id = get_user_id(&headers)?;

    let rows = sqlx::query_as::<_, FollowingRow>(
        r#"
        SELECT u.username, u.display_name
        FROM user_follows uf
        JOIN users u ON uf.following_id = u.id
        WHERE uf.follower_id = ?
        ORDER BY uf.created_at DESC
        "#,
    )
    .bind(user_id)
    .fetch_all(&state.db)
    .await?;

    let following = rows
        .into_iter()
        .map(|row| FollowingUser {
            username: row.username,
            display_name: row.display_name,
        })
        .collect();

    Ok(Json(FollowingListResponse { following }))
}

async fn get_feed(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<FeedResponse>, AuthError> {
    let user_id = get_user_id(&headers)?;

    let rows = sqlx::query_as::<_, FeedRow>(
        r#"
        SELECT u.username, u.display_name, ur.rating, ur.review_text,
               ur.edition_id, e.title, e.work_id, ec.cover_id, ur.updated_at
        FROM user_follows uf
        JOIN user_reviews ur ON uf.following_id = ur.user_id
        JOIN users u ON ur.user_id = u.id
        JOIN editions e ON ur.edition_id = e.id
        LEFT JOIN edition_covers ec ON e.id = ec.edition_id AND ec.position = 0
        WHERE uf.follower_id = ?
        ORDER BY ur.updated_at DESC
        LIMIT 20
        "#,
    )
    .bind(user_id)
    .fetch_all(&state.db)
    .await?;

    let feed = rows
        .into_iter()
        .map(|row| FeedItem {
            username: row.username,
            display_name: row.display_name,
            rating: row.rating,
            review_text: row.review_text,
            edition_slug: base36::encode(row.edition_id as i64),
            edition_title: row.title,
            work_slug: base36::encode(row.work_id as i64),
            cover_id: row.cover_id,
            updated_at: row.updated_at,
        })
        .collect();

    Ok(Json(FeedResponse { feed }))
}

pub fn extract_bearer_token(headers: &HeaderMap) -> Option<String> {
    let auth_header = headers.get("authorization")?.to_str().ok()?;
    auth_header
        .strip_prefix("Bearer ")
        .map(|t| t.to_string())
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
        .json(&ResendEmail {
            from: from_email,
            to: vec![email.to_string()],
            subject: "Verify your mylib account".into(),
            html: format!(
                r#"<p>Welcome to mylib!</p>
                <p>Please click the link below to verify your email address:</p>
                <p><a href="{}">Verify Email</a></p>
                <p>This link will expire in 24 hours.</p>"#,
                verify_url
            ),
            text: format!(
                "Welcome to mylib!\n\nPlease verify your email by visiting: {}\n\nThis link will expire in 24 hours.",
                verify_url
            ),
        })
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
                token: None,
                    }),
        )
            .into_response()
    }
}
