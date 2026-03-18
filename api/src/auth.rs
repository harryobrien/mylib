use std::sync::Arc;

use argon2::{
    password_hash::{rand_core::OsRng, PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};
use axum::{
    extract::{Path, Query, State},
    http::{header::SET_COOKIE, HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{delete, get, post, put},
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
}

fn generate_token() -> String {
    let bytes: [u8; 32] = rand::thread_rng().gen();
    hex::encode(bytes)
}

#[derive(Deserialize)]
pub struct RegisterRequest {
    email: String,
    password: String,
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

    let existing = sqlx::query_scalar::<_, i32>("SELECT id FROM users WHERE email = $1")
        .bind(&req.email)
        .fetch_optional(&state.db)
        .await?;

    if existing.is_some() {
        return Err(AuthError::EmailTaken);
    }

    let salt = SaltString::generate(&mut OsRng);
    let argon2 = Argon2::default();
    let password_hash = argon2
        .hash_password(req.password.as_bytes(), &salt)
        .map_err(|_| AuthError::Internal)?
        .to_string();

    let user_id = sqlx::query_scalar::<_, i32>(
        "INSERT INTO users (email, password_hash) VALUES ($1, $2) RETURNING id",
    )
    .bind(&req.email)
    .bind(&password_hash)
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
    let user = sqlx::query_as::<_, (i32, String, bool)>(
        "SELECT id, password_hash, email_verified FROM users WHERE email = $1",
    )
    .bind(&req.email)
    .fetch_optional(&state.db)
    .await?
    .ok_or(AuthError::InvalidCredentials)?;

    let (user_id, password_hash, email_verified) = user;

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
        Some(token) => sqlx::query_as::<_, (i32, String, bool)>(
            r#"
            SELECT u.id, u.email, u.email_verified
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
        .map(|(id, email, email_verified)| UserInfo {
            id,
            email,
            email_verified,
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
) -> Result<Json<serde_json::Value>, AuthError> {
    let user_id = get_user_id(&state, &headers).await?;
    let edition_id = base36::decode(&slug).ok_or(AuthError::InvalidToken)? as i32;

    if !["reading", "want_to_read", "finished", "did_not_finish"].contains(&req.status.as_str()) {
        return Err(AuthError::InvalidToken);
    }

    // Verify edition exists
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
        INSERT INTO user_editions (user_id, edition_id, status)
        VALUES ($1, $2, $3)
        ON CONFLICT (user_id, edition_id) DO UPDATE SET status = $3, created_at = NOW()
        "#,
    )
    .bind(user_id)
    .bind(edition_id)
    .bind(&req.status)
    .execute(&state.db)
    .await?;

    Ok(Json(serde_json::json!({
        "success": true,
        "status": req.status
    })))
}

async fn remove_edition(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(slug): Path<String>,
) -> Result<Json<serde_json::Value>, AuthError> {
    let user_id = get_user_id(&state, &headers).await?;
    let edition_id = base36::decode(&slug).ok_or(AuthError::InvalidToken)? as i32;

    sqlx::query("DELETE FROM user_editions WHERE user_id = $1 AND edition_id = $2")
        .bind(user_id)
        .bind(edition_id)
        .execute(&state.db)
        .await?;

    Ok(Json(serde_json::json!({ "success": true })))
}

async fn list_user_editions(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, AuthError> {
    let user_id = get_user_id(&state, &headers).await?;

    let editions = sqlx::query_as::<_, (i32, String, String, i32, Option<i64>)>(
        r#"
        SELECT e.id, e.title, ue.status, e.work_id, ec.cover_id
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

    let editions: Vec<_> = editions
        .into_iter()
        .map(|(id, title, status, work_id, cover_id)| {
            serde_json::json!({
                "slug": base36::encode(id as i64),
                "edition_id": id,
                "work_slug": base36::encode(work_id as i64),
                "title": title,
                "status": status,
                "cover_id": cover_id
            })
        })
        .collect();

    Ok(Json(serde_json::json!({
        "success": true,
        "editions": editions
    })))
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
