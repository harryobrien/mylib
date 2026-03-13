use sqlx::PgPool;

#[derive(Debug, serde::Serialize, sqlx::FromRow)]
pub struct Work {
    pub id: i32,
    pub key: String,
    pub title: String,
    pub subtitle: Option<String>,
    pub first_publish_date: Option<String>,
    pub description: Option<String>,
}

#[derive(Debug, serde::Serialize, sqlx::FromRow)]
pub struct Author {
    pub id: i32,
    pub key: String,
    pub name: String,
    pub fuller_name: Option<String>,
    pub bio: Option<String>,
    pub birth_date: Option<String>,
    pub death_date: Option<String>,
}

#[derive(Debug, serde::Serialize, sqlx::FromRow)]
pub struct Edition {
    pub id: i32,
    pub key: String,
    pub work_id: i32,
    pub title: String,
    pub subtitle: Option<String>,
    pub publish_date: Option<String>,
    pub publishers: Option<String>,
    pub physical_format: Option<String>,
    pub number_of_pages: Option<i32>,
    pub cover_id: Option<i64>,
}

pub async fn get_work_by_id(pool: &PgPool, id: i32) -> sqlx::Result<Option<Work>> {
    sqlx::query_as(
        "SELECT id, key, title, subtitle, first_publish_date, description FROM works WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(pool)
    .await
}

pub async fn get_work_by_key(pool: &PgPool, key: &str) -> sqlx::Result<Option<Work>> {
    sqlx::query_as(
        "SELECT id, key, title, subtitle, first_publish_date, description FROM works WHERE key = $1"
    )
    .bind(key)
    .fetch_optional(pool)
    .await
}

pub async fn get_author_by_id(pool: &PgPool, id: i32) -> sqlx::Result<Option<Author>> {
    sqlx::query_as(
        "SELECT id, key, name, fuller_name, bio, birth_date, death_date FROM authors WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(pool)
    .await
}

pub async fn get_author_by_key(pool: &PgPool, key: &str) -> sqlx::Result<Option<Author>> {
    sqlx::query_as(
        "SELECT id, key, name, fuller_name, bio, birth_date, death_date FROM authors WHERE key = $1"
    )
    .bind(key)
    .fetch_optional(pool)
    .await
}

pub async fn get_edition_by_id(pool: &PgPool, id: i32) -> sqlx::Result<Option<Edition>> {
    sqlx::query_as(
        r#"
        SELECT e.id, e.key, e.work_id, e.title, e.subtitle, e.publish_date,
               string_agg(DISTINCT ep.publisher, ', ') as publishers,
               e.physical_format, e.number_of_pages,
               (SELECT ec.cover_id FROM edition_covers ec
                JOIN cover_metadata cm ON ec.cover_id = cm.id
                WHERE ec.edition_id = e.id
                ORDER BY ec.position LIMIT 1) as cover_id
        FROM editions e
        LEFT JOIN edition_publishers ep ON e.id = ep.edition_id
        WHERE e.id = $1
        GROUP BY e.id
        "#,
    )
    .bind(id)
    .fetch_optional(pool)
    .await
}

pub async fn get_edition_by_key(pool: &PgPool, key: &str) -> sqlx::Result<Option<Edition>> {
    sqlx::query_as(
        r#"
        SELECT e.id, e.key, e.work_id, e.title, e.subtitle, e.publish_date,
               string_agg(DISTINCT ep.publisher, ', ') as publishers,
               e.physical_format, e.number_of_pages,
               (SELECT ec.cover_id FROM edition_covers ec
                JOIN cover_metadata cm ON ec.cover_id = cm.id
                WHERE ec.edition_id = e.id
                ORDER BY ec.position LIMIT 1) as cover_id
        FROM editions e
        LEFT JOIN edition_publishers ep ON e.id = ep.edition_id
        WHERE e.key = $1
        GROUP BY e.id
        "#,
    )
    .bind(key)
    .fetch_optional(pool)
    .await
}

pub async fn get_author_works(pool: &PgPool, author_id: i32) -> sqlx::Result<Vec<Work>> {
    sqlx::query_as(
        r#"
        SELECT w.id, w.key, w.title, w.subtitle, w.first_publish_date, w.description
        FROM works w
        JOIN work_authors wa ON w.id = wa.work_id
        WHERE wa.author_id = $1
        ORDER BY w.first_publish_date DESC NULLS LAST
        "#,
    )
    .bind(author_id)
    .fetch_all(pool)
    .await
}

pub async fn get_work_authors(pool: &PgPool, work_id: i32) -> sqlx::Result<Vec<Author>> {
    sqlx::query_as(
        r#"
        SELECT a.id, a.key, a.name, a.fuller_name, a.bio, a.birth_date, a.death_date
        FROM authors a
        JOIN work_authors wa ON a.id = wa.author_id
        WHERE wa.work_id = $1
        ORDER BY wa.position
        "#,
    )
    .bind(work_id)
    .fetch_all(pool)
    .await
}

pub async fn get_work_editions(pool: &PgPool, work_id: i32) -> sqlx::Result<Vec<Edition>> {
    sqlx::query_as(
        r#"
        SELECT e.id, e.key, e.work_id, e.title, e.subtitle, e.publish_date,
               string_agg(DISTINCT ep.publisher, ', ') as publishers,
               e.physical_format, e.number_of_pages,
               (SELECT ec.cover_id FROM edition_covers ec
                JOIN cover_metadata cm ON ec.cover_id = cm.id
                WHERE ec.edition_id = e.id
                ORDER BY ec.position LIMIT 1) as cover_id
        FROM editions e
        LEFT JOIN edition_publishers ep ON e.id = ep.edition_id
        WHERE e.work_id = $1
        GROUP BY e.id
        ORDER BY e.publish_date DESC NULLS LAST
        "#,
    )
    .bind(work_id)
    .fetch_all(pool)
    .await
}

pub async fn get_edition_isbns(pool: &PgPool, edition_id: i32) -> sqlx::Result<Vec<String>> {
    let rows: Vec<(String,)> =
        sqlx::query_as("SELECT isbn FROM edition_isbns WHERE edition_id = $1")
            .bind(edition_id)
            .fetch_all(pool)
            .await?;
    Ok(rows.into_iter().map(|(isbn,)| isbn).collect())
}

#[derive(Debug, serde::Serialize, sqlx::FromRow)]
pub struct CoverMetadata {
    pub id: i64,
    pub width: i32,
    pub height: i32,
}

pub async fn get_edition_covers(
    pool: &PgPool,
    edition_id: i32,
) -> sqlx::Result<Vec<CoverMetadata>> {
    sqlx::query_as(
        r#"
        SELECT cm.id, cm.width, cm.height
        FROM edition_covers ec
        JOIN cover_metadata cm ON ec.cover_id = cm.id
        WHERE ec.edition_id = $1
        ORDER BY ec.position
        "#,
    )
    .bind(edition_id)
    .fetch_all(pool)
    .await
}

#[derive(Debug, sqlx::FromRow)]
pub struct WorkForIndex {
    pub id: i32,
    pub key: String,
    pub title: String,
    pub subtitle: Option<String>,
    pub description: Option<String>,
    pub first_publish_date: Option<String>,
    pub subjects: Option<String>,
    pub author_names: Option<String>,
    pub cover_id: Option<i64>,
    pub popularity_score: Option<f64>,
    pub ratings_count: Option<i32>,
    pub rating_avg: Option<f32>,
}

pub async fn get_works_for_indexing(
    pool: &PgPool,
    after_id: i32,
    limit: i64,
) -> sqlx::Result<Vec<WorkForIndex>> {
    sqlx::query_as(
        r#"
        SELECT w.id, w.key, w.title, w.subtitle, w.description, w.first_publish_date,
               (SELECT string_agg(DISTINCT ws.subject, ' | ')
                FROM work_subjects ws WHERE ws.work_id = w.id) as subjects,
               (SELECT string_agg(DISTINCT a.name, ' | ')
                FROM work_authors wa
                JOIN authors a ON wa.author_id = a.id
                WHERE wa.work_id = w.id) as author_names,
               NULL::bigint as cover_id,
               (SELECT compute_popularity_score(wp.ratings_count, wp.ratings_sum,
                    wp.want_to_read, wp.currently_reading, wp.already_read)
                FROM work_popularity wp WHERE wp.work_id = w.id)::float8 as popularity_score,
               (SELECT wp.ratings_count FROM work_popularity wp WHERE wp.work_id = w.id) as ratings_count,
               (SELECT wp.ratings_sum::real / NULLIF(wp.ratings_count, 0)
                FROM work_popularity wp WHERE wp.work_id = w.id)::float4 as rating_avg
        FROM works w
        WHERE w.id > $1
        ORDER BY w.id
        LIMIT $2
        "#,
    )
    .bind(after_id)
    .bind(limit)
    .fetch_all(pool)
    .await
}

#[derive(Debug, sqlx::FromRow)]
pub struct AuthorForIndex {
    pub id: i32,
    pub key: String,
    pub name: String,
    pub alternate_names: Option<String>,
    pub bio: Option<String>,
    pub popularity_score: Option<f64>,
}

pub async fn get_authors_for_indexing(
    pool: &PgPool,
    after_id: i32,
    limit: i64,
) -> sqlx::Result<Vec<AuthorForIndex>> {
    sqlx::query_as(
        r#"
        SELECT a.id, a.key, a.name,
               (SELECT string_agg(DISTINCT aan.name, ' | ')
                FROM author_alternate_names aan WHERE aan.author_id = a.id) as alternate_names,
               a.bio,
               ap.popularity_score::float8 as popularity_score
        FROM authors a
        LEFT JOIN author_popularity ap ON ap.author_id = a.id
        WHERE a.id > $1
        ORDER BY a.id
        LIMIT $2
        "#,
    )
    .bind(after_id)
    .bind(limit)
    .fetch_all(pool)
    .await
}

#[derive(Debug, sqlx::FromRow)]
pub struct EditionForIndex {
    pub id: i32,
    pub key: String,
    pub work_id: i32,
    pub work_key: String,
    pub title: String,
    pub subtitle: Option<String>,
    pub isbns: Option<String>,
    pub publishers: Option<String>,
    pub publish_date: Option<String>,
    pub cover_id: Option<i64>,
    pub popularity_score: Option<f64>,
    pub ratings_count: Option<i32>,
    pub rating_avg: Option<f32>,
}

pub async fn get_editions_for_indexing(
    pool: &PgPool,
    after_id: i32,
    limit: i64,
) -> sqlx::Result<Vec<EditionForIndex>> {
    sqlx::query_as(
        r#"
        SELECT e.id, e.key, e.work_id, w.key as work_key,
               e.title, e.subtitle,
               (SELECT string_agg(DISTINCT ei.isbn, ' ')
                FROM edition_isbns ei WHERE ei.edition_id = e.id) as isbns,
               (SELECT string_agg(DISTINCT ep.publisher, ' | ')
                FROM edition_publishers ep WHERE ep.edition_id = e.id) as publishers,
               e.publish_date,
               NULL::bigint as cover_id,
               (SELECT compute_popularity_score(edp.ratings_count, edp.ratings_sum,
                    edp.want_to_read, edp.currently_reading, edp.already_read)
                FROM edition_popularity edp WHERE edp.edition_id = e.id)::float8 as popularity_score,
               (SELECT edp.ratings_count FROM edition_popularity edp WHERE edp.edition_id = e.id) as ratings_count,
               (SELECT edp.ratings_sum::real / NULLIF(edp.ratings_count, 0)
                FROM edition_popularity edp WHERE edp.edition_id = e.id)::float4 as rating_avg
        FROM editions e
        JOIN works w ON w.id = e.work_id
        WHERE e.id > $1
        ORDER BY e.id
        LIMIT $2
        "#,
    )
    .bind(after_id)
    .bind(limit)
    .fetch_all(pool)
    .await
}

#[derive(Debug, serde::Serialize, sqlx::FromRow)]
pub struct WorkPopularity {
    pub ratings_count: i32,
    pub rating_avg: Option<f32>,
    pub want_to_read: i32,
    pub currently_reading: i32,
    pub already_read: i32,
}

pub async fn get_work_popularity(pool: &PgPool, work_id: i32) -> sqlx::Result<Option<WorkPopularity>> {
    sqlx::query_as(
        r#"
        SELECT ratings_count,
               (ratings_sum::real / NULLIF(ratings_count, 0))::float4 as rating_avg,
               want_to_read, currently_reading, already_read
        FROM work_popularity
        WHERE work_id = $1
        "#,
    )
    .bind(work_id)
    .fetch_optional(pool)
    .await
}

#[derive(Debug, Clone, serde::Serialize, sqlx::FromRow)]
pub struct EditionPopularity {
    pub ratings_count: i32,
    pub rating_avg: Option<f32>,
    pub want_to_read: i32,
    pub currently_reading: i32,
    pub already_read: i32,
}

pub async fn get_edition_popularity(pool: &PgPool, edition_id: i32) -> sqlx::Result<Option<EditionPopularity>> {
    sqlx::query_as(
        r#"
        SELECT ratings_count,
               (ratings_sum::real / NULLIF(ratings_count, 0))::float4 as rating_avg,
               want_to_read, currently_reading, already_read
        FROM edition_popularity
        WHERE edition_id = $1
        "#,
    )
    .bind(edition_id)
    .fetch_optional(pool)
    .await
}

#[derive(Debug, sqlx::FromRow)]
pub struct EditionPopularityWithId {
    pub edition_id: i32,
    pub ratings_count: i32,
    pub rating_avg: Option<f32>,
    pub want_to_read: i32,
    pub currently_reading: i32,
    pub already_read: i32,
}

pub async fn get_edition_popularities_for_work(pool: &PgPool, work_id: i32) -> sqlx::Result<Vec<EditionPopularityWithId>> {
    sqlx::query_as(
        r#"
        SELECT ep.edition_id, ep.ratings_count,
               (ep.ratings_sum::real / NULLIF(ep.ratings_count, 0))::float4 as rating_avg,
               ep.want_to_read, ep.currently_reading, ep.already_read
        FROM edition_popularity ep
        JOIN editions e ON e.id = ep.edition_id
        WHERE e.work_id = $1
        "#,
    )
    .bind(work_id)
    .fetch_all(pool)
    .await
}

pub async fn count_works(pool: &PgPool) -> sqlx::Result<i64> {
    let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM works")
        .fetch_one(pool)
        .await?;
    Ok(count)
}

pub async fn count_authors(pool: &PgPool) -> sqlx::Result<i64> {
    let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM authors")
        .fetch_one(pool)
        .await?;
    Ok(count)
}

pub async fn count_editions(pool: &PgPool) -> sqlx::Result<i64> {
    let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM editions")
        .fetch_one(pool)
        .await?;
    Ok(count)
}

#[derive(Debug, sqlx::FromRow)]
pub struct WorkCover {
    pub work_id: i32,
    pub cover_id: i64,
}

/// Get covers for works (for backfilling index)
pub async fn get_work_covers(
    pool: &PgPool,
    after_work_id: i32,
    limit: i64,
) -> sqlx::Result<Vec<WorkCover>> {
    sqlx::query_as(
        r#"
        SELECT w.id as work_id, cover.cover_id
        FROM works w
        CROSS JOIN LATERAL (
            SELECT ec.cover_id
            FROM editions e
            JOIN edition_covers ec ON ec.edition_id = e.id
            JOIN cover_metadata cm ON ec.cover_id = cm.id
            WHERE e.work_id = w.id
            ORDER BY ec.position
            LIMIT 1
        ) cover
        WHERE w.id > $1
        ORDER BY w.id
        LIMIT $2
        "#,
    )
    .bind(after_work_id)
    .bind(limit)
    .fetch_all(pool)
    .await
}
