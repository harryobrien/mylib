use crate::{
    db,
    search::{generate_edge_ngrams, normalize_for_search, SearchIndex},
};
use sqlx::PgPool;

pub async fn build_missing_indexes(pool: &PgPool, search: &SearchIndex) -> anyhow::Result<()> {
    let (db_works, db_authors, db_editions) = tokio::join!(
        db::count_works(pool),
        db::count_authors(pool),
        db::count_editions(pool),
    );
    let db_works = db_works?;
    let db_authors = db_authors?;
    let db_editions = db_editions?;

    let idx_works = search.works.doc_count() as i64;
    let idx_authors = search.authors.doc_count() as i64;
    let idx_editions = search.editions.doc_count() as i64;

    let needs_works = idx_works < db_works;
    let needs_authors = idx_authors < db_authors;
    let needs_editions = idx_editions < db_editions;

    if !needs_works && !needs_authors && !needs_editions {
        return Ok(());
    }

    let works_start = if needs_works {
        search.works.max_id()?.unwrap_or(0)
    } else {
        0
    };
    let authors_start = if needs_authors {
        search.authors.max_id()?.unwrap_or(0)
    } else {
        0
    };
    let editions_start = if needs_editions {
        search.editions.max_id()?.unwrap_or(0)
    } else {
        0
    };

    tracing::info!(
        "Indexing: works={}/{} (from {}), authors={}/{} (from {}), editions={}/{} (from {})",
        idx_works,
        db_works,
        works_start,
        idx_authors,
        db_authors,
        authors_start,
        idx_editions,
        db_editions,
        editions_start,
    );

    let works_fut = async {
        if needs_works {
            index_works(pool, search, works_start).await
        } else {
            Ok(())
        }
    };
    let authors_fut = async {
        if needs_authors {
            index_authors(pool, search, authors_start).await
        } else {
            Ok(())
        }
    };
    let editions_fut = async {
        if needs_editions {
            index_editions(pool, search, editions_start).await
        } else {
            Ok(())
        }
    };

    let (works_result, authors_result, editions_result) =
        tokio::join!(works_fut, authors_fut, editions_fut);

    works_result?;
    authors_result?;
    editions_result?;

    tracing::info!("Indexing complete");
    Ok(())
}

pub async fn rebuild_all_indexes(pool: &PgPool, search: &SearchIndex) -> anyhow::Result<()> {
    tracing::info!("Rebuilding all indexes...");

    let (works_result, authors_result, editions_result) = tokio::join!(
        index_works(pool, search, 0),
        index_authors(pool, search, 0),
        index_editions(pool, search, 0),
    );

    works_result?;
    authors_result?;
    editions_result?;

    tracing::info!("Indexing complete");
    Ok(())
}

async fn index_works(pool: &PgPool, search: &SearchIndex, start_id: i32) -> anyhow::Result<()> {
    const BATCH_SIZE: i64 = 10000;

    tracing::info!("Indexing works from id {}...", start_id);
    let mut writer = search.works.writer()?;
    let total = db::count_works(pool).await?;
    let mut last_id = start_id;
    let mut indexed = search.works.doc_count() as i64;

    loop {
        let works = db::get_works_for_indexing(pool, last_id, BATCH_SIZE).await?;
        if works.is_empty() {
            break;
        }
        for w in &works {
            let year = extract_year(&w.first_publish_date);
            let mut doc = tantivy::TantivyDocument::new();
            doc.add_i64(search.works.fields.id, w.id as i64);
            doc.add_text(search.works.fields.key, &w.key);
            doc.add_text(search.works.fields.title, &w.title);
            doc.add_text(
                search.works.fields.title_ngram,
                &generate_edge_ngrams(&w.title, 2, 8),
            );
            if let Some(ref s) = w.subtitle {
                doc.add_text(search.works.fields.subtitle, s);
            }
            if let Some(ref d) = w.description {
                doc.add_text(search.works.fields.description, d);
            }
            if let Some(ref s) = w.subjects {
                doc.add_text(search.works.fields.subjects, s);
            }
            if let Some(ref a) = w.author_names {
                let normalized = normalize_for_search(a);
                doc.add_text(search.works.fields.author_names, &normalized);
                doc.add_text(
                    search.works.fields.author_names_ngram,
                    &generate_edge_ngrams(&normalized, 2, 8),
                );
            }
            if let Some(y) = year {
                doc.add_i64(search.works.fields.first_publish_year, y);
            }
            if let Some(c) = w.cover_id {
                doc.add_i64(search.works.fields.cover_id, c);
            }
            doc.add_f64(
                search.works.fields.popularity,
                w.popularity_score.unwrap_or(0.0),
            );
            if let Some(rc) = w.ratings_count {
                doc.add_i64(search.works.fields.ratings_count, rc as i64);
            }
            if let Some(ra) = w.rating_avg {
                doc.add_f64(search.works.fields.rating_avg, ra as f64);
            }
            writer.add_document(doc)?;
            last_id = w.id;
        }
        indexed += works.len() as i64;
        tracing::info!("  Works: {indexed}/{total}");
    }
    writer.commit()?;
    Ok(())
}

async fn index_authors(pool: &PgPool, search: &SearchIndex, start_id: i32) -> anyhow::Result<()> {
    const BATCH_SIZE: i64 = 10000;

    tracing::info!("Indexing authors from id {}...", start_id);
    let mut writer = search.authors.writer()?;
    let total = db::count_authors(pool).await?;
    let mut last_id = start_id;
    let mut indexed = search.authors.doc_count() as i64;

    loop {
        let authors = db::get_authors_for_indexing(pool, last_id, BATCH_SIZE).await?;
        if authors.is_empty() {
            break;
        }
        for a in &authors {
            let mut doc = tantivy::TantivyDocument::new();
            doc.add_i64(search.authors.fields.id, a.id as i64);
            doc.add_text(search.authors.fields.key, &a.key);
            let normalized_name = normalize_for_search(&a.name);
            doc.add_text(search.authors.fields.name, &normalized_name);
            doc.add_text(
                search.authors.fields.name_ngram,
                &generate_edge_ngrams(&normalized_name, 2, 8),
            );
            if let Some(ref alt) = a.alternate_names {
                doc.add_text(search.authors.fields.alternate_names, alt);
            }
            if let Some(ref bio) = a.bio {
                doc.add_text(search.authors.fields.bio, bio);
            }
            doc.add_f64(
                search.authors.fields.popularity,
                a.popularity_score.unwrap_or(0.0),
            );
            writer.add_document(doc)?;
            last_id = a.id;
        }
        indexed += authors.len() as i64;
        tracing::info!("  Authors: {indexed}/{total}");
    }
    writer.commit()?;
    Ok(())
}

async fn index_editions(pool: &PgPool, search: &SearchIndex, start_id: i32) -> anyhow::Result<()> {
    const BATCH_SIZE: i64 = 10000;

    tracing::info!("Indexing editions from id {}...", start_id);
    let mut writer = search.editions.writer()?;
    let total = db::count_editions(pool).await?;
    let mut last_id = start_id;
    let mut indexed = search.editions.doc_count() as i64;

    loop {
        let editions = db::get_editions_for_indexing(pool, last_id, BATCH_SIZE).await?;
        if editions.is_empty() {
            break;
        }
        for e in &editions {
            let year = extract_year(&e.publish_date);
            let mut doc = tantivy::TantivyDocument::new();
            doc.add_i64(search.editions.fields.id, e.id as i64);
            doc.add_text(search.editions.fields.key, &e.key);
            doc.add_i64(search.editions.fields.work_id, e.work_id as i64);
            doc.add_text(search.editions.fields.work_key, &e.work_key);
            doc.add_text(search.editions.fields.title, &e.title);
            doc.add_text(
                search.editions.fields.title_ngram,
                &generate_edge_ngrams(&e.title, 2, 8),
            );
            if let Some(ref s) = e.subtitle {
                doc.add_text(search.editions.fields.subtitle, s);
            }
            if let Some(ref i) = e.isbns {
                doc.add_text(search.editions.fields.isbns, i);
            }
            if let Some(ref p) = e.publishers {
                doc.add_text(search.editions.fields.publishers, p);
            }
            if let Some(y) = year {
                doc.add_i64(search.editions.fields.publish_year, y);
            }
            if let Some(c) = e.cover_id {
                doc.add_i64(search.editions.fields.cover_id, c);
            }
            doc.add_f64(
                search.editions.fields.popularity,
                e.popularity_score.unwrap_or(0.0),
            );
            if let Some(rc) = e.ratings_count {
                doc.add_i64(search.editions.fields.ratings_count, rc as i64);
            }
            if let Some(ra) = e.rating_avg {
                doc.add_f64(search.editions.fields.rating_avg, ra as f64);
            }
            writer.add_document(doc)?;
            last_id = e.id;
        }
        indexed += editions.len() as i64;
        tracing::info!("  Editions: {indexed}/{total}");
    }
    writer.commit()?;
    Ok(())
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
