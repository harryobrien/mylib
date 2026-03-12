use crate::{db, search::SearchIndex};
use sqlx::PgPool;

pub async fn build_indexes(pool: &PgPool, search: &SearchIndex) -> anyhow::Result<()> {
    let (works_result, authors_result, editions_result) = tokio::join!(
        index_works(pool, search),
        index_authors(pool, search),
        index_editions(pool, search),
    );

    works_result?;
    authors_result?;
    editions_result?;

    tracing::info!("Indexing complete");
    Ok(())
}

async fn index_works(pool: &PgPool, search: &SearchIndex) -> anyhow::Result<()> {
    const BATCH_SIZE: i64 = 10000;

    tracing::info!("Indexing works...");
    let mut writer = search.works.writer()?;
    let total = db::count_works(pool).await?;
    let mut offset = 0i64;

    while offset < total {
        let works = db::get_works_for_indexing(pool, offset, BATCH_SIZE).await?;
        for w in &works {
            let year = extract_year(&w.first_publish_date);
            let mut doc = tantivy::TantivyDocument::new();
            doc.add_i64(search.works.fields.id, w.id as i64);
            doc.add_text(search.works.fields.key, &w.key);
            doc.add_text(search.works.fields.title, &w.title);
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
                doc.add_text(search.works.fields.author_names, a);
            }
            if let Some(y) = year {
                doc.add_i64(search.works.fields.first_publish_year, y);
            }
            writer.add_document(doc)?;
        }
        offset += BATCH_SIZE;
        tracing::info!("  Works: {offset}/{total}");
    }
    writer.commit()?;
    Ok(())
}

async fn index_authors(pool: &PgPool, search: &SearchIndex) -> anyhow::Result<()> {
    const BATCH_SIZE: i64 = 10000;

    tracing::info!("Indexing authors...");
    let mut writer = search.authors.writer()?;
    let total = db::count_authors(pool).await?;
    let mut offset = 0i64;

    while offset < total {
        let authors = db::get_authors_for_indexing(pool, offset, BATCH_SIZE).await?;
        for a in &authors {
            let mut doc = tantivy::TantivyDocument::new();
            doc.add_i64(search.authors.fields.id, a.id as i64);
            doc.add_text(search.authors.fields.key, &a.key);
            doc.add_text(search.authors.fields.name, &a.name);
            if let Some(ref alt) = a.alternate_names {
                doc.add_text(search.authors.fields.alternate_names, alt);
            }
            if let Some(ref bio) = a.bio {
                doc.add_text(search.authors.fields.bio, bio);
            }
            writer.add_document(doc)?;
        }
        offset += BATCH_SIZE;
        tracing::info!("  Authors: {offset}/{total}");
    }
    writer.commit()?;
    Ok(())
}

async fn index_editions(pool: &PgPool, search: &SearchIndex) -> anyhow::Result<()> {
    const BATCH_SIZE: i64 = 10000;

    tracing::info!("Indexing editions...");
    let mut writer = search.editions.writer()?;
    let total = db::count_editions(pool).await?;
    let mut offset = 0i64;

    while offset < total {
        let editions = db::get_editions_for_indexing(pool, offset, BATCH_SIZE).await?;
        for e in &editions {
            let year = extract_year(&e.publish_date);
            let mut doc = tantivy::TantivyDocument::new();
            doc.add_i64(search.editions.fields.id, e.id as i64);
            doc.add_text(search.editions.fields.key, &e.key);
            doc.add_i64(search.editions.fields.work_id, e.work_id as i64);
            doc.add_text(search.editions.fields.work_key, &e.work_key);
            doc.add_text(search.editions.fields.title, &e.title);
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
            writer.add_document(doc)?;
        }
        offset += BATCH_SIZE;
        tracing::info!("  Editions: {offset}/{total}");
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
