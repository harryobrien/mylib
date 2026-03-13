use crate::base36;
use std::path::Path;
use tantivy::{
    collector::TopDocs,
    query::{AllQuery, BooleanQuery, BoostQuery, FuzzyTermQuery, Occur, Query, TermQuery},
    schema::*,
    Index, IndexReader, IndexWriter, Order, ReloadPolicy, Term,
};

/// Generate edge ngrams for each word in text.
/// "Virginia Woolf" -> "vi vir virg virgi virgin virgini virginia wo woo wool woolf"
pub fn generate_edge_ngrams(text: &str, min: usize, max: usize) -> String {
    text.split_whitespace()
        .flat_map(|word| {
            let word_lower = word.to_lowercase();
            let chars: Vec<char> = word_lower.chars().collect();
            (min..=max.min(chars.len())).map(move |n| chars[..n].iter().collect::<String>())
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Build a search query: exact matches (boosted) + fuzzy + ngram prefix on last term
fn build_fuzzy_query(
    query: &str,
    fields: &[Field],
    ngram_fields: &[Field],
    _schema: &Schema,
) -> Box<dyn Query> {
    let query_lower = query.to_lowercase();
    let terms: Vec<&str> = query_lower.split_whitespace().collect();

    if terms.is_empty() {
        return Box::new(tantivy::query::EmptyQuery);
    }

    let term_queries: Vec<(Occur, Box<dyn Query>)> = terms
        .iter()
        .enumerate()
        .map(|(i, term)| {
            let is_last = i == terms.len() - 1;
            let field_queries: Vec<(Occur, Box<dyn Query>)> = fields
                .iter()
                .flat_map(|field| {
                    let mut queries: Vec<(Occur, Box<dyn Query>)> = Vec::new();
                    let tantivy_term = Term::from_field_text(*field, term);

                    // Exact match with boost
                    queries.push((
                        Occur::Should,
                        Box::new(BoostQuery::new(
                            Box::new(TermQuery::new(
                                tantivy_term.clone(),
                                IndexRecordOption::Basic,
                            )),
                            2.0,
                        )),
                    ));

                    // Fuzzy match for typo tolerance (skip for very short terms)
                    if term.len() >= 3 {
                        queries.push((
                            Occur::Should,
                            Box::new(FuzzyTermQuery::new(tantivy_term.clone(), 1, true)),
                        ));
                    }

                    queries
                })
                .chain(
                    // For last term: also search ngram fields for prefix matching
                    if is_last && term.len() >= 2 {
                        ngram_fields
                            .iter()
                            .map(|field| {
                                let tantivy_term = Term::from_field_text(*field, term);
                                (
                                    Occur::Should,
                                    Box::new(BoostQuery::new(
                                        Box::new(TermQuery::new(
                                            tantivy_term,
                                            IndexRecordOption::Basic,
                                        )),
                                        1.5,
                                    )) as Box<dyn Query>,
                                )
                            })
                            .collect::<Vec<_>>()
                    } else {
                        vec![]
                    },
                )
                .collect();

            let field_bool: Box<dyn Query> = Box::new(BooleanQuery::new(field_queries));
            (Occur::Must, field_bool)
        })
        .collect();

    Box::new(BooleanQuery::new(term_queries))
}

pub struct SearchIndex {
    pub works: WorksIndex,
    pub authors: AuthorsIndex,
    pub editions: EditionsIndex,
}

impl SearchIndex {
    pub fn open_or_create(base_path: &str) -> anyhow::Result<Self> {
        let base = Path::new(base_path);
        std::fs::create_dir_all(base)?;

        Ok(Self {
            works: WorksIndex::open_or_create(&base.join("works"))?,
            authors: AuthorsIndex::open_or_create(&base.join("authors"))?,
            editions: EditionsIndex::open_or_create(&base.join("editions"))?,
        })
    }

    pub fn is_empty(&self) -> bool {
        self.works.doc_count() == 0
            && self.authors.doc_count() == 0
            && self.editions.doc_count() == 0
    }
}

// --- Works Index ---

pub struct WorksIndex {
    pub index: Index,
    pub reader: IndexReader,
    pub schema: Schema,
    pub fields: WorksFields,
}

pub struct WorksFields {
    pub id: Field,
    pub key: Field,
    pub title: Field,
    pub title_ngram: Field,
    pub subtitle: Field,
    pub description: Field,
    pub subjects: Field,
    pub author_names: Field,
    pub author_names_ngram: Field,
    pub first_publish_year: Field,
    pub cover_id: Field,
}

impl WorksIndex {
    fn build_schema() -> (Schema, WorksFields) {
        let mut builder = Schema::builder();

        let id = builder.add_i64_field("id", STORED | INDEXED);
        let key = builder.add_text_field("key", STRING | STORED);
        let title = builder.add_text_field("title", TEXT | STORED);
        let title_ngram = builder.add_text_field("title_ngram", TEXT);
        let subtitle = builder.add_text_field("subtitle", TEXT | STORED);
        let description = builder.add_text_field("description", TEXT);
        let subjects = builder.add_text_field("subjects", TEXT | STORED);
        let author_names = builder.add_text_field("author_names", TEXT | STORED);
        let author_names_ngram = builder.add_text_field("author_names_ngram", TEXT);
        let first_publish_year = builder.add_i64_field("first_publish_year", INDEXED | STORED);
        let cover_id = builder.add_i64_field("cover_id", STORED);

        let fields = WorksFields {
            id,
            key,
            title,
            title_ngram,
            subtitle,
            description,
            subjects,
            author_names,
            author_names_ngram,
            first_publish_year,
            cover_id,
        };
        (builder.build(), fields)
    }

    pub fn open_or_create(path: &Path) -> anyhow::Result<Self> {
        std::fs::create_dir_all(path)?;
        let (schema, fields) = Self::build_schema();

        let index = if path.join("meta.json").exists() {
            Index::open_in_dir(path)?
        } else {
            Index::create_in_dir(path, schema.clone())?
        };

        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommitWithDelay)
            .try_into()?;

        Ok(Self {
            index,
            reader,
            schema,
            fields,
        })
    }

    pub fn writer(&self) -> anyhow::Result<IndexWriter> {
        Ok(self.index.writer(50_000_000)?)
    }

    pub fn doc_count(&self) -> u64 {
        self.reader.searcher().num_docs()
    }

    pub fn max_id(&self) -> anyhow::Result<Option<i32>> {
        let searcher = self.reader.searcher();
        let top_docs = searcher.search(
            &AllQuery,
            &TopDocs::with_limit(1).order_by_fast_field::<i64>("id", Order::Desc),
        )?;
        if let Some((_score, doc_address)) = top_docs.first() {
            let doc: TantivyDocument = searcher.doc(*doc_address)?;
            let id = doc
                .get_first(self.fields.id)
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            Ok(Some(id as i32))
        } else {
            Ok(None)
        }
    }

    /// Get a document by its ID, returning all stored fields
    pub fn get_by_id(&self, id: i32) -> anyhow::Result<Option<TantivyDocument>> {
        let searcher = self.reader.searcher();
        let term = Term::from_field_i64(self.fields.id, id as i64);
        let query = tantivy::query::TermQuery::new(term, tantivy::schema::IndexRecordOption::Basic);
        let top_docs = searcher.search(&query, &TopDocs::with_limit(1))?;
        if let Some((_score, doc_address)) = top_docs.first() {
            Ok(Some(searcher.doc(*doc_address)?))
        } else {
            Ok(None)
        }
    }

    pub fn search(&self, query: &str, limit: usize) -> anyhow::Result<Vec<WorkHit>> {
        let searcher = self.reader.searcher();
        let fields = vec![
            self.fields.title,
            self.fields.subtitle,
            self.fields.author_names,
            self.fields.subjects,
        ];
        let ngram_fields = vec![self.fields.title_ngram, self.fields.author_names_ngram];
        let query = build_fuzzy_query(query, &fields, &ngram_fields, &self.schema);
        let top_docs = searcher.search(&*query, &TopDocs::with_limit(limit))?;

        let mut results = Vec::with_capacity(top_docs.len());
        for (score, doc_address) in top_docs {
            let doc: TantivyDocument = searcher.doc(doc_address)?;
            let id = doc
                .get_first(self.fields.id)
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            results.push(WorkHit {
                id,
                slug: base36::encode(id),
                ol_key: doc
                    .get_first(self.fields.key)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                title: doc
                    .get_first(self.fields.title)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                subtitle: doc
                    .get_first(self.fields.subtitle)
                    .and_then(|v| v.as_str())
                    .map(String::from),
                author_names: doc
                    .get_first(self.fields.author_names)
                    .and_then(|v| v.as_str())
                    .map(String::from),
                first_publish_year: doc
                    .get_first(self.fields.first_publish_year)
                    .and_then(|v| v.as_i64()),
                cover_id: doc.get_first(self.fields.cover_id).and_then(|v| v.as_i64()),
                score,
            });
        }
        Ok(results)
    }
}

#[derive(Debug, serde::Serialize)]
pub struct WorkHit {
    pub id: i64,
    pub slug: String,
    pub ol_key: String,
    pub title: String,
    pub subtitle: Option<String>,
    pub author_names: Option<String>,
    pub first_publish_year: Option<i64>,
    pub cover_id: Option<i64>,
    pub score: f32,
}

// --- Authors Index ---

pub struct AuthorsIndex {
    pub index: Index,
    pub reader: IndexReader,
    pub schema: Schema,
    pub fields: AuthorsFields,
}

pub struct AuthorsFields {
    pub id: Field,
    pub key: Field,
    pub name: Field,
    pub name_ngram: Field,
    pub alternate_names: Field,
    pub bio: Field,
}

impl AuthorsIndex {
    fn build_schema() -> (Schema, AuthorsFields) {
        let mut builder = Schema::builder();

        let id = builder.add_i64_field("id", STORED | INDEXED);
        let key = builder.add_text_field("key", STRING | STORED);
        let name = builder.add_text_field("name", TEXT | STORED);
        let name_ngram = builder.add_text_field("name_ngram", TEXT);
        let alternate_names = builder.add_text_field("alternate_names", TEXT | STORED);
        let bio = builder.add_text_field("bio", TEXT);

        let fields = AuthorsFields {
            id,
            key,
            name,
            name_ngram,
            alternate_names,
            bio,
        };
        (builder.build(), fields)
    }

    pub fn open_or_create(path: &Path) -> anyhow::Result<Self> {
        std::fs::create_dir_all(path)?;
        let (schema, fields) = Self::build_schema();

        let index = if path.join("meta.json").exists() {
            Index::open_in_dir(path)?
        } else {
            Index::create_in_dir(path, schema.clone())?
        };

        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommitWithDelay)
            .try_into()?;

        Ok(Self {
            index,
            reader,
            schema,
            fields,
        })
    }

    pub fn writer(&self) -> anyhow::Result<IndexWriter> {
        Ok(self.index.writer(50_000_000)?)
    }

    pub fn doc_count(&self) -> u64 {
        self.reader.searcher().num_docs()
    }

    pub fn max_id(&self) -> anyhow::Result<Option<i32>> {
        let searcher = self.reader.searcher();
        let top_docs = searcher.search(
            &AllQuery,
            &TopDocs::with_limit(1).order_by_fast_field::<i64>("id", Order::Desc),
        )?;
        if let Some((_score, doc_address)) = top_docs.first() {
            let doc: TantivyDocument = searcher.doc(*doc_address)?;
            let id = doc
                .get_first(self.fields.id)
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            Ok(Some(id as i32))
        } else {
            Ok(None)
        }
    }

    pub fn search(&self, query: &str, limit: usize) -> anyhow::Result<Vec<AuthorHit>> {
        let searcher = self.reader.searcher();
        let fields = vec![self.fields.name, self.fields.alternate_names];
        let ngram_fields = vec![self.fields.name_ngram];
        let query = build_fuzzy_query(query, &fields, &ngram_fields, &self.schema);
        let top_docs = searcher.search(&*query, &TopDocs::with_limit(limit))?;

        let mut results = Vec::with_capacity(top_docs.len());
        for (score, doc_address) in top_docs {
            let doc: TantivyDocument = searcher.doc(doc_address)?;
            let id = doc
                .get_first(self.fields.id)
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            results.push(AuthorHit {
                id,
                slug: base36::encode(id),
                ol_key: doc
                    .get_first(self.fields.key)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                name: doc
                    .get_first(self.fields.name)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                alternate_names: doc
                    .get_first(self.fields.alternate_names)
                    .and_then(|v| v.as_str())
                    .map(String::from),
                score,
            });
        }
        Ok(results)
    }
}

#[derive(Debug, serde::Serialize)]
pub struct AuthorHit {
    pub id: i64,
    pub slug: String,
    pub ol_key: String,
    pub name: String,
    pub alternate_names: Option<String>,
    pub score: f32,
}

// --- Editions Index ---

pub struct EditionsIndex {
    pub index: Index,
    pub reader: IndexReader,
    pub schema: Schema,
    pub fields: EditionsFields,
}

pub struct EditionsFields {
    pub id: Field,
    pub key: Field,
    pub work_id: Field,
    pub work_key: Field,
    pub title: Field,
    pub title_ngram: Field,
    pub subtitle: Field,
    pub isbns: Field,
    pub publishers: Field,
    pub publish_year: Field,
    pub cover_id: Field,
}

impl EditionsIndex {
    fn build_schema() -> (Schema, EditionsFields) {
        let mut builder = Schema::builder();

        let id = builder.add_i64_field("id", STORED | INDEXED);
        let key = builder.add_text_field("key", STRING | STORED);
        let work_id = builder.add_i64_field("work_id", STORED | INDEXED);
        let work_key = builder.add_text_field("work_key", STRING | STORED);
        let title = builder.add_text_field("title", TEXT | STORED);
        let title_ngram = builder.add_text_field("title_ngram", TEXT);
        let subtitle = builder.add_text_field("subtitle", TEXT | STORED);
        let isbns = builder.add_text_field("isbns", TEXT | STORED);
        let publishers = builder.add_text_field("publishers", TEXT | STORED);
        let publish_year = builder.add_i64_field("publish_year", INDEXED | STORED);
        let cover_id = builder.add_i64_field("cover_id", STORED);

        let fields = EditionsFields {
            id,
            key,
            work_id,
            work_key,
            title,
            title_ngram,
            subtitle,
            isbns,
            publishers,
            publish_year,
            cover_id,
        };
        (builder.build(), fields)
    }

    pub fn open_or_create(path: &Path) -> anyhow::Result<Self> {
        std::fs::create_dir_all(path)?;
        let (schema, fields) = Self::build_schema();

        let index = if path.join("meta.json").exists() {
            Index::open_in_dir(path)?
        } else {
            Index::create_in_dir(path, schema.clone())?
        };

        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommitWithDelay)
            .try_into()?;

        Ok(Self {
            index,
            reader,
            schema,
            fields,
        })
    }

    pub fn writer(&self) -> anyhow::Result<IndexWriter> {
        Ok(self.index.writer(50_000_000)?)
    }

    pub fn doc_count(&self) -> u64 {
        self.reader.searcher().num_docs()
    }

    pub fn max_id(&self) -> anyhow::Result<Option<i32>> {
        let searcher = self.reader.searcher();
        let top_docs = searcher.search(
            &AllQuery,
            &TopDocs::with_limit(1).order_by_fast_field::<i64>("id", Order::Desc),
        )?;
        if let Some((_score, doc_address)) = top_docs.first() {
            let doc: TantivyDocument = searcher.doc(*doc_address)?;
            let id = doc
                .get_first(self.fields.id)
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            Ok(Some(id as i32))
        } else {
            Ok(None)
        }
    }

    pub fn search(&self, query: &str, limit: usize) -> anyhow::Result<Vec<EditionHit>> {
        let searcher = self.reader.searcher();
        let fields = vec![self.fields.title, self.fields.isbns, self.fields.publishers];
        let ngram_fields = vec![self.fields.title_ngram];
        let query = build_fuzzy_query(query, &fields, &ngram_fields, &self.schema);
        let top_docs = searcher.search(&*query, &TopDocs::with_limit(limit))?;

        let mut results = Vec::with_capacity(top_docs.len());
        for (score, doc_address) in top_docs {
            let doc: TantivyDocument = searcher.doc(doc_address)?;
            let id = doc
                .get_first(self.fields.id)
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            let work_id = doc
                .get_first(self.fields.work_id)
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            results.push(EditionHit {
                id,
                slug: base36::encode(id),
                work_slug: base36::encode(work_id),
                ol_key: doc
                    .get_first(self.fields.key)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                title: doc
                    .get_first(self.fields.title)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                subtitle: doc
                    .get_first(self.fields.subtitle)
                    .and_then(|v| v.as_str())
                    .map(String::from),
                isbns: doc
                    .get_first(self.fields.isbns)
                    .and_then(|v| v.as_str())
                    .map(String::from),
                publishers: doc
                    .get_first(self.fields.publishers)
                    .and_then(|v| v.as_str())
                    .map(String::from),
                publish_year: doc
                    .get_first(self.fields.publish_year)
                    .and_then(|v| v.as_i64()),
                cover_id: doc.get_first(self.fields.cover_id).and_then(|v| v.as_i64()),
                score,
            });
        }
        Ok(results)
    }
}

#[derive(Debug, serde::Serialize)]
pub struct EditionHit {
    pub id: i64,
    pub slug: String,
    pub work_slug: String,
    pub ol_key: String,
    pub title: String,
    pub subtitle: Option<String>,
    pub isbns: Option<String>,
    pub publishers: Option<String>,
    pub publish_year: Option<i64>,
    pub cover_id: Option<i64>,
    pub score: f32,
}
