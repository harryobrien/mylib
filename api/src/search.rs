use std::path::Path;
use tantivy::{
    collector::TopDocs,
    query::{BooleanQuery, FuzzyTermQuery, Occur, Query, RegexQuery},
    schema::*,
    Index, IndexReader, IndexWriter, ReloadPolicy,
    Term,
};

/// Build a search query: prefix match + fuzzy fallback
fn build_fuzzy_query(query: &str, fields: &[Field], _schema: &Schema) -> Box<dyn Query> {
    let query_lower = query.to_lowercase();
    let terms: Vec<&str> = query_lower.split_whitespace().collect();

    if terms.is_empty() {
        return Box::new(tantivy::query::EmptyQuery);
    }

    let term_queries: Vec<(Occur, Box<dyn Query>)> = terms.iter().map(|term| {
        let field_queries: Vec<(Occur, Box<dyn Query>)> = fields.iter().flat_map(|field| {
            let mut queries: Vec<(Occur, Box<dyn Query>)> = Vec::new();

            // Prefix regex query (higher implicit score for exact prefix)
            let pattern = format!("{}.*", regex_escape(term));
            if let Ok(regex_q) = RegexQuery::from_pattern(&pattern, *field) {
                queries.push((Occur::Should, Box::new(regex_q)));
            }

            // Fuzzy query for typo tolerance
            let tantivy_term = Term::from_field_text(*field, term);
            let fuzzy: Box<dyn Query> = Box::new(
                FuzzyTermQuery::new(tantivy_term, 1, true)
            );
            queries.push((Occur::Should, fuzzy));

            queries
        }).collect();

        let field_bool: Box<dyn Query> = Box::new(BooleanQuery::new(field_queries));
        (Occur::Must, field_bool)
    }).collect();

    Box::new(BooleanQuery::new(term_queries))
}

fn regex_escape(s: &str) -> String {
    let mut result = String::with_capacity(s.len() * 2);
    for c in s.chars() {
        match c {
            '\\' | '.' | '+' | '*' | '?' | '(' | ')' | '|' | '[' | ']' | '{' | '}' | '^' | '$' => {
                result.push('\\');
                result.push(c);
            }
            _ => result.push(c),
        }
    }
    result
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
    pub subtitle: Field,
    pub description: Field,
    pub subjects: Field,
    pub author_names: Field,
    pub first_publish_year: Field,
}

impl WorksIndex {
    fn build_schema() -> (Schema, WorksFields) {
        let mut builder = Schema::builder();

        let id = builder.add_i64_field("id", STORED | INDEXED);
        let key = builder.add_text_field("key", STRING | STORED);
        let title = builder.add_text_field("title", TEXT | STORED);
        let subtitle = builder.add_text_field("subtitle", TEXT | STORED);
        let description = builder.add_text_field("description", TEXT);
        let subjects = builder.add_text_field("subjects", TEXT | STORED);
        let author_names = builder.add_text_field("author_names", TEXT | STORED);
        let first_publish_year = builder.add_i64_field("first_publish_year", INDEXED | STORED);

        let fields = WorksFields {
            id, key, title, subtitle, description, subjects, author_names, first_publish_year,
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

        let reader = index.reader_builder()
            .reload_policy(ReloadPolicy::OnCommitWithDelay)
            .try_into()?;

        Ok(Self { index, reader, schema, fields })
    }

    pub fn writer(&self) -> anyhow::Result<IndexWriter> {
        Ok(self.index.writer(50_000_000)?)
    }

    pub fn search(&self, query: &str, limit: usize) -> anyhow::Result<Vec<WorkHit>> {
        let searcher = self.reader.searcher();
        let fields = vec![
            self.fields.title, self.fields.subtitle,
            self.fields.author_names, self.fields.subjects
        ];
        let query = build_fuzzy_query(query, &fields, &self.schema);
        let top_docs = searcher.search(&*query, &TopDocs::with_limit(limit))?;

        let mut results = Vec::with_capacity(top_docs.len());
        for (score, doc_address) in top_docs {
            let doc: TantivyDocument = searcher.doc(doc_address)?;
            results.push(WorkHit {
                id: doc.get_first(self.fields.id).and_then(|v| v.as_i64()).unwrap_or(0),
                key: doc.get_first(self.fields.key).and_then(|v| v.as_str()).unwrap_or("").to_string(),
                title: doc.get_first(self.fields.title).and_then(|v| v.as_str()).unwrap_or("").to_string(),
                subtitle: doc.get_first(self.fields.subtitle).and_then(|v| v.as_str()).map(String::from),
                author_names: doc.get_first(self.fields.author_names).and_then(|v| v.as_str()).map(String::from),
                first_publish_year: doc.get_first(self.fields.first_publish_year).and_then(|v| v.as_i64()),
                score,
            });
        }
        Ok(results)
    }
}

#[derive(Debug, serde::Serialize)]
pub struct WorkHit {
    pub id: i64,
    pub key: String,
    pub title: String,
    pub subtitle: Option<String>,
    pub author_names: Option<String>,
    pub first_publish_year: Option<i64>,
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
    pub alternate_names: Field,
    pub bio: Field,
}

impl AuthorsIndex {
    fn build_schema() -> (Schema, AuthorsFields) {
        let mut builder = Schema::builder();

        let id = builder.add_i64_field("id", STORED | INDEXED);
        let key = builder.add_text_field("key", STRING | STORED);
        let name = builder.add_text_field("name", TEXT | STORED);
        let alternate_names = builder.add_text_field("alternate_names", TEXT | STORED);
        let bio = builder.add_text_field("bio", TEXT);

        let fields = AuthorsFields { id, key, name, alternate_names, bio };
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

        let reader = index.reader_builder()
            .reload_policy(ReloadPolicy::OnCommitWithDelay)
            .try_into()?;

        Ok(Self { index, reader, schema, fields })
    }

    pub fn writer(&self) -> anyhow::Result<IndexWriter> {
        Ok(self.index.writer(50_000_000)?)
    }

    pub fn search(&self, query: &str, limit: usize) -> anyhow::Result<Vec<AuthorHit>> {
        let searcher = self.reader.searcher();
        let fields = vec![self.fields.name, self.fields.alternate_names];
        let query = build_fuzzy_query(query, &fields, &self.schema);
        let top_docs = searcher.search(&*query, &TopDocs::with_limit(limit))?;

        let mut results = Vec::with_capacity(top_docs.len());
        for (score, doc_address) in top_docs {
            let doc: TantivyDocument = searcher.doc(doc_address)?;
            results.push(AuthorHit {
                id: doc.get_first(self.fields.id).and_then(|v| v.as_i64()).unwrap_or(0),
                key: doc.get_first(self.fields.key).and_then(|v| v.as_str()).unwrap_or("").to_string(),
                name: doc.get_first(self.fields.name).and_then(|v| v.as_str()).unwrap_or("").to_string(),
                alternate_names: doc.get_first(self.fields.alternate_names).and_then(|v| v.as_str()).map(String::from),
                score,
            });
        }
        Ok(results)
    }
}

#[derive(Debug, serde::Serialize)]
pub struct AuthorHit {
    pub id: i64,
    pub key: String,
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
    pub work_key: Field,
    pub title: Field,
    pub subtitle: Field,
    pub isbns: Field,
    pub publishers: Field,
    pub publish_year: Field,
}

impl EditionsIndex {
    fn build_schema() -> (Schema, EditionsFields) {
        let mut builder = Schema::builder();

        let id = builder.add_i64_field("id", STORED | INDEXED);
        let key = builder.add_text_field("key", STRING | STORED);
        let work_key = builder.add_text_field("work_key", STRING | STORED);
        let title = builder.add_text_field("title", TEXT | STORED);
        let subtitle = builder.add_text_field("subtitle", TEXT | STORED);
        let isbns = builder.add_text_field("isbns", TEXT | STORED);
        let publishers = builder.add_text_field("publishers", TEXT | STORED);
        let publish_year = builder.add_i64_field("publish_year", INDEXED | STORED);

        let fields = EditionsFields {
            id, key, work_key, title, subtitle, isbns, publishers, publish_year,
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

        let reader = index.reader_builder()
            .reload_policy(ReloadPolicy::OnCommitWithDelay)
            .try_into()?;

        Ok(Self { index, reader, schema, fields })
    }

    pub fn writer(&self) -> anyhow::Result<IndexWriter> {
        Ok(self.index.writer(50_000_000)?)
    }

    pub fn search(&self, query: &str, limit: usize) -> anyhow::Result<Vec<EditionHit>> {
        let searcher = self.reader.searcher();
        let fields = vec![self.fields.title, self.fields.isbns, self.fields.publishers];
        let query = build_fuzzy_query(query, &fields, &self.schema);
        let top_docs = searcher.search(&*query, &TopDocs::with_limit(limit))?;

        let mut results = Vec::with_capacity(top_docs.len());
        for (score, doc_address) in top_docs {
            let doc: TantivyDocument = searcher.doc(doc_address)?;
            results.push(EditionHit {
                id: doc.get_first(self.fields.id).and_then(|v| v.as_i64()).unwrap_or(0),
                key: doc.get_first(self.fields.key).and_then(|v| v.as_str()).unwrap_or("").to_string(),
                work_key: doc.get_first(self.fields.work_key).and_then(|v| v.as_str()).unwrap_or("").to_string(),
                title: doc.get_first(self.fields.title).and_then(|v| v.as_str()).unwrap_or("").to_string(),
                subtitle: doc.get_first(self.fields.subtitle).and_then(|v| v.as_str()).map(String::from),
                isbns: doc.get_first(self.fields.isbns).and_then(|v| v.as_str()).map(String::from),
                publishers: doc.get_first(self.fields.publishers).and_then(|v| v.as_str()).map(String::from),
                publish_year: doc.get_first(self.fields.publish_year).and_then(|v| v.as_i64()),
                score,
            });
        }
        Ok(results)
    }
}

#[derive(Debug, serde::Serialize)]
pub struct EditionHit {
    pub id: i64,
    pub key: String,
    pub work_key: String,
    pub title: String,
    pub subtitle: Option<String>,
    pub isbns: Option<String>,
    pub publishers: Option<String>,
    pub publish_year: Option<i64>,
    pub score: f32,
}
