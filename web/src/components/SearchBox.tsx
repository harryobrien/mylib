import { useState, useRef, useEffect } from 'react';
import type { ChangeEvent } from 'react';
import { useStore } from '@nanostores/react';
import { $searchQuery, $triggerSearch } from '../stores/search';

const API_BASE = import.meta.env.PUBLIC_API_URL || 'http://localhost:3000';
const STORAGE_KEY = 'mylib_search';
const HISTORY_KEY = 'mylib_history';

interface WorkHit {
  id: number;
  slug: string;
  ol_key: string;
  title: string;
  subtitle?: string;
  author_names?: string;
  first_publish_year?: number;
  cover_id?: number;
  ratings_count?: number;
  rating_avg?: number;
  score: number;
}

interface AuthorHit {
  id: number;
  slug: string;
  ol_key: string;
  name: string;
  alternate_names?: string;
  score: number;
}

interface EditionHit {
  id: number;
  slug: string;
  work_slug: string;
  ol_key: string;
  title: string;
  subtitle?: string;
  isbns?: string;
  publishers?: string;
  publish_year?: number;
  cover_id?: number;
  score: number;
}

interface SearchResponse {
  query: string;
  works: WorkHit[];
  authors: AuthorHit[];
  editions: EditionHit[];
}

interface GroupedResults {
  featuredAuthor?: AuthorHit;
  worksByAuthor: WorkHit[];
  otherWorks: WorkHit[];
  otherAuthors: AuthorHit[];
  editions: EditionHit[];
}

interface SavedState {
  q: string;
  g: GroupedResults;
  s: string;
}

function esc(str: string | undefined): string {
  if (!str) return '';
  return str.replace(/[\uFE20\uFE21]/g, '');
}

function normalizeForMatch(s: string): string {
  return s.toLowerCase().replace(/[^a-z0-9]/g, '');
}

function groupResults(data: SearchResponse, query: string): GroupedResults {
  const q = query.toLowerCase().trim();
  const qNorm = normalizeForMatch(query);

  let featuredAuthor: AuthorHit | undefined;
  for (const author of data.authors) {
    const nameNorm = normalizeForMatch(author.name);
    if (nameNorm === qNorm || author.name.toLowerCase() === q) {
      featuredAuthor = author;
      break;
    }
  }

  if (!featuredAuthor && data.authors.length > 0) {
    const topAuthor = data.authors[0];
    const nameWords = topAuthor.name.toLowerCase().split(/\s+/);
    const queryWords = q.split(/\s+/);
    if (queryWords.length >= 2 && queryWords.every(qw => nameWords.some(nw => nw.startsWith(qw)))) {
      featuredAuthor = topAuthor;
    }
  }

  let worksByAuthor: WorkHit[] = [];
  let otherWorks: WorkHit[] = [];

  if (featuredAuthor) {
    const authorNameLower = featuredAuthor.name.toLowerCase();
    for (const work of data.works) {
      if (work.author_names?.toLowerCase().includes(authorNameLower)) {
        worksByAuthor.push(work);
      } else {
        otherWorks.push(work);
      }
    }
    worksByAuthor.sort((a, b) => b.score - a.score);
    otherWorks.sort((a, b) => b.score - a.score);
  } else {
    otherWorks = [...data.works].sort((a, b) => b.score - a.score);
  }

  const otherAuthors = featuredAuthor
    ? data.authors.filter(a => a.id !== featuredAuthor!.id)
    : data.authors;

  return {
    featuredAuthor,
    worksByAuthor,
    otherWorks,
    otherAuthors,
    editions: data.editions,
  };
}

const emptyGrouped: GroupedResults = {
  featuredAuthor: undefined,
  worksByAuthor: [],
  otherWorks: [],
  otherAuthors: [],
  editions: [],
};

export default function SearchBox() {
  const [query, setQuery] = useState<string>(() => {
    try {
      const saved = sessionStorage.getItem(STORAGE_KEY);
      if (saved) return (JSON.parse(saved) as SavedState).q || '';
    } catch {}
    return '';
  });

  const [grouped, setGrouped] = useState<GroupedResults>(() => {
    try {
      const saved = sessionStorage.getItem(STORAGE_KEY);
      if (saved) return (JSON.parse(saved) as SavedState).g || emptyGrouped;
    } catch {}
    return emptyGrouped;
  });

  const [stats, setStats] = useState<string>(() => {
    try {
      const saved = sessionStorage.getItem(STORAGE_KEY);
      if (saved) return (JSON.parse(saved) as SavedState).s || '';
    } catch {}
    return '';
  });

  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const searchVersionRef = useRef(0);
  const displayedVersionRef = useRef(0);

  const storeQuery = useStore($searchQuery);
  const trigger = useStore($triggerSearch);

  useEffect(() => {
    if (trigger > 0) {
      setQuery(storeQuery);
      if (storeQuery) {
        search(storeQuery);
      } else {
        setGrouped(emptyGrouped);
        setStats('');
        saveState('', emptyGrouped, '');
      }
    }
  }, [trigger]);

  useEffect(() => {
    const hasResults = grouped.featuredAuthor || grouped.worksByAuthor.length > 0 ||
      grouped.otherWorks.length > 0 || grouped.otherAuthors.length > 0 || grouped.editions.length > 0;
    if (query && !hasResults) {
      search(query);
    }
  }, []);

  function saveState(q: string, g: GroupedResults, s: string): void {
    try {
      sessionStorage.setItem(STORAGE_KEY, JSON.stringify({ q, g, s }));
      $searchQuery.set(q);
    } catch {}
  }

  function saveToHistory(q: string): void {
    if (!q.trim()) return;
    try {
      const history: string[] = JSON.parse(sessionStorage.getItem(HISTORY_KEY) || '[]');
      const filtered = history.filter(h => h !== q);
      filtered.unshift(q);
      sessionStorage.setItem(HISTORY_KEY, JSON.stringify(filtered.slice(0, 20)));
    } catch {}
  }

  async function search(q: string): Promise<void> {
    if (!q.trim()) {
      searchVersionRef.current++;
      displayedVersionRef.current = searchVersionRef.current;
      setGrouped(emptyGrouped);
      setStats('');
      saveState('', emptyGrouped, '');
      return;
    }

    const version = ++searchVersionRef.current;
    const start = performance.now();
    try {
      const res = await fetch(`${API_BASE}/search?q=${encodeURIComponent(q)}&limit=15`);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data: SearchResponse = await res.json();

      if (version < displayedVersionRef.current) return;
      displayedVersionRef.current = version;

      const elapsed = (performance.now() - start).toFixed(0);
      const g = groupResults(data, q);
      const total = (g.featuredAuthor ? 1 : 0) + g.worksByAuthor.length +
        g.otherWorks.length + g.otherAuthors.length + g.editions.length;

      const statsText = `${total} results in ${elapsed}ms`;
      setStats(statsText);
      setGrouped(g);
      saveState(q, g, statsText);
    } catch (err) {
      setStats(`Error: ${(err as Error).message}`);
    }
  }

  function handleInput(e: ChangeEvent<HTMLInputElement>): void {
    const value = e.target.value;
    setQuery(value);
    $searchQuery.set(value);
    if (debounceRef.current) clearTimeout(debounceRef.current);
    debounceRef.current = setTimeout(() => search(value), 80);
  }

  function handleClear(): void {
    setQuery('');
    setGrouped(emptyGrouped);
    setStats('');
    saveState('', emptyGrouped, '');
  }

  function handleKeyDown(e: React.KeyboardEvent<HTMLInputElement>): void {
    if (e.key === 'Escape') {
      handleClear();
    }
  }

  const hasResults = grouped.featuredAuthor || grouped.worksByAuthor.length > 0 ||
    grouped.otherWorks.length > 0 || grouped.otherAuthors.length > 0 || grouped.editions.length > 0;

  function renderWork(w: WorkHit) {
    return (
      <a
        href={`/works/${w.slug}`}
        key={`work-${w.id}`}
        className="result"
        onClick={() => saveToHistory(query)}
      >
        <div className="result-cover">
          {w.cover_id ? (
            <img
              src={`https://covers.openlibrary.org/b/id/${w.cover_id}-S.jpg`}
              alt=""
              width={33}
              height={50}
              loading="lazy"
            />
          ) : (
            <div className="cover-placeholder" />
          )}
        </div>
        <div className="result-content">
          <div className="result-title">{esc(w.title)}</div>
          {w.subtitle && <div className="result-subtitle">{esc(w.subtitle)}</div>}
          {w.author_names && <div className="result-authors">{esc(w.author_names)}</div>}
          {w.ratings_count && w.ratings_count > 0 && (
            <div className="result-rating">
              <span className="rating-stars">{w.rating_avg?.toFixed(1)}</span>
              <span className="rating-count">({w.ratings_count.toLocaleString()})</span>
            </div>
          )}
        </div>
      </a>
    );
  }

  function renderAuthor(a: AuthorHit) {
    return (
      <a
        href={`/authors/${a.slug}`}
        key={`author-${a.id}`}
        className="result"
        onClick={() => saveToHistory(query)}
      >
        <div className="result-content">
          <div className="result-title">{esc(a.name)}</div>
          {a.alternate_names && <div className="result-meta">{esc(a.alternate_names)}</div>}
        </div>
      </a>
    );
  }

  function renderEdition(e: EditionHit) {
    return (
      <a
        href={`/works/${e.work_slug}`}
        key={`edition-${e.id}`}
        className="result"
        onClick={() => saveToHistory(query)}
      >
        <div className="result-cover">
          {e.cover_id ? (
            <img
              src={`https://covers.openlibrary.org/b/id/${e.cover_id}-S.jpg`}
              alt=""
              width={33}
              height={50}
              loading="lazy"
            />
          ) : (
            <div className="cover-placeholder" />
          )}
        </div>
        <div className="result-content">
          <div className="result-title">{esc(e.title)}</div>
          {e.subtitle && <div className="result-subtitle">{esc(e.subtitle)}</div>}
          <div className="result-meta">
            {e.publishers && <span>{esc(e.publishers)}</span>}
            {e.publish_year && <span> · {e.publish_year}</span>}
            {e.isbns && <span> · {esc(e.isbns.split(' ')[0])}</span>}
          </div>
        </div>
      </a>
    );
  }

  return (
    <div className="search-container">
      <div className="search-input-wrapper">
        <input
          type="text"
          value={query}
          onChange={handleInput}
          onKeyDown={handleKeyDown}
          placeholder="Search books, authors, ISBNs..."
          autoFocus
        />
        {query && (
          <button className="search-clear" onClick={handleClear} type="button">
            &times;
          </button>
        )}
      </div>

      {(query || hasResults) && <div className="stats">{stats}</div>}

      <div className="results">
        {!hasResults && query && <div className="empty">No results</div>}

        {grouped.featuredAuthor && (
          <div className="result-group">
            <a
              href={`/authors/${grouped.featuredAuthor.slug}`}
              className="featured-author"
              onClick={() => saveToHistory(query)}
            >
              <div className="featured-author-name">{esc(grouped.featuredAuthor.name)}</div>
              {grouped.featuredAuthor.alternate_names && (
                <div className="featured-author-aka">
                  {esc(grouped.featuredAuthor.alternate_names)}
                </div>
              )}
              {grouped.worksByAuthor.length > 0 && (
                <div className="featured-author-works">
                  {grouped.worksByAuthor.slice(0, 3).map(w => w.title).join(' · ')}
                </div>
              )}
            </a>
          </div>
        )}

        {grouped.worksByAuthor.length > 0 && (
          <div className="result-group">
            <div className="result-group-header">
              Works by {grouped.featuredAuthor?.name}
            </div>
            {grouped.worksByAuthor.slice(0, 5).map(renderWork)}
          </div>
        )}

        {grouped.otherWorks.length > 0 && (
          <div className="result-group">
            <div className="result-group-header">
              {grouped.featuredAuthor ? 'Other works' : 'Works'}
            </div>
            {grouped.otherWorks.slice(0, 5).map(renderWork)}
          </div>
        )}

        {grouped.otherAuthors.length > 0 && (
          <div className="result-group">
            <div className="result-group-header">Authors</div>
            {grouped.otherAuthors.slice(0, 3).map(renderAuthor)}
          </div>
        )}

        {grouped.editions.length > 0 && (
          <div className="result-group">
            <div className="result-group-header">Editions</div>
            {grouped.editions.slice(0, 3).map(renderEdition)}
          </div>
        )}
      </div>

      <style>{`
        .search-input-wrapper {
          position: relative;
          display: flex;
        }
        .search-container input {
          width: 100%;
          padding: 14px 16px;
          padding-right: 40px;
          font-size: 18px;
          border: 1px solid #000;
          background: #fffef9;
          outline: none;
          font-family: inherit;
        }
        .search-container input:focus { background: #fff; }
        .search-clear {
          position: absolute;
          right: 8px;
          top: 50%;
          transform: translateY(-50%);
          background: none;
          border: none;
          font-size: 24px;
          color: #8a8477;
          cursor: pointer;
          padding: 4px 8px;
          line-height: 1;
        }
        .search-clear:hover { color: #5a5549; }
        .stats {
          font-size: 13px;
          color: #5a5549;
          margin: 12px 0;
          min-height: 1.2em;
        }
        .results {
          border: 1px solid #000;
          background: #faf6ed;
          max-height: calc(100vh - 220px);
          overflow-y: auto;
        }
        .results:empty {
          display: none;
        }
        .result {
          display: flex;
          gap: 12px;
          padding: 12px 15px;
          background: #faf6ed;
          border-bottom: 1px solid #d9d4c8;
          text-decoration: none;
          color: inherit;
          align-items: flex-start;
          animation: fadeIn 150ms ease;
        }
        @keyframes fadeIn {
          from { opacity: 0; transform: translateY(-4px); }
          to { opacity: 1; transform: translateY(0); }
        }
        .result:last-child { border-bottom: none; }
        .result:hover { background: #f0ebdf; }
        .tag {
          font-size: 10px;
          font-weight: 600;
          text-transform: uppercase;
          padding: 3px 0;
          flex-shrink: 0;
          margin-top: 2px;
          width: 52px;
          text-align: center;
        }
        .tag-work { background: #e8e4d9; color: #5a5549; }
        .tag-author { background: #d9e8d9; color: #3a5a3a; }
        .tag-edition { background: #d9e0e8; color: #3a4a5a; }
        .result-cover {
          width: 33px;
          height: 50px;
          flex-shrink: 0;
        }
        .result-cover img {
          width: 33px;
          height: 50px;
          object-fit: cover;
          background: #e8e4d9;
        }
        .cover-placeholder {
          width: 33px;
          height: 50px;
          background: #e8e4d9;
        }
        .result-content { flex: 1; min-width: 0; }
        .result-title {
          font-size: 15px;
          font-weight: 500;
          color: #1a1a1a;
        }
        .result-subtitle {
          font-size: 14px;
          font-style: italic;
          color: #5a5549;
          margin-top: 2px;
        }
        .result-authors {
          font-size: 13px;
          color: #5a5549;
          margin-top: 3px;
        }
        .result-meta {
          font-size: 12px;
          color: #8a8477;
          margin-top: 3px;
        }
        .result-rating {
          font-size: 12px;
          color: #8a8477;
          margin-top: 3px;
        }
        .rating-stars {
          color: #b8860b;
          font-weight: 500;
        }
        .rating-count {
          margin-left: 4px;
        }
        .empty {
          color: #8a8477;
          text-align: center;
          padding: 40px;
          font-size: 13px;
          background: #faf6ed;
        }
        .result-group {
          border-bottom: 1px solid #c9c4b8;
        }
        .result-group:last-child {
          border-bottom: none;
        }
        .result-group-header {
          font-size: 11px;
          font-weight: 600;
          text-transform: uppercase;
          color: #8a8477;
          padding: 10px 15px 6px;
          background: #f0ebdf;
          letter-spacing: 0.5px;
        }
        .featured-author {
          display: block;
          padding: 16px;
          background: linear-gradient(to right, #e8f0e8, #f0ebdf);
          text-decoration: none;
          color: inherit;
          border-bottom: 1px solid #c9c4b8;
        }
        .featured-author:hover {
          background: linear-gradient(to right, #dce8dc, #e8e3d7);
        }
        .featured-author-name {
          font-size: 18px;
          font-weight: 600;
          color: #1a1a1a;
        }
        .featured-author-aka {
          font-size: 12px;
          color: #5a5549;
          margin-top: 2px;
        }
        .featured-author-works {
          font-size: 13px;
          color: #3a5a3a;
          margin-top: 6px;
          font-style: italic;
        }
      `}</style>
    </div>
  );
}
