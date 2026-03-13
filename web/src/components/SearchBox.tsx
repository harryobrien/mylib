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

type ResultType = 'work' | 'author' | 'edition';

interface TaggedResult {
  _type: ResultType;
  _score: number;
  id: number;
  slug: string;
  work_slug?: string;
  title?: string;
  name?: string;
  subtitle?: string;
  author_names?: string;
  alternate_names?: string;
  publishers?: string;
  publish_year?: number;
  isbns?: string;
  cover_id?: number;
}

interface SavedState {
  q: string;
  r: TaggedResult[];
  s: string;
}

function esc(str: string | undefined): string {
  if (!str) return '';
  return str.replace(/[\uFE20\uFE21]/g, '');
}

function scoreResult(r: TaggedResult, query: string): number {
  const q = query.toLowerCase();
  const primary = (r.name || r.title || '').toLowerCase();

  if (primary === q) return 100;
  if (primary.startsWith(q)) return 80;

  const words = primary.split(/\s+/);
  if (words.some(w => w.startsWith(q))) return 60;

  const qWords = q.split(/\s+/);
  if (qWords.every(qw => primary.includes(qw))) return 50;

  if (primary.includes(q)) return 40;

  if (r.author_names && r.author_names.toLowerCase().includes(q)) return 30;

  return 10;
}

export default function SearchBox() {
  const [query, setQuery] = useState<string>(() => {
    try {
      const saved = sessionStorage.getItem(STORAGE_KEY);
      if (saved) return (JSON.parse(saved) as SavedState).q || '';
    } catch {}
    return '';
  });

  const [results, setResults] = useState<TaggedResult[]>(() => {
    try {
      const saved = sessionStorage.getItem(STORAGE_KEY);
      if (saved) return (JSON.parse(saved) as SavedState).r || [];
    } catch {}
    return [];
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
        setResults([]);
        setStats('');
        saveState('', [], '');
      }
    }
  }, [trigger]);

  useEffect(() => {
    if (query && results.length === 0) {
      search(query);
    }
  }, []);

  function saveState(q: string, r: TaggedResult[], s: string): void {
    try {
      sessionStorage.setItem(STORAGE_KEY, JSON.stringify({ q, r, s }));
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
      setResults([]);
      setStats('');
      saveState('', [], '');
      return;
    }

    const version = ++searchVersionRef.current;
    const start = performance.now();
    try {
      const res = await fetch(`${API_BASE}/search?q=${encodeURIComponent(q)}&limit=10`);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data: SearchResponse = await res.json();

      // Ignore if a newer search has already been displayed
      if (version < displayedVersionRef.current) return;
      displayedVersionRef.current = version;

      const elapsed = (performance.now() - start).toFixed(0);
      const combined: TaggedResult[] = [
        ...data.works.map(w => ({ ...w, _type: 'work' as const, _score: 0 })),
        ...data.authors.map(a => ({ ...a, title: a.name, _type: 'author' as const, _score: 0 })),
        ...data.editions.map(e => ({ ...e, _type: 'edition' as const, _score: 0 })),
      ].map(r => ({ ...r, _score: scoreResult(r, q) }))
       .sort((a, b) => b._score - a._score);

      const statsText = `${combined.length} results in ${elapsed}ms`;
      setStats(statsText);
      setResults(combined);
      saveState(q, combined, statsText);
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
    setResults([]);
    setStats('');
    saveState('', [], '');
  }

  function handleKeyDown(e: React.KeyboardEvent<HTMLInputElement>): void {
    if (e.key === 'Escape') {
      handleClear();
    }
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

      {(query || results.length > 0) && <div className="stats">{stats}</div>}

      <div className="results">
        {results.length === 0 && query && <div className="empty">No results</div>}
        {results.map((r) => (
          <a
            href={`/${r._type === 'edition' ? 'works' : r._type + 's'}/${r._type === 'edition' ? r.work_slug : r.slug}`}
            key={`${r._type}-${r.id}`}
            className="result"
            onClick={() => saveToHistory(query)}
          >
            <span className={`tag tag-${r._type}`}>{r._type}</span>
            {r._type !== 'author' && (
              <div className="result-cover">
                {r.cover_id ? (
                  <img
                    src={`https://covers.openlibrary.org/b/id/${r.cover_id}-S.jpg`}
                    alt=""
                    width={33}
                    height={50}
                    loading="lazy"
                  />
                ) : (
                  <div className="cover-placeholder" />
                )}
              </div>
            )}
            <div className="result-content">
              <div className="result-title">{esc(r.title || r.name)}</div>
              {r.subtitle && <div className="result-subtitle">{esc(r.subtitle)}</div>}
              {r._type === 'work' && r.author_names && (
                <div className="result-authors">{esc(r.author_names)}</div>
              )}
              {r._type === 'edition' && (
                <div className="result-meta">
                  {r.publishers && <span>{esc(r.publishers)}</span>}
                  {r.publish_year && <span> · {r.publish_year}</span>}
                  {r.isbns && <span> · {esc(r.isbns.split(' ')[0])}</span>}
                </div>
              )}
              {r._type === 'author' && r.alternate_names && (
                <div className="result-meta">{esc(r.alternate_names)}</div>
              )}
            </div>
          </a>
        ))}
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
        .empty {
          color: #8a8477;
          text-align: center;
          padding: 40px;
          font-size: 13px;
          background: #faf6ed;
        }
      `}</style>
    </div>
  );
}
