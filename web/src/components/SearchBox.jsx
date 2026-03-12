import { useState, useRef, useEffect } from 'react';

const API_BASE = import.meta.env.PUBLIC_API_URL || 'http://localhost:3000';
const STORAGE_KEY = 'mylib_search';
const HISTORY_KEY = 'mylib_history';

function esc(str) {
  if (!str) return '';
  return str.replace(/[\uFE20\uFE21]/g, '');
}

function scoreResult(r, query) {
  const q = query.toLowerCase();
  const primary = (r.name || r.title || '').toLowerCase();

  // Exact match on primary field
  if (primary === q) return 100;

  // Primary field starts with query
  if (primary.startsWith(q)) return 80;

  // Query matches start of any word in primary field
  const words = primary.split(/\s+/);
  if (words.some(w => w.startsWith(q))) return 60;

  // All query words appear in primary field
  const qWords = q.split(/\s+/);
  if (qWords.every(qw => primary.includes(qw))) return 50;

  // Partial match
  if (primary.includes(q)) return 40;

  // Author match on works/editions
  if (r.author_names && r.author_names.toLowerCase().includes(q)) return 30;

  return 10;
}

export default function SearchBox() {
  const [query, setQuery] = useState(() => {
    try {
      const saved = sessionStorage.getItem(STORAGE_KEY);
      if (saved) return JSON.parse(saved).q || '';
    } catch (e) {}
    return '';
  });
  const [results, setResults] = useState(() => {
    try {
      const saved = sessionStorage.getItem(STORAGE_KEY);
      if (saved) return JSON.parse(saved).r || [];
    } catch (e) {}
    return [];
  });
  const [stats, setStats] = useState(() => {
    try {
      const saved = sessionStorage.getItem(STORAGE_KEY);
      if (saved) return JSON.parse(saved).s || '';
    } catch (e) {}
    return '';
  });
  const debounceRef = useRef(null);

  useEffect(() => {
    function handleSearchQuery(e) {
      const q = e.detail;
      setQuery(q);
      search(q);
    }
    function handleClear() {
      setQuery('');
      setResults([]);
      setStats('');
      saveState('', [], '');
    }
    window.addEventListener('searchquery', handleSearchQuery);
    window.addEventListener('searchclear', handleClear);
    return () => {
      window.removeEventListener('searchquery', handleSearchQuery);
      window.removeEventListener('searchclear', handleClear);
    };
  }, []);

  // Search on mount if query exists but results are empty (from history click)
  useEffect(() => {
    if (query && results.length === 0) {
      search(query);
    }
  }, []);

  function saveState(q, r, s) {
    try {
      sessionStorage.setItem(STORAGE_KEY, JSON.stringify({ q, r, s }));
    } catch (e) {}
  }

  function saveToHistory(q) {
    if (!q.trim()) return;
    try {
      const history = JSON.parse(sessionStorage.getItem(HISTORY_KEY) || '[]');
      const filtered = history.filter(h => h !== q);
      filtered.unshift(q);
      sessionStorage.setItem(HISTORY_KEY, JSON.stringify(filtered.slice(0, 20)));
    } catch (e) {}
  }

  async function search(q) {
    if (!q.trim()) {
      setResults([]);
      setStats('');
      saveState('', [], '');
      return;
    }

    const start = performance.now();
    try {
      const res = await fetch(`${API_BASE}/search?q=${encodeURIComponent(q)}&limit=10`);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      const elapsed = (performance.now() - start).toFixed(0);

      const combined = [
        ...data.works.map(w => ({ ...w, _type: 'work' })),
        ...data.authors.map(a => ({ ...a, _type: 'author' })),
        ...data.editions.map(e => ({ ...e, _type: 'edition' })),
      ].map(r => ({ ...r, _score: scoreResult(r, q) }))
       .sort((a, b) => b._score - a._score);

      const statsText = `${combined.length} results in ${elapsed}ms`;
      setStats(statsText);
      setResults(combined);
      saveState(q, combined, statsText);
    } catch (err) {
      setStats(`Error: ${err.message}`);
    }
  }

  function handleInput(e) {
    const value = e.target.value;
    setQuery(value);
    clearTimeout(debounceRef.current);
    debounceRef.current = setTimeout(() => search(value), 80);
  }

  return (
    <div className="search-container">
      <input
        type="text"
        value={query}
        onChange={handleInput}
        placeholder="Search books, authors, ISBNs..."
        autoFocus
      />

      <div className="stats">{stats}</div>

      <div className="results">
        {results.length === 0 && query && <div className="empty">No results</div>}
        {results.map((r, i) => (
          <a href={`/${r._type === 'edition' ? 'works' : r._type + 's'}/${r._type === 'edition' ? r.work_slug : r.slug}`} key={`${r._type}-${r.id}`} className="result" onClick={() => saveToHistory(query)}>
            <span className={`tag tag-${r._type}`}>{r._type}</span>
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
        .search-container input {
          width: 100%;
          padding: 14px 16px;
          font-size: 18px;
          border: 1px solid #000;
          background: #fffef9;
          outline: none;
          font-family: inherit;
        }
        .search-container input:focus { background: #fff; }
        .stats {
          font-size: 13px;
          color: #5a5549;
          margin: 12px 0;
          min-height: 1.2em;
        }
        .results {
          border: 1px solid #000;
          background: #faf6ed;
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
          padding: 3px 6px;
          flex-shrink: 0;
          margin-top: 2px;
        }
        .tag-work { background: #e8e4d9; color: #5a5549; }
        .tag-author { background: #d9e8d9; color: #3a5a3a; }
        .tag-edition { background: #d9e0e8; color: #3a4a5a; }
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
