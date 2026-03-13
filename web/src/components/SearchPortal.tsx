import { useState, useEffect } from 'react';
import type { MouseEvent } from 'react';
import { useStore } from '@nanostores/react';
import { $searchQuery, setSearch } from '../stores/search';

const STORAGE_KEY = 'mylib_search';
const HISTORY_KEY = 'mylib_history';

export default function SearchPortal() {
  const [history, setHistory] = useState<string[]>([]);
  const currentQuery = useStore($searchQuery);

  useEffect(() => {
    try {
      const hist: string[] = JSON.parse(sessionStorage.getItem(HISTORY_KEY) || '[]');
      setHistory(hist);
    } catch {}
  }, []);

  function handleClick(e: MouseEvent<HTMLAnchorElement>, query: string): void {
    try {
      sessionStorage.setItem(STORAGE_KEY, JSON.stringify({ q: query, r: [], s: '' }));
    } catch {}

    if (window.location.pathname === '/') {
      e.preventDefault();
      setSearch(query);
    }
  }

  function clearHistory(): void {
    sessionStorage.removeItem(HISTORY_KEY);
    setHistory([]);
  }

  return (
    <div className="search-portal-container">
      {history.map((q, i) => (
        <a
          key={i}
          href="/"
          className={`search-portal ${q === currentQuery ? 'current' : ''}`}
          onClick={(e) => handleClick(e, q)}
        >
          {q}
        </a>
      ))}
      {history.length > 0 && (
        <button className="search-portal-clear" onClick={clearHistory} title="Clear history">
          &times;
        </button>
      )}
    </div>
  );
}
