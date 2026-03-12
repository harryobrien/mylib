import { useState, useEffect } from 'react';
import type { MouseEvent } from 'react';

const STORAGE_KEY = 'mylib_search';
const HISTORY_KEY = 'mylib_history';

interface SavedState {
  q: string;
  r: unknown[];
  s: string;
}

export default function SearchPortal() {
  const [history, setHistory] = useState<string[]>([]);
  const [currentQuery, setCurrentQuery] = useState<string>('');

  useEffect(() => {
    try {
      const saved = sessionStorage.getItem(STORAGE_KEY);
      if (saved) {
        const { q } = JSON.parse(saved) as SavedState;
        if (q) setCurrentQuery(q);
      }
      const hist: string[] = JSON.parse(sessionStorage.getItem(HISTORY_KEY) || '[]');
      setHistory(hist);
    } catch {}

    function handleSearchQuery(e: CustomEvent<string>): void {
      setCurrentQuery(e.detail);
    }
    window.addEventListener('searchquery', handleSearchQuery as EventListener);
    return () => window.removeEventListener('searchquery', handleSearchQuery as EventListener);
  }, []);

  function handleClick(e: MouseEvent<HTMLAnchorElement>, query: string): void {
    try {
      sessionStorage.setItem(STORAGE_KEY, JSON.stringify({ q: query, r: [], s: '' }));
    } catch {}

    if (window.location.pathname === '/') {
      e.preventDefault();
      window.dispatchEvent(new CustomEvent('searchquery', { detail: query }));
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
