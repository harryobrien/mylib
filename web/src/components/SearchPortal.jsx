import { useState, useEffect } from 'react';

const STORAGE_KEY = 'mylib_search';
const HISTORY_KEY = 'mylib_history';

export default function SearchPortal() {
  const [history, setHistory] = useState([]);
  const [currentQuery, setCurrentQuery] = useState('');

  useEffect(() => {
    try {
      const saved = sessionStorage.getItem(STORAGE_KEY);
      if (saved) {
        const { q } = JSON.parse(saved);
        if (q) setCurrentQuery(q);
      }
      const hist = JSON.parse(sessionStorage.getItem(HISTORY_KEY) || '[]');
      setHistory(hist);
    } catch (e) {}

    // Listen for query changes
    function handleSearchQuery(e) {
      setCurrentQuery(e.detail);
    }
    window.addEventListener('searchquery', handleSearchQuery);
    return () => window.removeEventListener('searchquery', handleSearchQuery);
  }, []);

  function handleClick(e, query) {
    // Set this as the current search so it loads when returning
    try {
      sessionStorage.setItem(STORAGE_KEY, JSON.stringify({ q: query, r: [], s: '' }));
    } catch (e) {}

    // If we're on the search page, dispatch event instead of navigating
    if (window.location.pathname === '/') {
      e.preventDefault();
      window.dispatchEvent(new CustomEvent('searchquery', { detail: query }));
    }
  }

  function clearHistory() {
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
