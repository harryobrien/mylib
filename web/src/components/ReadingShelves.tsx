import { useState, useEffect } from 'react';
import { useStore } from '@nanostores/react';
import { $searchQuery } from '../stores/search';

const API_BASE = import.meta.env.PUBLIC_API_URL || 'http://localhost:3000';

interface Edition {
  slug: string;
  work_slug: string;
  title: string;
  status: string;
  cover_id: number | null;
}

export default function ReadingShelves() {
  const [editions, setEditions] = useState<Edition[]>([]);
  const [isLoggedIn, setIsLoggedIn] = useState(false);
  const [loading, setLoading] = useState(true);
  const searchQuery = useStore($searchQuery);

  useEffect(() => {
    loadEditions();
  }, []);

  async function loadEditions() {
    try {
      const res = await fetch(`${API_BASE}/auth/editions`, { credentials: 'include' });
      if (res.ok) {
        setIsLoggedIn(true);
        const data = await res.json();
        setEditions(data.editions || []);
      }
    } catch {
      // Not logged in
    } finally {
      setLoading(false);
    }
  }

  if (loading || !isLoggedIn || editions.length === 0 || searchQuery) {
    return null;
  }

  const reading = editions.filter(e => e.status === 'reading');
  const wantToRead = editions.filter(e => e.status === 'want_to_read');
  const finished = editions.filter(e => e.status === 'finished');

  return (
    <div className="shelves">
      {reading.length > 0 && <Shelf title="Reading" editions={reading} />}
      {wantToRead.length > 0 && <Shelf title="Want to Read" editions={wantToRead} />}
      {finished.length > 0 && <Shelf title="Finished" editions={finished} />}
    </div>
  );
}

function Shelf({ title, editions }: { title: string; editions: Edition[] }) {
  return (
    <div className="shelf">
      <div className="shelf-title">{title}</div>
      <div className="shelf-books">
        {editions.map(e => (
          <a key={e.slug} href={`/works/${e.work_slug}`} className="shelf-book">
            {e.cover_id ? (
              <img
                src={`https://covers.openlibrary.org/b/id/${e.cover_id}-M.jpg`}
                alt=""
                className="shelf-cover"
              />
            ) : (
              <div className="shelf-cover-placeholder" />
            )}
            <div className="shelf-book-title">{e.title}</div>
          </a>
        ))}
      </div>
    </div>
  );
}
