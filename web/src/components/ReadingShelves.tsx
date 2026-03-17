import { useEffect } from 'react';
import { useStore } from '@nanostores/react';
import { $user } from '../stores/user';
import { $searchQuery, $hasSearchResults, $userEditions, $userEditionsLoading, loadUserEditions, type Edition } from '../stores/search';

const API_BASE = import.meta.env.PUBLIC_API_URL || 'http://localhost:3000';

export default function ReadingShelves() {
  const user = useStore($user);
  const editions = useStore($userEditions);
  const loading = useStore($userEditionsLoading);
  const searchQuery = useStore($searchQuery);
  const hasSearchResults = useStore($hasSearchResults);

  useEffect(() => {
    if (user) {
      loadUserEditions(API_BASE);
    }
  }, [user]);

  if (!user || loading || editions === null || editions.length === 0 || searchQuery || hasSearchResults) {
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
