import { useState, useRef } from 'react';
import useSWR, { mutate } from 'swr';
import { useStore } from '@nanostores/react';
import { $user } from '../stores/user';
import { $userEditions } from '../stores/search';
import { fetchUserEditions, updateProgress } from '../lib/fetchers';

interface Props {
  slug: string;
}

function formatDate(iso: string): string {
  return new Date(iso + 'T00:00:00').toLocaleDateString(undefined, {
    year: 'numeric',
    month: 'short',
    day: 'numeric',
  });
}

export default function ReadingProgress({ slug }: Props) {
  const user = useStore($user);
  const { data: editions } = useSWR(
    user ? 'userEditions' : null,
    fetchUserEditions,
    {
      onSuccess: (data) => $userEditions.set(data),
      revalidateOnFocus: false,
    }
  );

  const edition = editions?.find(e => e.slug === slug);
  const [editingPage, setEditingPage] = useState(false);
  const [pageValue, setPageValue] = useState('');
  const [editingStart, setEditingStart] = useState(false);
  const [editingFinish, setEditingFinish] = useState(false);
  const pageInputRef = useRef<HTMLInputElement>(null);

  if (!user || !edition) return null;

  const { status, started_at, finished_at, current_page, number_of_pages } = edition;
  const progress = (current_page && number_of_pages && number_of_pages > 0)
    ? Math.min(100, Math.round((current_page / number_of_pages) * 100))
    : null;

  async function savePage() {
    const page = parseInt(pageValue);
    if (!isNaN(page) && page >= 0) {
      await updateProgress(slug, { current_page: page });
      mutate('userEditions');
    }
    setEditingPage(false);
  }

  async function saveDate(field: 'started_at' | 'finished_at', value: string) {
    if (value) {
      await updateProgress(slug, { [field]: value });
      mutate('userEditions');
    }
    setEditingStart(false);
    setEditingFinish(false);
  }

  return (
    <div className="reading-progress">
      {number_of_pages && (
        <div className="progress-track">
          <div className="progress-bar" style={{ width: `${progress || 0}%` }} />
          <span className="progress-label">
            {editingPage ? (
              <input
                ref={pageInputRef}
                type="number"
                className="progress-page-input"
                value={pageValue}
                onChange={e => setPageValue(e.target.value)}
                onBlur={savePage}
                onKeyDown={e => { if (e.key === 'Enter') savePage(); if (e.key === 'Escape') setEditingPage(false); }}
                min={0}
                max={number_of_pages}
                autoFocus
              />
            ) : (
              <button
                type="button"
                className="progress-page-btn"
                onClick={() => { setPageValue(String(current_page || 0)); setEditingPage(true); }}
              >
                {current_page || 0}
              </button>
            )}
            <span className="progress-separator">/</span>
            <span>{number_of_pages} pages</span>
            {progress !== null && <span className="progress-pct">{progress}%</span>}
          </span>
        </div>
      )}

      <div className="reading-dates">
        {(status === 'reading' || status === 'finished' || started_at) && (
          <span className="reading-date">
            Started{' '}
            {editingStart ? (
              <input
                type="date"
                className="date-input"
                defaultValue={started_at || ''}
                onBlur={e => saveDate('started_at', e.target.value)}
                onKeyDown={e => { if (e.key === 'Enter') saveDate('started_at', (e.target as HTMLInputElement).value); if (e.key === 'Escape') setEditingStart(false); }}
                autoFocus
              />
            ) : started_at ? (
              <button type="button" className="date-btn" onClick={() => setEditingStart(true)}>
                {formatDate(started_at)}
              </button>
            ) : (
              <button type="button" className="date-btn date-btn-empty" onClick={() => setEditingStart(true)}>
                set date
              </button>
            )}
          </span>
        )}
        {(status === 'finished' || finished_at) && (
          <span className="reading-date">
            Finished{' '}
            {editingFinish ? (
              <input
                type="date"
                className="date-input"
                defaultValue={finished_at || ''}
                onBlur={e => saveDate('finished_at', e.target.value)}
                onKeyDown={e => { if (e.key === 'Enter') saveDate('finished_at', (e.target as HTMLInputElement).value); if (e.key === 'Escape') setEditingFinish(false); }}
                autoFocus
              />
            ) : finished_at ? (
              <button type="button" className="date-btn" onClick={() => setEditingFinish(true)}>
                {formatDate(finished_at)}
              </button>
            ) : (
              <button type="button" className="date-btn date-btn-empty" onClick={() => setEditingFinish(true)}>
                set date
              </button>
            )}
          </span>
        )}
      </div>
    </div>
  );
}
