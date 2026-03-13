import { useState, useEffect } from 'react';
import { useStore } from '@nanostores/react';
import { $userEditions, loadUserEditions, invalidateUserEditions } from '../stores/search';

const API_BASE = import.meta.env.PUBLIC_API_URL || 'http://localhost:3000';

interface Props {
  slug: string;
}

type Status = 'reading' | 'want_to_read' | 'finished' | 'did_not_finish' | null;

export default function ReadingStatus({ slug }: Props) {
  const editions = useStore($userEditions);
  const [status, setStatus] = useState<Status>(null);

  useEffect(() => {
    loadUserEditions(API_BASE);
  }, []);

  useEffect(() => {
    if (editions) {
      const edition = editions.find(e => e.slug === slug);
      setStatus(edition?.status as Status || null);
    }
  }, [editions, slug]);

  async function setEditionStatus(newStatus: Status) {
    if (!newStatus) {
      await fetch(`${API_BASE}/auth/editions/${slug}`, {
        method: 'DELETE',
        credentials: 'include',
      });
    } else {
      await fetch(`${API_BASE}/auth/editions/${slug}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({ status: newStatus }),
      });
    }
    setStatus(newStatus);
    invalidateUserEditions();
    loadUserEditions(API_BASE, true);
  }

  // Not logged in or still loading
  if (editions === null) {
    return null;
  }

  function btnClass(s: Status, isDnf = false) {
    if (status === s) return isDnf ? 'status-btn status-btn-dnf' : 'status-btn status-btn-active';
    return 'status-btn';
  }

  return (
    <span className="status-btns">
      <button onClick={() => setEditionStatus(status === 'want_to_read' ? null : 'want_to_read')} className={btnClass('want_to_read')} title="Want to read">want</button>
      <button onClick={() => setEditionStatus(status === 'reading' ? null : 'reading')} className={btnClass('reading')} title="Currently reading">reading</button>
      <button onClick={() => setEditionStatus(status === 'finished' ? null : 'finished')} className={btnClass('finished')} title="Finished">finished</button>
      <button onClick={() => setEditionStatus(status === 'did_not_finish' ? null : 'did_not_finish')} className={btnClass('did_not_finish', true)} title="Did not finish">dnf</button>
    </span>
  );
}
