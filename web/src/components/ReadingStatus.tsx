import { useState, useEffect } from 'react';

const API_BASE = import.meta.env.PUBLIC_API_URL || 'http://localhost:3000';

interface Props {
  slug: string;
}

type Status = 'reading' | 'want_to_read' | 'finished' | 'did_not_finish' | null;

export default function ReadingStatus({ slug }: Props) {
  const [status, setStatus] = useState<Status>(null);
  const [isLoggedIn, setIsLoggedIn] = useState(false);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    checkStatus();
  }, []);

  async function checkStatus() {
    try {
      const res = await fetch(`${API_BASE}/auth/editions`, { credentials: 'include' });
      if (res.ok) {
        setIsLoggedIn(true);
        const data = await res.json();
        const edition = data.editions?.find((e: any) => e.slug === slug);
        if (edition) {
          setStatus(edition.status);
        }
      }
    } catch {
      // Not logged in or error
    } finally {
      setLoading(false);
    }
  }

  async function setEditionStatus(newStatus: Status) {
    if (!newStatus) {
      await fetch(`${API_BASE}/auth/editions/${slug}`, {
        method: 'DELETE',
        credentials: 'include',
      });
      setStatus(null);
    } else {
      await fetch(`${API_BASE}/auth/editions/${slug}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({ status: newStatus }),
      });
      setStatus(newStatus);
    }
  }

  if (loading || !isLoggedIn) {
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
