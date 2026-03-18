import useSWR, { mutate } from 'swr';
import { useStore } from '@nanostores/react';
import { $user } from '../stores/user';
import { $userEditions } from '../stores/search';
import { fetchUserEditions } from '../lib/fetchers';

const API_BASE = import.meta.env.PUBLIC_API_URL || 'http://localhost:3000';

interface Props {
  slug: string;
}

type Status = 'reading' | 'want_to_read' | 'finished' | 'did_not_finish' | null;

export default function ReadingStatus({ slug }: Props) {
  const user = useStore($user);

  const { data: editions, isLoading } = useSWR(
    user ? 'userEditions' : null,
    fetchUserEditions,
    {
      onSuccess: (data) => $userEditions.set(data),
      revalidateOnFocus: false,
    }
  );

  const status = (editions?.find(e => e.slug === slug)?.status as Status) ?? null;

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
    mutate('userEditions');
  }

  if (isLoading || !user) {
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
