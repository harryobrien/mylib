import useSWR, { mutate } from 'swr';
import { useStore } from '@nanostores/react';
import { $user, clearUser } from '../stores/user';
import { $userEditions } from '../stores/search';
import { $editingMode, toggleEditingMode } from '../stores/editing';
import { fetchUser } from '../lib/fetchers';

const API_BASE = import.meta.env.PUBLIC_API_URL || 'http://localhost:3000';

export default function AccountButton() {
  const { data: user, isLoading: loading } = useSWR('user', fetchUser, {
    onSuccess: (data) => $user.set(data),
    revalidateOnFocus: false,
  });
  const editing = useStore($editingMode);

  async function handleLogout(e: React.MouseEvent) {
    e.preventDefault();
    await fetch(`${API_BASE}/auth/logout`, {
      method: 'POST',
      credentials: 'include',
    });
    clearUser();
    $userEditions.set(null);
    mutate('user', null, false);
    mutate('userEditions', [], false);
  }

  if (loading) {
    return <span className="account-text">...</span>;
  }

  if (!user) {
    const isAuthPage = typeof window !== 'undefined' &&
      (window.location.pathname === '/login' || window.location.pathname === '/register');
    if (isAuthPage) return null;
    return <a href="/login" className="account-link">Login</a>;
  }

  return (
    <span className="account-text">
      {user.email}
      <span className="account-sep"> · </span>
      <a href="#" onClick={(e) => { e.preventDefault(); toggleEditingMode(); }} className={editing ? 'account-link account-link-active' : 'account-link'}>
        {editing ? 'Editing' : 'Edit'}
      </a>
      <span className="account-sep"> · </span>
      <a href="#" onClick={handleLogout} className="account-link">Logout</a>
    </span>
  );
}
