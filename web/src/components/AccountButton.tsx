import { useEffect } from 'react';
import { useStore } from '@nanostores/react';
import { $user, $userLoading, loadUser, clearUser } from '../stores/user';
import { invalidateUserEditions } from '../stores/search';
import { $editingMode, toggleEditingMode } from '../stores/editing';

const API_BASE = import.meta.env.PUBLIC_API_URL || 'http://localhost:3000';

export default function AccountButton() {
  const user = useStore($user);
  const loading = useStore($userLoading);
  const editing = useStore($editingMode);

  useEffect(() => {
    loadUser(API_BASE);
  }, []);

  async function handleLogout(e: React.MouseEvent) {
    e.preventDefault();
    await fetch(`${API_BASE}/auth/logout`, {
      method: 'POST',
      credentials: 'include',
    });
    clearUser();
    invalidateUserEditions();
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
