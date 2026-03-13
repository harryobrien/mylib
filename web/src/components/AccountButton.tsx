import { useState, useEffect } from 'react';

const API_BASE = import.meta.env.PUBLIC_API_URL || 'http://localhost:3000';

interface User {
  id: number;
  email: string;
  email_verified: boolean;
}

export default function AccountButton() {
  const [user, setUser] = useState<User | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    checkAuth();
  }, []);

  async function checkAuth() {
    try {
      const res = await fetch(`${API_BASE}/auth/me`, { credentials: 'include' });
      if (res.ok) {
        const data = await res.json();
        if (data.success && data.user) {
          setUser(data.user);
        }
      }
    } catch {
      // Not logged in
    } finally {
      setLoading(false);
    }
  }

  async function handleLogout(e: React.MouseEvent) {
    e.preventDefault();
    await fetch(`${API_BASE}/auth/logout`, {
      method: 'POST',
      credentials: 'include',
    });
    setUser(null);
  }

  if (loading) {
    return <span className="account-text">...</span>;
  }

  if (!user) {
    return <a href="/login" className="account-link">Login</a>;
  }

  return (
    <span className="account-text">
      {user.email}
      <span className="account-sep"> · </span>
      <a href="#" onClick={handleLogout} className="account-link">Logout</a>
    </span>
  );
}
