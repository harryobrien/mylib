import { useState, useEffect } from 'react';

const API_BASE = import.meta.env.PUBLIC_API_URL || '/api';

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
    return <span style={styles.text}>...</span>;
  }

  if (!user) {
    return (
      <a href="/login" style={styles.link}>
        Login
      </a>
    );
  }

  return (
    <span style={styles.text}>
      {user.email}
      <span style={styles.separator}> · </span>
      <a href="#" onClick={handleLogout} style={styles.link}>
        Logout
      </a>
    </span>
  );
}

const styles: Record<string, React.CSSProperties> = {
  text: {
    fontSize: '14px',
    color: '#5a5549',
  },
  link: {
    fontSize: '14px',
    color: '#5a5549',
    textDecoration: 'underline',
    textUnderlineOffset: '2px',
  },
  separator: {
    color: '#a9a49a',
  },
};
