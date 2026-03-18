import type { User } from '../stores/user';
import type { Edition } from '../stores/search';

const API_BASE = import.meta.env.PUBLIC_API_URL || 'http://localhost:3000';

export async function fetchUser(): Promise<User | null> {
  const res = await fetch(`${API_BASE}/auth/me`, { credentials: 'include' });
  if (!res.ok) return null;
  const data = await res.json();
  return data.success && data.user ? data.user : null;
}

export async function fetchUserEditions(): Promise<Edition[]> {
  const res = await fetch(`${API_BASE}/auth/editions`, { credentials: 'include' });
  if (!res.ok) return [];
  const data = await res.json();
  return data.editions || [];
}
