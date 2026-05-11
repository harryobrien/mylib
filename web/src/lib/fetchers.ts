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

export interface Review {
  user_id: number;
  rating: number;
  review_text: string | null;
  edition_slug?: string;
  edition_title?: string;
  created_at: string;
  updated_at: string;
}

export async function fetchEditionReviews(slug: string): Promise<Review[]> {
  const res = await fetch(`${API_BASE}/editions/${slug}/reviews`);
  if (!res.ok) return [];
  const data = await res.json();
  return data.reviews || [];
}

export async function fetchWorkReviews(slug: string): Promise<Review[]> {
  const res = await fetch(`${API_BASE}/works/${slug}/reviews`);
  if (!res.ok) return [];
  const data = await res.json();
  return data.reviews || [];
}

export async function fetchUserReview(slug: string): Promise<Review | null> {
  const res = await fetch(`${API_BASE}/auth/editions/${slug}/review`, { credentials: 'include' });
  if (!res.ok) return null;
  const data = await res.json();
  return data.review || null;
}

export async function updateProgress(
  slug: string,
  data: { current_page?: number; started_at?: string; finished_at?: string },
): Promise<boolean> {
  const res = await fetch(`${API_BASE}/auth/editions/${slug}/progress`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    credentials: 'include',
    body: JSON.stringify(data),
  });
  return res.ok;
}
