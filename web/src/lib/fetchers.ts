import type { User } from "../stores/user";
import type { Edition } from "../stores/search";

const API_BASE = import.meta.env.PUBLIC_API_URL || "http://localhost:3000";

export async function fetchUser(): Promise<User | null> {
  const res = await fetch(`${API_BASE}/auth/me`, { credentials: "include" });
  if (!res.ok) return null;
  const data = await res.json();
  return data.success && data.user ? data.user : null;
}

export async function fetchUserEditions(): Promise<Edition[]> {
  const res = await fetch(`${API_BASE}/auth/editions`, { credentials: "include" });
  if (!res.ok) return [];
  const data = await res.json();
  return data.editions || [];
}

export interface Review {
  user_id: number;
  username: string;
  display_name: string | null;
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
  const res = await fetch(`${API_BASE}/auth/editions/${slug}/review`, { credentials: "include" });
  if (!res.ok) return null;
  const data = await res.json();
  return data.review || null;
}

export async function updateProgress(
  slug: string,
  data: { current_page?: number; started_at?: string; finished_at?: string },
): Promise<boolean> {
  const res = await fetch(`${API_BASE}/auth/editions/${slug}/progress`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    credentials: "include",
    body: JSON.stringify(data),
  });
  return res.ok;
}

export interface UserProfile {
  username: string;
  display_name: string | null;
  bio: string | null;
  created_at: string;
  reading_stats: {
    want_to_read: number;
    reading: number;
    finished: number;
    did_not_finish: number;
  };
  reviews: Review[];
  lists: { id: number; title: string; work_count: number }[];
  followers_count: number;
  following_count: number;
}

export async function fetchUserProfile(username: string): Promise<UserProfile | null> {
  const res = await fetch(`${API_BASE}/users/${username}`);
  if (!res.ok) return null;
  return await res.json();
}

export async function updateProfile(data: {
  username?: string;
  display_name?: string;
  bio?: string;
}): Promise<{ success: boolean; message?: string }> {
  const res = await fetch(`${API_BASE}/auth/profile`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    credentials: "include",
    body: JSON.stringify(data),
  });
  return await res.json();
}

export async function fetchFollowState(username: string): Promise<boolean> {
  const res = await fetch(`${API_BASE}/auth/following/${username}`, { credentials: "include" });
  if (!res.ok) return false;
  const data = await res.json();
  return data.following ?? false;
}

export async function followUser(username: string): Promise<boolean> {
  const res = await fetch(`${API_BASE}/auth/following/${username}`, {
    method: "PUT",
    credentials: "include",
  });
  return res.ok;
}

export async function unfollowUser(username: string): Promise<boolean> {
  const res = await fetch(`${API_BASE}/auth/following/${username}`, {
    method: "DELETE",
    credentials: "include",
  });
  return res.ok;
}

export interface FeedItem {
  username: string;
  display_name: string | null;
  rating: number;
  review_text: string | null;
  edition_slug: string;
  edition_title: string;
  work_slug: string;
  cover_id: number | null;
  updated_at: string;
}

export async function fetchFeed(): Promise<FeedItem[]> {
  const res = await fetch(`${API_BASE}/auth/feed`, { credentials: "include" });
  if (!res.ok) return [];
  const data = await res.json();
  return data.feed || [];
}

export interface ListDetail {
  id: number;
  title: string;
  description: string | null;
  username: string;
  display_name: string | null;
  created_at: string;
  works: {
    slug: string;
    title: string;
    description: string | null;
    cover_id: number | null;
    position: number;
  }[];
}

export interface ListSummary {
  id: number;
  title: string;
  description: string | null;
  work_count: number;
  work_slugs?: string[];
}

export interface WorkListItem {
  id: number;
  title: string;
  username: string;
  display_name: string | null;
}

export async function fetchList(id: number): Promise<ListDetail | null> {
  const res = await fetch(`${API_BASE}/lists/${id}`);
  if (!res.ok) return null;
  return await res.json();
}

export async function fetchWorkLists(slug: string): Promise<WorkListItem[]> {
  const res = await fetch(`${API_BASE}/works/${slug}/lists`);
  if (!res.ok) return [];
  const data = await res.json();
  return data.lists || [];
}

export async function fetchMyLists(): Promise<ListSummary[]> {
  const res = await fetch(`${API_BASE}/auth/lists`, { credentials: "include" });
  if (!res.ok) return [];
  const data = await res.json();
  return data.lists || [];
}

export async function createList(data: {
  title: string;
  description?: string;
}): Promise<{ success: boolean; id?: number }> {
  const res = await fetch(`${API_BASE}/auth/lists`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    credentials: "include",
    body: JSON.stringify(data),
  });
  return await res.json();
}

export async function deleteList(id: number): Promise<boolean> {
  const res = await fetch(`${API_BASE}/auth/lists/${id}`, {
    method: "DELETE",
    credentials: "include",
  });
  return res.ok;
}

export async function addWorkToList(listId: number, workSlug: string): Promise<boolean> {
  const res = await fetch(`${API_BASE}/auth/lists/${listId}/works/${workSlug}`, {
    method: "PUT",
    credentials: "include",
  });
  return res.ok;
}

export async function removeWorkFromList(listId: number, workSlug: string): Promise<boolean> {
  const res = await fetch(`${API_BASE}/auth/lists/${listId}/works/${workSlug}`, {
    method: "DELETE",
    credentials: "include",
  });
  return res.ok;
}
