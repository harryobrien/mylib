import { atom } from 'nanostores';

export const $searchQuery = atom('');
export const $triggerSearch = atom(0);
export const $hasSearchResults = atom(false);

export function setSearch(query: string) {
  $searchQuery.set(query);
  $triggerSearch.set($triggerSearch.get() + 1);
}

export function clearSearch() {
  $searchQuery.set('');
  $triggerSearch.set($triggerSearch.get() + 1);
}

// User editions cache
export interface Edition {
  slug: string;
  work_slug: string;
  title: string;
  status: string;
  cover_id: number | null;
}

export const $userEditions = atom<Edition[] | null>(null);
export const $userEditionsLoading = atom(false);

export async function loadUserEditions(apiBase: string, force = false) {
  if ($userEditions.get() !== null && !force) return;
  if ($userEditionsLoading.get()) return;

  $userEditionsLoading.set(true);
  try {
    const res = await fetch(`${apiBase}/auth/editions`, { credentials: 'include' });
    if (res.ok) {
      const data = await res.json();
      $userEditions.set(data.editions || []);
    } else {
      $userEditions.set([]);
    }
  } catch {
    $userEditions.set([]);
  } finally {
    $userEditionsLoading.set(false);
  }
}

export function invalidateUserEditions() {
  $userEditions.set(null);
}
