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

// User editions - shared via nanostore, fetched via SWR in components
export interface Edition {
  slug: string;
  work_slug: string;
  title: string;
  status: string;
  cover_id: number | null;
}

export const $userEditions = atom<Edition[] | null>(null);
