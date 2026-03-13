import { atom } from 'nanostores';

export const $searchQuery = atom('');
export const $triggerSearch = atom(0); // increment to trigger search with current $searchQuery

export function setSearch(query: string) {
  $searchQuery.set(query);
  $triggerSearch.set($triggerSearch.get() + 1);
}

export function clearSearch() {
  $searchQuery.set('');
  $triggerSearch.set($triggerSearch.get() + 1);
}
