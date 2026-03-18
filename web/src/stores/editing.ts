import { atom } from 'nanostores';

export const $editingMode = atom(false);

export function toggleEditingMode() {
  $editingMode.set(!$editingMode.get());
}

export function setEditingMode(value: boolean) {
  $editingMode.set(value);
}
