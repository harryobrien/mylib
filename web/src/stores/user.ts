import { atom } from 'nanostores';

export interface User {
  id: number;
  email: string;
  email_verified: boolean;
}

export const $user = atom<User | null>(null);

export function clearUser() {
  $user.set(null);
}
