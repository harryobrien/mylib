import { atom } from "nanostores";

export interface User {
  id: number;
  email: string;
  email_verified: boolean;
  username: string;
  display_name: string | null;
}

export const $user = atom<User | null>(null);

export function clearUser() {
  $user.set(null);
}
