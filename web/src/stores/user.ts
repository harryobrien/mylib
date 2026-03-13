import { atom } from 'nanostores';

export interface User {
  id: number;
  email: string;
  email_verified: boolean;
}

export const $user = atom<User | null>(null);
export const $userLoading = atom(true);

export async function loadUser(apiBase: string) {
  try {
    const res = await fetch(`${apiBase}/auth/me`, { credentials: 'include' });
    if (res.ok) {
      const data = await res.json();
      if (data.success && data.user) {
        $user.set(data.user);
      }
    }
  } catch {
    // Not logged in
  } finally {
    $userLoading.set(false);
  }
}

export function clearUser() {
  $user.set(null);
}
