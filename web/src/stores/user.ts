import { atom } from "nanostores";

export interface User {
  id: number;
  email: string;
  email_verified: boolean;
  username: string;
  display_name: string | null;
}

export const $user = atom<User | null>(null);

const TOKEN_KEY = "mylib_token";

export function getToken(): string | null {
  if (typeof window === "undefined") return null;
  return localStorage.getItem(TOKEN_KEY);
}

export function setToken(token: string) {
  localStorage.setItem(TOKEN_KEY, token);
}

export function clearToken() {
  localStorage.removeItem(TOKEN_KEY);
}

export function clearUser() {
  $user.set(null);
  clearToken();
}

export function getUserFromToken(): User | null {
  const token = getToken();
  if (!token) return null;
  try {
    const payload = JSON.parse(atob(token.split(".")[1]));
    if (payload.exp * 1000 < Date.now()) {
      clearToken();
      return null;
    }
    return {
      id: payload.sub,
      email: payload.email,
      email_verified: payload.email_verified ?? false,
      username: payload.username,
      display_name: payload.display_name ?? null,
    };
  } catch {
    clearToken();
    return null;
  }
}
