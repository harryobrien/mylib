import useSWR, { mutate } from "swr";
import { useStore } from "@nanostores/react";
import { $user, clearUser } from "../stores/user";
import { $userEditions } from "../stores/search";
import { $editingMode, toggleEditingMode } from "../stores/editing";
import { fetchUser } from "../lib/fetchers";

export default function AccountButton() {
  const { data: user, isLoading: loading } = useSWR("user", fetchUser, {
    onSuccess: (data) => $user.set(data),
    revalidateOnFocus: false,
  });
  const editing = useStore($editingMode);

  function handleLogout(e: React.MouseEvent) {
    e.preventDefault();
    clearUser();
    $userEditions.set(null);
    mutate("user", null, false);
    mutate("userEditions", [], false);
  }

  if (loading) {
    return <span className="account-text">...</span>;
  }

  if (!user) {
    const isAuthPage =
      typeof window !== "undefined" &&
      (window.location.pathname === "/login" || window.location.pathname === "/register");
    if (isAuthPage) return null;
    return (
      <a href="/login" className="account-link">
        Login
      </a>
    );
  }

  return (
    <span className="account-text">
      <a href={`/users/${user.username}`} className="account-link">
        {user.display_name || user.username}
      </a>
      <span className="account-sep"> · </span>
      <button
        type="button"
        onClick={toggleEditingMode}
        className={editing ? "account-link account-link-active" : "account-link"}
      >
        {editing ? "Editing" : "Edit"}
      </button>
      <span className="account-sep"> · </span>
      <button type="button" onClick={handleLogout} className="account-link">
        Logout
      </button>
    </span>
  );
}
