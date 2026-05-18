import useSWR from "swr";
import { useStore } from "@nanostores/react";
import { $user } from "../stores/user";
import { fetchMyLists, createList, deleteList } from "../lib/fetchers";
import { mutate } from "swr";
import { useState } from "react";

interface Props {
  username: string;
}

export default function UserLists({ username }: Props) {
  const user = useStore($user);
  const isOwner = user?.username === username;

  const { data: lists } = useSWR(
    isOwner ? "myLists" : `userLists:${username}`,
    isOwner
      ? fetchMyLists
      : async () => {
          const API_BASE = import.meta.env.PUBLIC_API_URL || "http://localhost:3000";
          const res = await fetch(`${API_BASE}/users/${username}/lists`);
          if (!res.ok) return [];
          const data = await res.json();
          return data.lists || [];
        },
    { revalidateOnFocus: false },
  );

  const [creating, setCreating] = useState(false);
  const [title, setTitle] = useState("");

  async function handleCreate() {
    if (!title.trim()) return;
    await createList({ title: title.trim() });
    mutate("myLists");
    setTitle("");
    setCreating(false);
  }

  async function handleDelete(id: number) {
    await deleteList(id);
    mutate("myLists");
  }

  if (!lists || (lists.length === 0 && !isOwner)) return null;

  return (
    <section className="profile-lists">
      <h2>Lists ({lists.length})</h2>
      {lists.length > 0 && (
        <div className="profile-lists-grid">
          {lists.map((l: any) => (
            <div key={l.id} className="profile-list-item">
              <a href={`/lists/${l.id}`} className="profile-list-link">
                <span className="profile-list-title">{l.title}</span>
                <span className="profile-list-count">
                  {l.work_count} {l.work_count === 1 ? "book" : "books"}
                </span>
              </a>
              {isOwner && (
                <button
                  type="button"
                  onClick={() => handleDelete(l.id)}
                  className="profile-list-delete"
                  title="Delete list"
                  aria-label={`Delete list ${l.title}`}
                >
                  &times;
                </button>
              )}
            </div>
          ))}
        </div>
      )}
      {isOwner &&
        (creating ? (
          <div className="add-to-list-create" style={{ marginTop: "8px" }}>
            <input
              type="text"
              value={title}
              onChange={(e) => setTitle(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter") handleCreate();
                if (e.key === "Escape") setCreating(false);
              }}
              placeholder="List name..."
              maxLength={200}
              autoFocus
            />
            <button type="button" onClick={handleCreate} className="review-submit-btn">
              Create
            </button>
          </div>
        ) : (
          <button
            type="button"
            onClick={() => setCreating(true)}
            className="add-to-list-new"
            style={{ marginTop: "8px" }}
          >
            + New list
          </button>
        ))}
    </section>
  );
}
