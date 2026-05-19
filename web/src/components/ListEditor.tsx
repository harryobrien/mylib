import { useState } from "react";
import { useStore } from "@nanostores/react";
import { $user } from "../stores/user";
import { updateList } from "../lib/fetchers";

interface Props {
  listId: number;
  ownerUsername: string;
  initialTitle: string;
  initialDescription: string;
}

export default function ListEditor({ listId, ownerUsername, initialTitle, initialDescription }: Props) {
  const user = useStore($user);
  const [editing, setEditing] = useState(false);
  const [title, setTitle] = useState(initialTitle);
  const [description, setDescription] = useState(initialDescription);
  const [saving, setSaving] = useState(false);

  if (!user || user.username !== ownerUsername) return null;

  async function handleSave() {
    if (!title.trim()) return;
    setSaving(true);
    const ok = await updateList(listId, {
      title: title.trim(),
      description: description.trim() || undefined,
    });
    if (ok) {
      setEditing(false);
    }
    setSaving(false);
  }

  if (!editing) {
    return (
      <button type="button" onClick={() => setEditing(true)} className="review-edit-btn">
        Edit List
      </button>
    );
  }

  return (
    <div className="list-editor">
      <div className="profile-editor-field">
        <label htmlFor="list-title">Title</label>
        <input
          id="list-title"
          type="text"
          value={title}
          onChange={(e) => setTitle(e.target.value)}
          maxLength={200}
        />
      </div>
      <div className="profile-editor-field">
        <label htmlFor="list-description">Description</label>
        <textarea
          id="list-description"
          value={description}
          onChange={(e) => setDescription(e.target.value)}
          placeholder="Describe this list..."
          rows={3}
        />
      </div>
      <div className="review-actions">
        <button type="button" onClick={handleSave} disabled={saving || !title.trim()} className="review-submit-btn">
          {saving ? "Saving..." : "Save"}
        </button>
        <button
          type="button"
          onClick={() => {
            setTitle(initialTitle);
            setDescription(initialDescription);
            setEditing(false);
          }}
          className="review-cancel-btn"
        >
          Cancel
        </button>
      </div>
    </div>
  );
}
