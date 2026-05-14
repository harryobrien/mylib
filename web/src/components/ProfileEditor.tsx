import { useState } from 'react';
import { useStore } from '@nanostores/react';
import { $user } from '../stores/user';
import { updateProfile } from '../lib/fetchers';
import { mutate } from 'swr';

interface Props {
  username: string;
}

export default function ProfileEditor({ username }: Props) {
  const user = useStore($user);
  const [editing, setEditing] = useState(false);
  const [formUsername, setFormUsername] = useState('');
  const [displayName, setDisplayName] = useState('');
  const [bio, setBio] = useState('');
  const [error, setError] = useState('');
  const [saving, setSaving] = useState(false);

  if (!user || user.username !== username) return null;

  function startEditing() {
    setFormUsername(user!.username);
    setDisplayName(user!.display_name || '');
    setBio('');
    setEditing(true);
    setError('');
  }

  async function handleSave() {
    setSaving(true);
    setError('');
    const data: Record<string, string> = {};
    if (formUsername !== user!.username) data.username = formUsername;
    if (displayName !== (user!.display_name || '')) data.display_name = displayName;
    if (bio) data.bio = bio;

    if (Object.keys(data).length === 0) {
      setEditing(false);
      setSaving(false);
      return;
    }

    const result = await updateProfile(data);
    if (result.success) {
      mutate('user');
      if (data.username) {
        window.location.href = `/users/${data.username}`;
      } else {
        setEditing(false);
      }
    } else {
      setError(result.message || 'Failed to save');
    }
    setSaving(false);
  }

  if (!editing) {
    return <button onClick={startEditing} className="review-edit-btn">Edit Profile</button>;
  }

  return (
    <div className="profile-editor">
      <div className="profile-editor-field">
        <label>Username</label>
        <input
          type="text"
          value={formUsername}
          onChange={e => setFormUsername(e.target.value)}
          maxLength={30}
        />
      </div>
      <div className="profile-editor-field">
        <label>Display Name</label>
        <input
          type="text"
          value={displayName}
          onChange={e => setDisplayName(e.target.value)}
          placeholder="Optional"
          maxLength={100}
        />
      </div>
      <div className="profile-editor-field">
        <label>Bio</label>
        <textarea
          value={bio}
          onChange={e => setBio(e.target.value)}
          placeholder="Tell us about yourself..."
          rows={3}
          maxLength={5000}
        />
      </div>
      {error && <p className="profile-editor-error">{error}</p>}
      <div className="review-actions">
        <button onClick={handleSave} disabled={saving} className="review-submit-btn">
          {saving ? 'Saving...' : 'Save'}
        </button>
        <button onClick={() => setEditing(false)} className="review-cancel-btn">Cancel</button>
      </div>
    </div>
  );
}
