import { useState } from 'react';
import useSWR, { mutate } from 'swr';
import { useStore } from '@nanostores/react';
import { $user } from '../stores/user';
import { fetchMyLists, addWorkToList, removeWorkFromList, createList } from '../lib/fetchers';

interface Props {
  workSlug: string;
}

export default function AddToList({ workSlug }: Props) {
  const user = useStore($user);
  const [open, setOpen] = useState(false);
  const [creating, setCreating] = useState(false);
  const [newTitle, setNewTitle] = useState('');

  const { data: lists } = useSWR(
    user ? 'myLists' : null,
    fetchMyLists,
    { revalidateOnFocus: false }
  );

  if (!user) return null;

  async function toggle(listId: number, inList: boolean) {
    if (inList) {
      await removeWorkFromList(listId, workSlug);
    } else {
      await addWorkToList(listId, workSlug);
    }
    mutate('myLists');
    mutate(`workLists:${workSlug}`);
  }

  async function handleCreate() {
    if (!newTitle.trim()) return;
    const result = await createList({ title: newTitle.trim() });
    if (result.success && result.id) {
      await addWorkToList(result.id, workSlug);
      mutate('myLists');
      setNewTitle('');
      setCreating(false);
    }
  }

  return (
    <div className="add-to-list">
      <button onClick={() => setOpen(!open)} className="add-to-list-btn">
        {open ? '− Lists' : '+ List'}
      </button>
      {open && (
        <div className="add-to-list-dropdown">
          {lists && lists.length > 0 && lists.map(list => {
            const inList = list.work_slugs?.includes(workSlug) ?? false;
            return (
              <label key={list.id} className="add-to-list-item">
                <input
                  type="checkbox"
                  checked={inList}
                  onChange={() => toggle(list.id, inList)}
                />
                <span>{list.title}</span>
                <span className="add-to-list-count">{list.work_count}</span>
              </label>
            );
          })}
          {creating ? (
            <div className="add-to-list-create">
              <input
                type="text"
                value={newTitle}
                onChange={e => setNewTitle(e.target.value)}
                onKeyDown={e => { if (e.key === 'Enter') handleCreate(); if (e.key === 'Escape') setCreating(false); }}
                placeholder="List name..."
                maxLength={200}
                autoFocus
              />
              <button onClick={handleCreate} className="review-submit-btn">Add</button>
            </div>
          ) : (
            <button onClick={() => setCreating(true)} className="add-to-list-new">
              + New list
            </button>
          )}
        </div>
      )}
    </div>
  );
}
