import { useState } from 'react';
import { useStore } from '@nanostores/react';
import { $editingMode } from '../stores/editing';

const API_BASE = import.meta.env.PUBLIC_API_URL || 'http://localhost:3000';

interface Props {
  value: string;
  field: string;
  endpoint: string;
  as?: 'span' | 'p' | 'div' | 'h2';
  className?: string;
  multiline?: boolean;
  placeholder?: string;
  label?: string;
}

export default function EditableField({
  value: initialValue,
  field,
  endpoint,
  as: Tag = 'span',
  className,
  multiline = false,
  placeholder = 'Add...',
  label,
}: Props) {
  const editing = useStore($editingMode);
  const [value, setValue] = useState(initialValue || '');
  const [isEditing, setIsEditing] = useState(false);
  const [saving, setSaving] = useState(false);

  async function handleSave() {
    if (value === initialValue) {
      setIsEditing(false);
      return;
    }

    setSaving(true);
    try {
      const res = await fetch(`${API_BASE}${endpoint}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({ [field]: value || null }),
      });

      if (!res.ok) {
        const data = await res.json();
        alert(data.message || 'Failed to save');
        setValue(initialValue || '');
      }
    } catch {
      alert('Network error');
      setValue(initialValue || '');
    } finally {
      setSaving(false);
      setIsEditing(false);
    }
  }

  function handleKeyDown(e: React.KeyboardEvent) {
    if (e.key === 'Enter' && !multiline) {
      e.preventDefault();
      handleSave();
    }
    if (e.key === 'Escape') {
      setValue(initialValue || '');
      setIsEditing(false);
    }
  }

  if (!editing) {
    return value ? <Tag className={className}>{label}{value}</Tag> : null;
  }

  if (isEditing) {
    return (
      <Tag className={className}>
        {label}
        <span className="editable-field-editing">
          {multiline ? (
            <textarea
              value={value}
              onChange={(e) => setValue(e.target.value)}
              onBlur={handleSave}
              onKeyDown={handleKeyDown}
              autoFocus
              disabled={saving}
              rows={4}
            />
          ) : (
            <input
              type="text"
              value={value}
              onChange={(e) => setValue(e.target.value)}
              onBlur={handleSave}
              onKeyDown={handleKeyDown}
              autoFocus
              disabled={saving}
            />
          )}
        </span>
      </Tag>
    );
  }

  return (
    <Tag className={`editable-field ${className || ''}`}>
      {label}
      {value || <span className="editable-placeholder">{placeholder}</span>}
      <button
        className="edit-pencil"
        onClick={() => setIsEditing(true)}
        title={`Edit ${field}`}
      >
        &#9998;
      </button>
    </Tag>
  );
}
