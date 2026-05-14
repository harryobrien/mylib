import { useState } from 'react';
import useSWR, { mutate } from 'swr';
import { useStore } from '@nanostores/react';
import { $user } from '../stores/user';
import { fetchUserReview } from '../lib/fetchers';
import { StarsDisplay, StarsInput } from './Stars';

const API_BASE = import.meta.env.PUBLIC_API_URL || 'http://localhost:3000';

interface Props {
  editionSlug: string;
}

export default function ReviewForm({ editionSlug }: Props) {
  const user = useStore($user);
  const { data: existingReview } = useSWR(
    user ? `userReview:${editionSlug}` : null,
    () => fetchUserReview(editionSlug),
    { revalidateOnFocus: false }
  );

  const [rating, setRating] = useState<number>(0);
  const [hoverRating, setHoverRating] = useState<number>(0);
  const [reviewText, setReviewText] = useState('');
  const [editing, setEditing] = useState(false);
  const [submitting, setSubmitting] = useState(false);

  const hasReview = existingReview != null;

  function startEditing() {
    setRating(existingReview?.rating || 0);
    setReviewText(existingReview?.review_text || '');
    setEditing(true);
  }

  async function handleSubmit() {
    const finalRating = rating || existingReview?.rating;
    if (!finalRating) return;
    setSubmitting(true);
    try {
      await fetch(`${API_BASE}/auth/editions/${editionSlug}/review`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({
          rating: finalRating,
          review_text: reviewText || null,
        }),
      });
      mutate(`userReview:${editionSlug}`);
      mutate(`editionReviews:${editionSlug}`);
      setEditing(false);
    } finally {
      setSubmitting(false);
    }
  }

  async function handleDelete() {
    setSubmitting(true);
    try {
      await fetch(`${API_BASE}/auth/editions/${editionSlug}/review`, {
        method: 'DELETE',
        credentials: 'include',
      });
      mutate(`userReview:${editionSlug}`);
      mutate(`editionReviews:${editionSlug}`);
      setRating(0);
      setReviewText('');
      setEditing(false);
    } finally {
      setSubmitting(false);
    }
  }

  if (!user) return null;

  if (hasReview && !editing) {
    return (
      <div className="review-form">
        <div className="review-yours">
          <span className="review-label">Your rating</span>
          <StarsDisplay rating={existingReview.rating} />
          {existingReview.review_text && (
            <p className="review-text-preview">{existingReview.review_text}</p>
          )}
          <button onClick={startEditing} className="review-edit-btn">Edit</button>
        </div>
      </div>
    );
  }

  if (!editing && !hasReview) {
    return (
      <div className="review-form">
        <div className="review-quick">
          <span className="review-label">Rate this edition</span>
          <StarsInput
            value={0}
            hoverValue={hoverRating}
            onHover={setHoverRating}
            onSelect={(v) => { setRating(v); startEditing(); setRating(v); }}
          />
        </div>
      </div>
    );
  }

  return (
    <div className="review-form">
      <div className="review-editing">
        <span className="review-label">{hasReview ? 'Edit your review' : 'Write a review'}</span>
        <StarsInput
          value={rating}
          hoverValue={hoverRating}
          onHover={setHoverRating}
          onSelect={setRating}
        />
        <textarea
          className="review-textarea"
          value={reviewText}
          onChange={(e) => setReviewText(e.target.value)}
          placeholder="Write a review (optional)..."
          rows={4}
          maxLength={10000}
        />
        <div className="review-actions">
          <button onClick={handleSubmit} disabled={submitting || (!rating && !existingReview?.rating)} className="review-submit-btn">
            {submitting ? 'Saving...' : 'Save'}
          </button>
          <button onClick={() => setEditing(false)} className="review-cancel-btn">Cancel</button>
          {hasReview && (
            <button onClick={handleDelete} disabled={submitting} className="review-delete-btn">
              Delete
            </button>
          )}
        </div>
      </div>
    </div>
  );
}
