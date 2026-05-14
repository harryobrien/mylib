import useSWR from 'swr';
import { fetchEditionReviews, fetchWorkReviews, type Review } from '../lib/fetchers';
import { StarsDisplay } from './Stars';

interface Props {
  editionSlug?: string;
  workSlug?: string;
}

function formatDate(iso: string): string {
  return new Date(iso).toLocaleDateString(undefined, {
    year: 'numeric',
    month: 'short',
    day: 'numeric',
  });
}

export default function ReviewList({ editionSlug, workSlug }: Props) {
  const swrKey = editionSlug
    ? `editionReviews:${editionSlug}`
    : workSlug
      ? `workReviews:${workSlug}`
      : null;

  const fetcher = editionSlug
    ? () => fetchEditionReviews(editionSlug)
    : workSlug
      ? () => fetchWorkReviews(workSlug)
      : null;

  const { data: reviews } = useSWR<Review[]>(swrKey, fetcher!, {
    revalidateOnFocus: false,
  });

  if (!reviews || reviews.length === 0) return null;

  return (
    <div className="review-list">
      <h2>Reviews ({reviews.length})</h2>
      {reviews.map((r, i) => (
        <div key={`${r.user_id}-${i}`} className="review-item">
          <div className="review-header">
            {r.username && (
              <a href={`/users/${r.username}`} className="review-author">
                {r.display_name || r.username}
              </a>
            )}
            <StarsDisplay rating={r.rating} />
            {r.edition_title && (
              <span className="review-edition">{r.edition_title}</span>
            )}
            <span className="review-date">{formatDate(r.updated_at)}</span>
          </div>
          {r.review_text && <p className="review-body">{r.review_text}</p>}
        </div>
      ))}
    </div>
  );
}
