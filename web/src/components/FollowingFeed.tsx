import useSWR from "swr";
import { useStore } from "@nanostores/react";
import { $user } from "../stores/user";
import { $searchQuery, $hasSearchResults } from "../stores/search";
import { fetchFeed, type FeedItem } from "../lib/fetchers";
import { StarsDisplay } from "./Stars";

function formatDate(iso: string): string {
  return new Date(iso).toLocaleDateString(undefined, {
    year: "numeric",
    month: "short",
    day: "numeric",
  });
}

export default function FollowingFeed() {
  const user = useStore($user);
  const searchQuery = useStore($searchQuery);
  const hasSearchResults = useStore($hasSearchResults);

  const { data: feed } = useSWR(user ? "feed" : null, fetchFeed, { revalidateOnFocus: false });

  if (!user || !feed || feed.length === 0 || searchQuery || hasSearchResults) {
    return null;
  }

  return (
    <div className="feed">
      <div className="feed-title">From people you follow</div>
      <div className="feed-items">
        {feed.map((item, i) => (
          <div key={`${item.username}-${item.edition_slug}-${i}`} className="feed-item">
            {item.cover_id ? (
              <a href={`/works/${item.work_slug}/${item.edition_slug}`} className="feed-cover">
                <img
                  src={`https://covers.openlibrary.org/b/id/${item.cover_id}-S.jpg`}
                  alt={`Cover of ${item.edition_title}`}
                  width={33}
                  height={50}
                  loading="lazy"
                />
              </a>
            ) : (
              <a href={`/works/${item.work_slug}/${item.edition_slug}`} className="feed-cover">
                <div className="feed-cover-placeholder" />
              </a>
            )}
            <div className="feed-content">
              <div className="feed-header">
                <a href={`/users/${item.username}`} className="feed-reviewer">
                  {item.display_name || item.username}
                </a>
                <span className="feed-action">reviewed</span>
                <a href={`/works/${item.work_slug}/${item.edition_slug}`} className="feed-book">
                  {item.edition_title}
                </a>
              </div>
              <div className="feed-rating">
                <StarsDisplay rating={item.rating} />
                <span className="feed-date">{formatDate(item.updated_at)}</span>
              </div>
              {item.review_text && (
                <p className="feed-text">
                  {item.review_text.length > 200
                    ? item.review_text.slice(0, 200) + "..."
                    : item.review_text}
                </p>
              )}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
