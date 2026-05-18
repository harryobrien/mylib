import useSWR from "swr";
import { fetchWorkLists } from "../lib/fetchers";

interface Props {
  workSlug: string;
}

export default function WorkLists({ workSlug }: Props) {
  const { data: lists } = useSWR(`workLists:${workSlug}`, () => fetchWorkLists(workSlug), {
    revalidateOnFocus: false,
  });

  if (!lists || lists.length === 0) return null;

  return (
    <section className="work-lists">
      <h2>In Lists</h2>
      <div className="work-lists-items">
        {lists.map((l) => (
          <a key={l.id} href={`/lists/${l.id}`} className="work-list-item">
            <span className="work-list-title">{l.title}</span>
            <span className="work-list-by">by {l.display_name || l.username}</span>
          </a>
        ))}
      </div>
    </section>
  );
}
