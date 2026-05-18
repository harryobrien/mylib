import { useState } from "react";
import useSWR, { mutate } from "swr";
import { useStore } from "@nanostores/react";
import { $user } from "../stores/user";
import { fetchFollowState, followUser, unfollowUser } from "../lib/fetchers";

interface Props {
  username: string;
}

export default function FollowButton({ username }: Props) {
  const user = useStore($user);
  const [loading, setLoading] = useState(false);

  const { data: following } = useSWR(
    user && user.username !== username ? `followState:${username}` : null,
    () => fetchFollowState(username),
    { revalidateOnFocus: false },
  );

  if (!user || user.username === username) return null;

  async function toggle() {
    setLoading(true);
    if (following) {
      await unfollowUser(username);
    } else {
      await followUser(username);
    }
    mutate(`followState:${username}`);
    setLoading(false);
  }

  return (
    <button
      type="button"
      onClick={toggle}
      disabled={loading}
      className={following ? "follow-btn follow-btn-following" : "follow-btn"}
    >
      {following ? "Following" : "Follow"}
    </button>
  );
}
