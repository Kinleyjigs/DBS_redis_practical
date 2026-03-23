import redis
from typing import List, Dict, Optional, Tuple
from datetime import date


class Leaderboard:
    def __init__(
        self,
        game: str = "game",
        mode: str = "default",
        date_str: Optional[str] = None,
        redis_client: Optional[redis.Redis] = None,
    ) -> None:
        self.game = game
        self.mode = mode

        if mode == "daily":
            self.date_str = date_str or date.today().isoformat()
            self.key = f"leaderboard:{game}:daily:{self.date_str}"
        elif mode == "alltime":
            self.key = f"leaderboard:{game}:alltime"
        else:
            self.key = f"leaderboard:{game}"

        self.r = redis_client or redis.Redis(
            host="127.0.0.1",
            port=6379,
            db=0,
            decode_responses=True,
        )

    def _refresh_daily_ttl(self) -> None:
        if self.mode == "daily":
            self.r.expire(self.key, 7 * 24 * 60 * 60)

    def add_score(self, player_id: str, score: float) -> int:
        self.r.zadd(self.key, {player_id: score})
        self._refresh_daily_ttl()
        return self.get_rank(player_id)

    def increment_score(self, player_id: str, delta: float) -> Tuple[float, int]:
        new_score = self.r.zincrby(self.key, delta, player_id)
        self._refresh_daily_ttl()
        new_rank = self.get_rank(player_id)
        return new_score, new_rank

    def get_rank(self, player_id: str) -> Optional[int]:
        rank_zero_based = self.r.zrevrank(self.key, player_id)
        if rank_zero_based is None:
            return None
        return rank_zero_based + 1

    def get_score(self, player_id: str) -> Optional[float]:
        score = self.r.zscore(self.key, player_id)
        return float(score) if score is not None else None

    def get_top(self, n: int = 10) -> List[Dict]:
        results = self.r.zrevrange(self.key, 0, n - 1, withscores=True)
        return [
            {"rank": i + 1, "player": player, "score": score}
            for i, (player, score) in enumerate(results)
        ]

    def get_page(self, page: int, page_size: int = 10) -> List[Dict]:
        if page < 1:
            raise ValueError("page must be >= 1")
        start = (page - 1) * page_size
        end = start + page_size - 1
        results = self.r.zrevrange(self.key, start, end, withscores=True)
        return [
            {"rank": start + i + 1, "player": player, "score": score}
            for i, (player, score) in enumerate(results)
        ]

    def get_around_player(self, player_id: str, radius: int = 2) -> List[Dict]:
        rank_zero_based = self.r.zrevrank(self.key, player_id)
        if rank_zero_based is None:
            return []
        start = max(0, rank_zero_based - radius)
        end = rank_zero_based + radius
        results = self.r.zrevrange(self.key, start, end, withscores=True)
        return [
            {"rank": start + i + 1, "player": player, "score": score}
            for i, (player, score) in enumerate(results)
        ]

    def count_players(self) -> int:
        return self.r.zcard(self.key)

    def remove_player(self, player_id: str) -> bool:
        removed = self.r.zrem(self.key, player_id)
        return removed > 0

    def get_players_in_score_range(self, min_score: float, max_score: float) -> List[Dict]:
        results = self.r.zrevrangebyscore(self.key, max_score, min_score, withscores=True)
        return [
            {"rank": self.get_rank(player), "player": player, "score": score}
            for player, score in results
        ]

    def set_expiry(self, days: int = 7) -> None:
        if self.mode != "daily":
            return
        seconds = days * 24 * 60 * 60
        self.r.expire(self.key, seconds)
        print(f"Key '{self.key}' will expire in {days} day(s) ({seconds} seconds).")


def demo():
    lb = Leaderboard("game:season1")
    lb.r.delete(lb.key)

    print("=" * 45)
    print("Original Demo — Practical 1A Base")
    print("=" * 45)

    print("\nAdding initial scores...")
    lb.add_score("alice", 1500)
    lb.add_score("bob", 2300)
    lb.add_score("charlie", 1800)
    lb.add_score("diana", 2100)
    lb.add_score("eve", 1950)

    print("\nTop 3 players:")
    for entry in lb.get_top(3):
        print(f" #{entry['rank']} {entry['player']}: {entry['score']}")

    print("\nCharlie's current rank and score:")
    print(" Rank:", lb.get_rank("charlie"))
    print(" Score:", lb.get_score("charlie"))

    print("\nIncrementing Charlie's score by 500...")
    new_score, new_rank = lb.increment_score("charlie", 500)
    print(f" New score: {new_score}, new rank: {new_rank}")

    print("\nPlayers around Charlie:")
    for entry in lb.get_around_player("charlie", radius=2):
        print(f" #{entry['rank']} {entry['player']}: {entry['score']}")

    print("\nPage 1 of leaderboard (page_size=3):")
    for entry in lb.get_page(page=1, page_size=3):
        print(f" #{entry['rank']} {entry['player']}: {entry['score']}")


def demo_exercise1():
    print("\n" + "=" * 45)
    print("Exercise 1 — Daily & All-Time Leaderboards")
    print("=" * 45)

    daily = Leaderboard("game", mode="daily")
    alltime = Leaderboard("game", mode="alltime")

    daily.r.delete(daily.key)
    alltime.r.delete(alltime.key)

    print(f"\nDaily key   : {daily.key}")
    print(f"All-time key: {alltime.key}")

    print("\n--- Daily Leaderboard ---")
    daily.add_score("alice", 800)
    daily.add_score("bob", 950)
    daily.add_score("charlie", 700)

    for entry in daily.get_top(3):
        print(f" #{entry['rank']} {entry['player']}: {entry['score']}")

    print("\n--- All-Time Leaderboard ---")
    alltime.add_score("alice", 15000)
    alltime.add_score("bob", 23000)
    alltime.add_score("diana", 19500)
    alltime.increment_score("alice", 3000)

    for entry in alltime.get_top(3):
        print(f" #{entry['rank']} {entry['player']}: {entry['score']}")

    print(f"\nTotal players today   : {daily.count_players()}")
    print(f"Total players all-time: {alltime.count_players()}")

    print("\n--- Exercise 3 — Setting TTL on Daily Leaderboard ---")
    daily.set_expiry(days=7)
    ttl = daily.r.ttl(daily.key)
    print(f"Seconds remaining before key expires: {ttl}")


def demo_exercise2():
    print("\n" + "=" * 45)
    print("Exercise 2 — Players in Score Range")
    print("=" * 45)

    lb = Leaderboard("game:season1")

    print("\nAll players between score 1800 and 2300:")
    for entry in lb.get_players_in_score_range(1800, 2300):
        print(f" #{entry['rank']} {entry['player']}: {entry['score']}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    demo()
    demo_exercise1()
    demo_exercise2()