import redis
import argparse
from typing import Optional, List
from datetime import date, timedelta

class RealtimeAnalytics:
    """
    Real-time analytics using Redis bitmaps and HyperLogLog.
    Tracks:
      - Daily Active Users (DAU) via bitmaps.
      - Daily Unique Visitors (UV) via HyperLogLog.
      - Weekly UV via PFMERGE (Exercise 1).
      - Stickiness Ratio (DAU / MAU) via PFMERGE of 30 days (Exercise 2).
      - CLI report for a given date (Exercise 3).
    """

    def __init__(self, redis_client: Optional[redis.Redis] = None) -> None:
        self.r = redis_client or redis.Redis(
            host="127.0.0.1",
            port=6379,
            db=0,
            decode_responses=True,
        )

    # --------------------
    # Bitmap-based metrics
    # --------------------
    def _dau_key(self, date_str: str) -> str:
        return f"analytics:dau:{date_str}"

    def mark_user_active(self, date_str: str, user_id: int) -> None:
        if user_id < 0:
            raise ValueError("user_id must be non-negative")
        self.r.setbit(self._dau_key(date_str), user_id, 1)

    def is_user_active(self, date_str: str, user_id: int) -> bool:
        return self.r.getbit(self._dau_key(date_str), user_id) == 1

    def count_daily_active_users(self, date_str: str) -> int:
        return self.r.bitcount(self._dau_key(date_str))

    # ------------------------
    # HyperLogLog-based metrics
    # ------------------------
    def _uv_key(self, date_str: str) -> str:
        return f"analytics:uv:{date_str}"

    def add_visit(self, date_str: str, user_identifier: str) -> None:
        self.r.pfadd(self._uv_key(date_str), user_identifier)

    def count_unique_visitors(self, date_str: str) -> int:
        return self.r.pfcount(self._uv_key(date_str))

    # -------------------------------------------------------
    # Exercise 1: merge_uv — merge daily HLLs into a weekly key
    # -------------------------------------------------------
    def merge_uv(self, date_str_list: List[str], dest_key: str) -> int:
        """
        Merge multiple daily HyperLogLog UV keys into a single destination key
        using PFMERGE, then return the approximate unique visitor count.
        """
        source_keys = [self._uv_key(d) for d in date_str_list]
        self.r.pfmerge(dest_key, *source_keys)
        return self.r.pfcount(dest_key)

    # -------------------------------------------------------
    # Exercise 2: compute_stickiness — DAU / MAU ratio
    # -------------------------------------------------------
    def compute_stickiness(self, reference_date_str: str) -> float:
        """
        Compute the stickiness ratio = DAU / MAU.
        DAU: exact count for today using bitmap BITCOUNT.
        MAU: approximate unique users over 30 days using PFMERGE + PFCOUNT.
        """
        ref = date.fromisoformat(reference_date_str)
        last_30_days = [
            (ref - timedelta(days=i)).isoformat()
            for i in range(30)
        ]
        dau = self.count_daily_active_users(reference_date_str)
        mau_key = f"analytics:uv:mau_temp:{reference_date_str}"
        source_keys = [self._uv_key(d) for d in last_30_days]
        self.r.pfmerge(mau_key, *source_keys)
        mau = self.r.pfcount(mau_key)
        self.r.delete(mau_key)
        if mau == 0:
            return 0.0
        return dau / mau

    # -------------------------------------------------------
    # Exercise 3: print_daily_report — CLI report for a given date
    # -------------------------------------------------------
    def print_daily_report(self, date_str: str) -> None:
        """
        Print DAU and UV report for a given date.

        DAU: exact count of users active on that day (bitmap BITCOUNT).
        UV:  approximate unique visitors on that day (HyperLogLog PFCOUNT).

        Args:
            date_str: Date string in YYYY-MM-DD format passed from CLI.
        """
        dau = self.count_daily_active_users(date_str)
        uv = self.count_unique_visitors(date_str)

        print(f"=== Analytics Report for {date_str} ===")
        print(f"  Daily Active Users (DAU) [exact]:    {dau}")
        print(f"  Unique Visitors     (UV) [approx]:   {uv}")


def seed_demo_data(analytics: RealtimeAnalytics, date_str: str) -> None:
    """Seed some demo data so the CLI report has something to show."""
    analytics.r.delete(analytics._dau_key(date_str))
    analytics.r.delete(analytics._uv_key(date_str))

    # 3 users active today
    analytics.mark_user_active(date_str, 1)
    analytics.mark_user_active(date_str, 2)
    analytics.mark_user_active(date_str, 3)

    # 4 unique visitors today (user2 visits twice — still counts as 1)
    analytics.add_visit(date_str, "user1")
    analytics.add_visit(date_str, "user2")
    analytics.add_visit(date_str, "user2")  # duplicate
    analytics.add_visit(date_str, "user3")
    analytics.add_visit(date_str, "user4")


# -------------------------------------------------------
# CLI entry point
# Usage: python3 analytics.py --date 2026-03-17
# -------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Print DAU and UV analytics report for a given date."
    )
    parser.add_argument(
        "--date",
        type=str,
        required=True,
        help="Date in YYYY-MM-DD format (e.g. 2026-03-17)",
    )
    args = parser.parse_args()

    analytics = RealtimeAnalytics()

    # Seed demo data for the given date so report is not empty
    seed_demo_data(analytics, args.date)

    # Print the report for the given date
    analytics.print_daily_report(args.date)