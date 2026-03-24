import redis
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

        Args:
            date_str_list: List of date strings e.g. ['2026-03-11', ..., '2026-03-17']
            dest_key:      Destination Redis key for the merged HLL
                           e.g. 'analytics:uv:week:2026-W12'

        Returns:
            Approximate unique visitors across all the given dates.
        """
        source_keys = [self._uv_key(d) for d in date_str_list]
        # PFMERGE unions all daily HLL keys into dest_key
        # duplicates across days are automatically deduplicated
        self.r.pfmerge(dest_key, *source_keys)
        return self.r.pfcount(dest_key)

    # -------------------------------------------------------
    # Exercise 2: compute_stickiness — DAU / MAU ratio
    # -------------------------------------------------------
    def compute_stickiness(self, reference_date_str: str) -> float:
        """
        Compute the stickiness ratio = DAU / MAU.

        DAU (Daily Active Users):
            Exact count for reference_date_str using bitmap BITCOUNT.
            Represents how many users were active today.

        MAU (Monthly Active Users):
            Approximate unique users over the 30-day window ending on
            reference_date_str. Derived by merging 30 daily HLL keys
            using PFMERGE, then PFCOUNT.
            Represents how many unique users were active this month.

        Stickiness = DAU / MAU:
            A higher ratio means users return daily — the app is "sticky".
            e.g. 0.50 means 50% of monthly users come back every day.

        Args:
            reference_date_str: The "today" date string (e.g. '2026-03-17').

        Returns:
            Stickiness ratio as a float between 0.0 and 1.0.
            Returns 0.0 if MAU is zero to avoid division by zero.
        """
        ref = date.fromisoformat(reference_date_str)

        # Build the last 30 days including today
        last_30_days = [
            (ref - timedelta(days=i)).isoformat()
            for i in range(30)
        ]

        # --- DAU: exact count from bitmap ---
        dau = self.count_daily_active_users(reference_date_str)

        # --- MAU: merge 30 daily HLL keys into a temporary key ---
        mau_key = f"analytics:uv:mau_temp:{reference_date_str}"
        source_keys = [self._uv_key(d) for d in last_30_days]
        self.r.pfmerge(mau_key, *source_keys)
        mau = self.r.pfcount(mau_key)

        # Delete temp key immediately — no need to persist it
        self.r.delete(mau_key)

        # --- Stickiness = DAU / MAU ---
        if mau == 0:
            return 0.0
        return dau / mau


def demo():
    analytics = RealtimeAnalytics()
    date_str = "2026-03-17"

    # Clear previous data
    analytics.r.delete(analytics._dau_key(date_str))
    for i in range(30):
        d = (date.fromisoformat(date_str) - timedelta(days=i)).isoformat()
        analytics.r.delete(analytics._uv_key(d))

    print(f"Seeding data for stickiness demo ({date_str})...\n")

    # Seed DAU — 3 users active today
    analytics.mark_user_active(date_str, 1)
    analytics.mark_user_active(date_str, 2)
    analytics.mark_user_active(date_str, 3)

    # Seed UV across 3 days to simulate monthly visitors
    # 7 unique users total across the month
    analytics.add_visit("2026-03-17", "user1")
    analytics.add_visit("2026-03-17", "user2")
    analytics.add_visit("2026-03-17", "user3")
    analytics.add_visit("2026-03-16", "user2")
    analytics.add_visit("2026-03-16", "user4")
    analytics.add_visit("2026-03-16", "user5")
    analytics.add_visit("2026-03-15", "user1")
    analytics.add_visit("2026-03-15", "user6")
    analytics.add_visit("2026-03-15", "user7")

    # Get DAU and MAU individually for display
    dau = analytics.count_daily_active_users(date_str)

    # Build MAU for display (separate from compute_stickiness temp key)
    mau_display_key = "analytics:uv:mau:2026-03-17"
    analytics.r.delete(mau_display_key)
    last_30 = [
        (date.fromisoformat(date_str) - timedelta(days=i)).isoformat()
        for i in range(30)
    ]
    analytics.r.pfmerge(mau_display_key, *[analytics._uv_key(d) for d in last_30])
    mau = analytics.r.pfcount(mau_display_key)

    stickiness = analytics.compute_stickiness(date_str)

    print(f"Daily Active Users  (DAU) [exact]:   {dau}")
    print(f"Monthly Active Users(MAU) [approx]:  {mau}")
    print(f"Stickiness Ratio (DAU/MAU):          {stickiness:.2f} ({stickiness:.2%})")


if __name__ == "__main__":
    demo()