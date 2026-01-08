#!/usr/bin/env python3

import pandas as pd
from pathlib import Path

INPUT = Path("ai_wayback_panel_tool_day.final.csv")
OUTPUT = Path("ai_wayback_panel_tool_day.final.csv")

def main():
    print("🔹 Reading file...")
    df = pd.read_csv(
        INPUT,
        encoding="utf-8",
    )

    print(f"🔹 Rows before cleanup: {len(df):,}")

    # ----- PERFECT ROW DEDUP -----
    df = df.drop_duplicates()

    print(f"🔹 Rows after dedup: {len(df):,}")

    # ----- STABLE SORT -----
    df = df.sort_values(
        by=["tool_name", "snapshot_day"],
        kind="stable",
    )

    print("🔹 Writing cleaned file...")
    df.to_csv(
        OUTPUT,
        index=False,
        encoding="utf-8",
        errors="replace",
    )

    print(f"✅ Cleanup complete → {OUTPUT}")

if __name__ == "__main__":
    main()
