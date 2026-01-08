#!/usr/bin/env python3

import pandas as pd
import json
from pathlib import Path
from typing import List, Optional
from tqdm import tqdm
import re

WAYBACK_RE = re.compile(r"/web/(\d{14})/")

FINAL_COLUMNS = [
    "tool_name",
    "snapshot_day",
    "date",
    "raw_classes",
    "internal_link",
    "external_link",
    "tags",
    "pricing_text",
    "release_text",
    "saves",
    "comments",
    "rating",
]

FLUSH_EVERY = 2000  # RAM-safe

# ---------- helpers ----------

def extract_snapshot_day(url: str):
    if not isinstance(url, str):
        return None
    m = WAYBACK_RE.search(url)
    if not m:
        return None
    return m.group(1)[:8]  # YYYYMMDD


def iso_date(day: Optional[str]):
    if not day:
        return None
    return f"{day[:4]}-{day[4:6]}-{day[6:]}"


def safe_json_load(x):
    if pd.isna(x) or x in ("", "[]"):
        return []
    try:
        return json.loads(x)
    except Exception:
        return []


def append_chunk(master_path: Path, rows: list):
    if not rows:
        return

    df = pd.DataFrame(rows, columns=FINAL_COLUMNS)

    write_header = not master_path.exists() or master_path.stat().st_size == 0

    df.to_csv(
        master_path,
        mode="a",
        header=write_header,
        index=False,
        encoding="utf-8",
        errors="replace",
    )


# ---------- main processor ----------

def process_csv(
    csv_path: Path,
    master_path: Path,
):
    buffer = []

    for chunk in pd.read_csv(csv_path, chunksize=500):
        for _, r in chunk.iterrows():

            snapshot_day = extract_snapshot_day(r.get("link"))
            date_iso = iso_date(snapshot_day)

            # ---------- PRIMARY TOOL ----------
            buffer.append({
                "tool_name": r.get("use_case_name"),
                "snapshot_day": snapshot_day,
                "date": date_iso,
                "raw_classes": r.get("use_case_category"),
                "internal_link": r.get("link"),
                "external_link": r.get("tool_link"),
                "tags": r.get("tags"),
                "pricing_text": r.get("tag_price"),
                "release_text": r.get("use_case_created_date"),
                "saves": None,
                "comments": None,
                "rating": None,
            })

            # ---------- LISTINGS JSON ----------
            tools = safe_json_load(r.get("listings_json"))
            for t in tools:
                buffer.append({
                    "tool_name": t.get("name"),
                    "snapshot_day": snapshot_day,
                    "date": date_iso,
                    "raw_classes": t.get("task_name"),
                    "internal_link": t.get("internal_link"),
                    "external_link": t.get("external_link"),
                    "tags": t.get("tags"),
                    "pricing_text": t.get("pricing_text"),
                    "release_text": t.get("release_date"),
                    "saves": t.get("saves"),
                    "comments": t.get("comments"),
                    "rating": t.get("rating"),
                })

            if len(buffer) >= FLUSH_EVERY:
                append_chunk(master_path, buffer)
                buffer.clear()

    append_chunk(master_path, buffer)


# ---------- run ----------

if __name__ == "__main__":
    MASTER = Path("ai_wayback_panel_tool_day.csv")

    process_csv(
        csv_path=Path("ai_wayback_async_out_2023.csv"),
        master_path=MASTER,
    )

    print(f"✅ Merge complete → {MASTER}")
