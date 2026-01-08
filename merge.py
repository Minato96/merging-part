#!/usr/bin/env python3

import pandas as pd
import json
from pathlib import Path
from typing import Optional
from tqdm import tqdm
import re
import time

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
    "views",
    "saves",
    "comments",
    "rating",
]

FLUSH_EVERY = 2000  # RAM-safe
LOG_EVERY_FLUSH = 1  # log every flush

# ---------- helpers ----------

def count_rows(csv_path: Path) -> int:
    with open(csv_path, "r", encoding="utf-8", errors="ignore") as f:
        return sum(1 for _ in f) - 1  # minus header

def build_pricing_text(model, paid_from, frequency):
    parts = []

    for v in (model, paid_from, frequency):
        if pd.notna(v) and v != "":
            parts.append(str(v).strip())

    return " | ".join(parts) if parts else None


def extract_release_date(versions_json):
    versions = safe_json_load(versions_json)
    dates = [
        v.get("date")
        for v in versions
        if isinstance(v, dict) and v.get("date")
    ]
    return min(dates) if dates else None


def extract_snapshot_day(url: str):
    if not isinstance(url, str):
        return None
    m = WAYBACK_RE.search(url)
    if not m:
        return None
    return m.group(1)[:8]


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
        return 0

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

    return len(df)


# ---------- main processor ----------

def process_csv(csv_path: Path, master_path: Path):
    total_rows = count_rows(csv_path)
    buffer = []

    processed = 0
    written = 0
    flush_count = 0
    start = time.time()

    print(f"\n▶ Starting merge: {csv_path.name}")
    print(f"▶ Total source rows: {total_rows:,}")
    print(f"▶ Flush threshold: {FLUSH_EVERY:,} rows\n")

    with tqdm(total=total_rows, unit="rows", desc="Processing") as pbar:
        for chunk in pd.read_csv(csv_path, chunksize=500):
            for _, r in chunk.iterrows():

                snapshot_day = extract_snapshot_day(r.get("link"))
                date_iso = iso_date(snapshot_day)

                # ---------- PRIMARY TOOL ----------
                buffer.append({
                    "tool_name": r.get("name"),
                    "snapshot_day": snapshot_day,
                    "date": date_iso,
                    "raw_classes": r.get("use_case_category") or r.get("use_case"),
                    "internal_link": r.get("link"),
                    "external_link": r.get("tool_link"),
                    "tags": r.get("tags"),
                    "pricing_text": build_pricing_text(
                                        r.get("pricing_model"),
                                        r.get("paid_options_from"),
                                        r.get("billing_frequency"),
                                    ),
                    "release_text":  extract_release_date(r.get("versions")) or r.get("use_case_created_date"),
                    "views": r.get("views"),
                    "saves": r.get("saves"),
                    "comments": r.get("comments_count"),
                    "rating": r.get("rating"),
                })

                # ---------- TOOLS JSON ----------
                tools = safe_json_load(r.get("top_alternative_json"))
                for t in tools:
                    buffer.append({
                        "tool_name": t.get("data_name"),
                        "snapshot_day": snapshot_day,
                        "date": date_iso,
                        "raw_classes": t.get("task_name"),
                        "internal_link": t.get("ai_page"),
                        "external_link": t.get("ai_page"),
                        "tags": t.get("tags"),
                        "pricing_text": t.get("price_text"),
                        "release_text": t.get("release_date"),
                        "views": t.get("views"),
                        "saves": t.get("saves"),
                        "comments": t.get("comments"),
                        "rating": t.get("average_rating"),
                    })

                processed += 1
                pbar.update(1)

                if len(buffer) >= FLUSH_EVERY:
                    rows_written = append_chunk(master_path, buffer)
                    written += rows_written
                    flush_count += 1
                    buffer.clear()

                    if flush_count % LOG_EVERY_FLUSH == 0:
                        elapsed = time.time() - start
                        print(
                            f"\n✔ Flush #{flush_count} | "
                            f"written: {written:,} rows | "
                            f"elapsed: {elapsed:.1f}s"
                        )

        # final flush
        if buffer:
            rows_written = append_chunk(master_path, buffer)
            written += rows_written
            buffer.clear()

    elapsed = time.time() - start
    print("\n✅ Merge finished")
    print(f"▶ Source rows processed : {processed:,}")
    print(f"▶ Panel rows written    : {written:,}")
    print(f"▶ Total time            : {elapsed:.1f}s")
    print(f"▶ Output file           : {master_path}\n")


# ---------- run ----------

if __name__ == "__main__":
    MASTER = Path("ai_wayback_panel_tool_day.clean.csv")

    process_csv(
        csv_path=Path("ai_wayback_async_out_2025.csv"),
        master_path=MASTER,
    )
