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
    "views",
    "saves",
    "rating",
    "price_text",
    "date",
]

FLUSH_EVERY = 1000  # rows

def extract_snapshot_date(url: str):
    if not isinstance(url, str):
        return None
    m = WAYBACK_RE.search(url)
    if not m:
        return None
    return m.group(1)[:8]  # YYYYMMDD

def safe_json_load(x):
    if pd.isna(x) or x == "" or x == "[]":
        return []
    try:
        return json.loads(x)
    except Exception:
        return []


def combine_price(price_label: Optional[str], pricing_text: Optional[str]) -> Optional[str]:
    parts = []
    if price_label:
        parts.append(str(price_label).strip())
    if pricing_text:
        parts.append(str(pricing_text).strip())
    return " + ".join(parts) if parts else None


def append_chunk(master_path: Path, chunk: list):
    if not chunk:
        return

    new_df = pd.DataFrame(chunk, columns=FINAL_COLUMNS)

    if master_path.exists() and master_path.stat().st_size > 0:
        master = pd.read_csv(master_path, encoding='latin1')
        master = pd.concat([master, new_df], ignore_index=True)
    else:
        master = new_df

    # full-row dedup + sort
    master = master.drop_duplicates()
    master = master.sort_values(by="tool_name", kind="stable")

    master.to_csv(master_path, index=False)


def process_csv(
    csv_path: Path,
    primary_tool_col: str,
    link_col: str,
    json_cols: List[str],
    master_path: Path,
):
    df = pd.read_csv(csv_path)
    buffer = []

    for _, r in tqdm(df.iterrows(), total=len(df), desc=csv_path.name):
        date = extract_snapshot_date(r.get(link_col))

        # ---------- PRIMARY TOOL ----------
        buffer.append({
            "tool_name": r.get(primary_tool_col),
            "views": r.get("views"),
            "saves": r.get("saves"),
            "rating": r.get("rating"),
            "price_text": r.get("tag_price"),
            "date": date,
        })

        # ---------- JSON TOOLS ----------
        for jc in json_cols:
            if jc not in df.columns:
                continue

            tools = safe_json_load(r.get(jc))
            for t in tools:
                buffer.append({
                    "tool_name": t.get("name"),
                    "views": None,
                    "saves": t.get("saves"),
                    "rating": t.get("rating"),
                    "price_text": combine_price(
                        t.get("price_label"),
                        t.get("pricing_text"),
                    ),
                    "date": date,
                })

        # ---------- FLUSH ----------
        if len(buffer) >= FLUSH_EVERY:
            append_chunk(master_path, buffer)
            buffer.clear()

    # final flush
    append_chunk(master_path, buffer)


# ----------------- USAGE -----------------
if __name__ == "__main__":
    MASTER = Path("2025_featured_extracted_ai_data_sorted.csv")

    process_csv(
        csv_path=Path("ai_wayback_async_out_2023.csv"),
        primary_tool_col="use_case_category",
        link_col="link",
        json_cols=["listings_json"],
        master_path=MASTER,
    )

    print(f"Merge complete → {MASTER}")
