#!/usr/bin/env python3

import pandas as pd
import json
from pathlib import Path
from typing import List, Optional

FINAL_COLUMNS = [
    "tool_name",
    "views",
    "saves",
    "rating",
    "price_text",
    "date",
]


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


def process_csv(
    csv_path: Path,
    primary_tool_col: str,
    date_col: str,
    json_cols: List[str],
) -> pd.DataFrame:
    df = pd.read_csv(csv_path)

    rows = []

    for _, r in df.iterrows():
        date = r.get(date_col)

        # ---------- PRIMARY TOOL ----------
        rows.append({
            "tool_name": r.get(primary_tool_col),
            "views": r.get("views"),
            "saves": r.get("saves"),
            "rating": r.get("rating"),
            "price_text": r.get("price_text"),
            "date": date,
        })

        # ---------- JSON TOOLS ----------
        for jc in json_cols:
            if jc not in df.columns:
                continue

            tools = safe_json_load(r.get(jc))
            for t in tools:
                price_text = combine_price(
                    t.get("price_label"),
                    t.get("pricing_text"),
                )

                rows.append({
                    "tool_name": t.get("name"),
                    "views": None,
                    "saves": t.get("saves"),
                    "rating": t.get("rating"),
                    "price_text": price_text,
                    "date": date,
                })

    out = pd.DataFrame(rows, columns=FINAL_COLUMNS)
    return out


def append_to_master(master_path: Path, new_df: pd.DataFrame):
    if master_path.exists():
        master = pd.read_csv(master_path)
        master = pd.concat([master, new_df], ignore_index=True)
    else:
        master = new_df

    master.to_csv(master_path, index=False)


# ----------------- USAGE EXAMPLE -----------------
if __name__ == "__main__":
    MASTER = Path("merged_tools.csv")

    df_out = process_csv(
        csv_path=Path("input.csv"),
        primary_tool_col="use_case_category",   # example: H2O AI
        date_col="use_case_created_date",
        json_cols=["listings_json", "also_searched_json"],
    )

    append_to_master(MASTER, df_out)
    print(f"Merged data appended to {MASTER}")