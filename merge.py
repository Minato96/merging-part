#!/usr/bin/env python3

import pandas as pd
import json
import re
from pathlib import Path
from tqdm import tqdm
from urllib.parse import urlparse, urlunparse

WAYBACK_RE = re.compile(r"/web/\d{14}/")

FINAL_COLUMNS = [
    "tool_id",
    "tool_name",
    "snapshot_day",
    "date",
    "release_date",
    "internal_link",
    "external_link",
    "pricing_text",
    "views",
    "saves",
    "comments_count",
    "rating",
    "source",
]


# ----------------------------
# Helpers
# ----------------------------
WAYBACK_ANY_RE = re.compile(r"/web/\d{14}/(https?://.+)")

def unwrap_wayback(url: str | None) -> str | None:
    if not isinstance(url, str):
        return None

    m = WAYBACK_ANY_RE.search(url)
    if m:
        return m.group(1)

    return url

WAYBACK_TS_RE = re.compile(r"/web/(\d{14})/")

def extract_snapshot_from_url(*urls):
    for url in urls:
        if not isinstance(url, str):
            continue
        m = WAYBACK_TS_RE.search(url)
        if m:
            ts = m.group(1)
            snapshot_day = ts[:8]
            date = pd.to_datetime(snapshot_day, format="%Y%m%d").date().isoformat()
            return snapshot_day, date
    return None, None

def extract_comments_count(row):
    for key in ("comments_count", "number_of_comments"):
        val = row.get(key)
        if pd.notna(val):
            return val
    return None

def normalize_url(url: str | None) -> str | None:
    url = unwrap_wayback(url)
    if not isinstance(url, str) or not url.strip():
        return None

    p = urlparse(url)

    return urlunparse(
        (
            p.scheme.lower(),
            p.netloc.lower(),
            p.path.rstrip("/"),  # KEEP path
            "",                  # params
            "",                  # query REMOVED
            "",                  # fragment
        )
    )

def compute_tool_id(internal_link, external_link):
    internal = normalize_url(internal_link)
    if internal:
        return internal
    return normalize_url(external_link)



def safe_int(v):
    try:
        if pd.isna(v):
            return None
        return int(str(v).replace(",", "").strip())
    except Exception:
        return None


def safe_float(v):
    try:
        if pd.isna(v):
            return None
        return float(v)
    except Exception:
        return None


def parse_json(v):
    if not isinstance(v, str) or not v.strip():
        return []
    try:
        return json.loads(v)
    except Exception:
        return []


def extract_release_date(versions_json):
    dates = []
    for item in parse_json(versions_json):
        d = item.get("date")
        if isinstance(d, str):
            try:
                dates.append(pd.to_datetime(d))
            except Exception:
                pass
    return min(dates).date().isoformat() if dates else None


# ----------------------------
# Row builder
# ----------------------------

def build_row(
    *,
    tool_name,
    internal_link,
    external_link,
    pricing_text,
    views,
    saves,
    comments_count,
    rating,
    versions,
    snapshot_day,
    date,
    source,
):
    tool_id = compute_tool_id(internal_link, external_link)
    if not tool_id:
        return None

    return {
        "tool_id": tool_id,
        "tool_name": tool_name,
        "snapshot_day": snapshot_day,
        "date": date,
        "release_date": extract_release_date(versions),
        "internal_link": normalize_url(internal_link),
        "external_link": normalize_url(external_link),
        "pricing_text": pricing_text if isinstance(pricing_text, str) else None,
        "views": safe_int(views),
        "saves": safe_int(saves),
        "comments_count": safe_int(comments_count),
        "rating": safe_float(rating),
        "source": source,
    }


# ----------------------------
# Main processor
# ----------------------------

def process_csv(path: Path, source: str) -> list[dict]:
    df = pd.read_csv(path)
    out = []

    for _, r in tqdm(df.iterrows(), total=len(df), desc=path.name):
        snapshot_day, date = extract_snapshot_from_url(
                r.get("link"),
                r.get("internal_link"),
                r.get("tool_link"),
                r.get("external_link"),
            )

        # Main tool row
        row = build_row(
            tool_name=r.get("name") or r.get("tool_name"),
            internal_link=r.get("link") or r.get("internal_link"),
            external_link=r.get("tool_link") or r.get("external_link"),
            pricing_text=r.get("pricing_model") or r.get("price_text") or r.get("pricing_text"),
            views=r.get("views"),
            saves=r.get("saves"),
            comments_count=extract_comments_count(r),
            rating=r.get("rating"),
            versions=r.get("versions"),
            snapshot_day=snapshot_day,
            date=date,
            source=source,
        )

        if row:
            out.append(row)

        # Expand alternatives
        for col in ("top_alternative_json", "featured_items_json"):
            for item in parse_json(r.get(col)):
                alt = build_row(
                    tool_name=item.get("name") or item.get("data_name"),
                    internal_link=item.get("ai_page"),
                    external_link=item.get("external_url") or item.get("data_url"),
                    pricing_text=item.get("pricing") or item.get("price_text"),
                    views=item.get("views"),
                    saves=item.get("saves"),
                    comments_count=None,
                    rating=item.get("rating"),
                    versions=None,
                    snapshot_day=snapshot_day,
                    date=date,
                    source=source,
                )
                if alt:
                    out.append(alt)

    return out


# ----------------------------
# Runner
# ----------------------------

def build_panel(csv_inputs: dict[str, str], output_path: Path):
    all_rows = []

    for csv_path, source in csv_inputs.items():
        all_rows.extend(process_csv(Path(csv_path), source))

    df = pd.DataFrame(all_rows)

    # Deduplicate: tool × snapshot
    df = (
        df.sort_values(
            by=["views", "saves", "rating"],
            ascending=False,
            na_position="last",
        )
        .drop_duplicates(subset=["tool_id", "snapshot_day"], keep="first")
    )

    df = df[FINAL_COLUMNS]
    df.to_csv(output_path, index=False)


if __name__ == "__main__":
    CSV_INPUTS = {
        "ai_wayback_async_out_2025.csv": "2025",
    }

    build_panel(CSV_INPUTS, Path("final_panel_data.csv"))
