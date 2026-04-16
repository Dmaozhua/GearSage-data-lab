from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse


PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from shared.reel_pipeline import (
    INPUT_HEADERS,
    WORKBOOK_DEFAULT,
    clean_text,
    print_json,
    read_sheet_rows,
    stable_id,
    utc_now,
    write_multiple_sheets,
)


CREATOR_INVENTORY_HEADERS = [
    "creator_id",
    "creator_name",
    "creator_home_url",
    "video_id",
    "video_url",
    "title_detected",
    "publish_date",
    "cover_text",
    "source_rank",
    "inventory_status",
    "inventory_notes",
    "created_at",
    "updated_at",
    "queued_for_ingest",
]


def merge_note_tokens(*values: str) -> str:
    seen: list[str] = []
    for value in values:
        text = clean_text(value)
        if not text:
            continue
        for token in [part.strip() for part in text.split(";")]:
            if token and token not in seen:
                seen.append(token)
    return "; ".join(seen)

PLAYWRIGHT_NODE_MODULES = PROJECT_ROOT / ".tmp" / "playwright-runner" / "node_modules"
PLAYWRIGHT_ENUMERATOR = (
    PROJECT_ROOT / "skills" / "creator-video-inventory" / "scripts" / "playwright_creator_inventory.cjs"
)


def creator_input_url(workbook_path: Path) -> str:
    headers, rows = read_sheet_rows(workbook_path, "input_videos")
    for row in rows:
        if clean_text(row.get("creator", "")) == "小蒋钓鱼":
            return row.get("url", "")
    return rows[0].get("url", "") if rows else ""


def parse_video_id(value: str) -> str:
    text = clean_text(value)
    if not text:
        return ""
    parsed = urlparse(text)
    if "/video/" in parsed.path:
        candidate = clean_text(parsed.path.split("/video/")[1].split("/")[0])
        if candidate.isdigit():
            return candidate
    query = parse_qs(parsed.query)
    for key in ("modal_id", "aweme_id"):
        candidate = clean_text((query.get(key) or [""])[0])
        if candidate.isdigit():
            return candidate
    return ""


def canonical_video_url(video_id: str, fallback_url: str = "") -> str:
    video_id = clean_text(video_id)
    return f"https://www.douyin.com/video/{video_id}" if video_id else clean_text(fallback_url)


def input_identity(row: dict[str, str]) -> str:
    raw_url = clean_text(row.get("url", ""))
    video_id = parse_video_id(raw_url)
    if video_id:
        return f"video_id:{video_id}"
    return f"url:{raw_url}"


def input_row_rank(row: dict[str, str], index: int) -> tuple[int, int, int, int]:
    manual_notes = clean_text(row.get("manual_notes", ""))
    status = clean_text(row.get("status", ""))
    seeded_penalty = 1 if manual_notes == "seeded from creator_video_inventory" else 0
    status_penalty = 0 if status and status != "pending" else 1
    title_penalty = 0 if clean_text(row.get("title_hint", "")) else 1
    return (seeded_penalty, status_penalty, title_penalty, index)


def dedupe_input_rows(rows: list[dict[str, str]]) -> tuple[list[dict[str, str]], int]:
    selected: dict[str, tuple[tuple[int, int, int, int], dict[str, str]]] = {}
    removed = 0
    for index, row in enumerate(rows):
        identity = input_identity(row)
        rank = input_row_rank(row, index)
        existing = selected.get(identity)
        if existing is None or rank < existing[0]:
            if existing is not None:
                removed += 1
            selected[identity] = (rank, row)
        else:
            removed += 1
    deduped = [item[1] for item in sorted(selected.values(), key=lambda item: item[0][3])]
    return deduped, removed


def run_playwright_inventory(
    *,
    url: str,
    cookies_file: str = "",
    scroll_rounds: int = 12,
    wait_ms: int = 1800,
    category_title: str = "",
    category_index: int | None = None,
) -> dict[str, Any]:
    output_json = PROJECT_ROOT / "data" / "cache" / "creator_inventory_latest.json"
    output_json.parent.mkdir(parents=True, exist_ok=True)
    if output_json.exists():
        output_json.unlink()
    env = os.environ.copy()
    env["NODE_PATH"] = str(PLAYWRIGHT_NODE_MODULES)
    command = [
        "node",
        str(PLAYWRIGHT_ENUMERATOR),
        "--url",
        url,
        "--output-json",
        str(output_json),
        "--scroll-rounds",
        str(scroll_rounds),
        "--wait-ms",
        str(wait_ms),
    ]
    if cookies_file:
        command.extend(["--cookies-file", cookies_file])
    if clean_text(category_title):
        command.extend(["--category-title", clean_text(category_title)])
    if category_index is not None:
        command.extend(["--category-index", str(category_index)])
    process = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
        timeout=240,
    )
    if output_json.exists():
        return json.loads(output_json.read_text(encoding="utf-8"))
    if process.stdout.strip():
        return json.loads(process.stdout)
    return {
        "input_url": url,
        "final_url": "",
        "creator_home_url": "",
        "creator_id": "",
        "creator_name": "",
        "discovered_count": 0,
        "deduped_count": 0,
        "notes": [clean_text(process.stderr) or "enumeration returned no structured output"],
        "records": [],
    }


def merge_creator_inventory(
    workbook_path: Path,
    result: dict[str, Any],
    *,
    append_to_input: bool,
) -> dict[str, Any]:
    _, inventory_rows = read_sheet_rows(workbook_path, "creator_video_inventory")
    input_headers, input_rows = read_sheet_rows(workbook_path, "input_videos")

    inventory_by_video: dict[str, dict[str, str]] = {}
    for row in inventory_rows:
        key = row.get("video_id", "") or row.get("video_url", "")
        if key:
            inventory_by_video[key] = row

    input_urls = set()
    input_video_ids = set()
    input_canonical_urls = set()
    for row in input_rows:
        raw_url = clean_text(row.get("url", ""))
        if not raw_url:
            continue
        input_urls.add(raw_url)
        existing_video_id = parse_video_id(raw_url)
        if existing_video_id:
            input_video_ids.add(existing_video_id)
            input_canonical_urls.add(canonical_video_url(existing_video_id))

    now = utc_now()
    appended_count = 0
    written_count = 0
    summary_creator_name = clean_text(result.get("creator_name", ""))
    summary_creator_home_url = clean_text(result.get("creator_home_url", ""))
    source_bucket = clean_text(result.get("source_bucket", ""))
    input_count_before = len(input_rows)
    for record in result.get("records", []):
        video_id = clean_text(record.get("video_id", ""))
        raw_video_url = clean_text(record.get("video_url", ""))
        video_url = canonical_video_url(video_id, raw_video_url)
        if not video_id and not video_url:
            continue
        key = video_id or video_url
        existing = inventory_by_video.get(key, {})
        queued_for_ingest = existing.get("queued_for_ingest", "")
        already_exists = (
            (video_id and video_id in input_video_ids)
            or (video_url and video_url in input_canonical_urls)
            or (video_url and video_url in input_urls)
            or (raw_video_url and raw_video_url in input_urls)
        )
        if append_to_input and video_url and not already_exists:
            task_id = stable_id("rvp", result.get("creator_id", ""), video_id or video_url)
            input_rows.append(
                {
                    "task_id": task_id,
                    "platform": "douiyin",
                    "url": video_url,
                    "creator": clean_text(record.get("creator_name", "") or result.get("creator_name", "")),
                    "title_hint": clean_text(record.get("title_detected", "")),
                    "content_type": "mixed",
                    "priority": "",
                    "status": "pending",
                    "language": "",
                    "target_field_scope": "",
                    "manual_notes": merge_note_tokens(
                        "seeded from creator_video_inventory",
                        f"source_bucket={source_bucket}" if source_bucket else "",
                    ),
                    "created_at": now,
                    "updated_at": now,
                }
            )
            input_urls.add(video_url)
            if raw_video_url:
                input_urls.add(raw_video_url)
            if video_id:
                input_video_ids.add(video_id)
                input_canonical_urls.add(video_url)
            queued_for_ingest = "yes"
            appended_count += 1
        summary_creator_name = summary_creator_name or clean_text(record.get("creator_name", ""))
        summary_creator_home_url = summary_creator_home_url or clean_text(record.get("creator_home_url", ""))
        merged = {
            "creator_id": clean_text(record.get("creator_id", "") or result.get("creator_id", "")),
            "creator_name": clean_text(record.get("creator_name", "") or result.get("creator_name", "")),
            "creator_home_url": clean_text(record.get("creator_home_url", "") or result.get("creator_home_url", "")),
            "video_id": video_id,
            "video_url": video_url,
            "title_detected": clean_text(record.get("title_detected", "")),
            "publish_date": clean_text(record.get("publish_date", "")),
            "cover_text": clean_text(record.get("cover_text", "")),
            "source_rank": clean_text(record.get("source_rank", "")),
            "inventory_status": clean_text(record.get("inventory_status", "")) or "discovered",
            "inventory_notes": merge_note_tokens(
                record.get("inventory_notes", ""),
                f"source_bucket={source_bucket}" if source_bucket else "",
            ),
            "created_at": existing.get("created_at", "") or now,
            "updated_at": now,
            "queued_for_ingest": queued_for_ingest,
        }
        inventory_by_video[key] = merged
        written_count += 1

    deduped_input_rows, removed_duplicates = dedupe_input_rows(input_rows)

    write_multiple_sheets(
        workbook_path,
        {
            "creator_video_inventory": (
                CREATOR_INVENTORY_HEADERS,
                sorted(
                    inventory_by_video.values(),
                    key=lambda row: (
                        row.get("creator_name", ""),
                        row.get("video_id", ""),
                        row.get("video_url", ""),
                    ),
                ),
            ),
            "input_videos": (input_headers or INPUT_HEADERS, deduped_input_rows),
        },
    )

    return {
        "creator_home_url": summary_creator_home_url or result.get("creator_home_url", ""),
        "creator_id": result.get("creator_id", ""),
        "creator_name": summary_creator_name or result.get("creator_name", ""),
        "source_bucket": source_bucket,
        "selection_mode": clean_text(result.get("selection_mode", "")),
        "available_tabs": result.get("available_tabs", []),
        "discovered_count": result.get("discovered_count", 0),
        "deduped_count": result.get("deduped_count", 0),
        "written_inventory_count": written_count,
        "appended_input_videos_count": len(deduped_input_rows) - input_count_before,
        "raw_appended_input_videos_count": appended_count,
        "input_videos_removed_as_duplicates": removed_duplicates,
        "input_videos_total_after": len(deduped_input_rows),
        "notes": result.get("notes", []),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Enumerate one Douyin creator homepage and write creator_video_inventory.")
    parser.add_argument("--workbook", default=str(WORKBOOK_DEFAULT))
    parser.add_argument("--creator-url", default="", help="Creator homepage or creator-scoped Douyin page.")
    parser.add_argument("--cookies-file", default="", help="Optional Netscape cookies file passed to Playwright.")
    parser.add_argument("--scroll-rounds", type=int, default=12)
    parser.add_argument("--wait-ms", type=int, default=1800)
    parser.add_argument("--category-title", default="", help="Optional visible category/tab title to click before enumeration.")
    parser.add_argument("--category-index", type=int, default=None, help="Optional visible category/tab index to click before enumeration.")
    parser.add_argument("--append-to-input", action="store_true", help="Append unseen video urls into input_videos.")
    args = parser.parse_args()

    workbook_path = Path(args.workbook).resolve()
    creator_url = clean_text(args.creator_url) or clean_text(creator_input_url(workbook_path))
    if not creator_url:
        print_json({"error": "No creator url available"})
        return 1

    result = run_playwright_inventory(
        url=creator_url,
        cookies_file=args.cookies_file,
        scroll_rounds=args.scroll_rounds,
        wait_ms=args.wait_ms,
        category_title=args.category_title,
        category_index=args.category_index,
    )
    summary = merge_creator_inventory(
        workbook_path,
        result,
        append_to_input=args.append_to_input,
    )
    summary["input_url"] = creator_url
    print_json(summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
