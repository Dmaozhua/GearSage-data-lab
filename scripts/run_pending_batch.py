from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from urllib.parse import parse_qs, urlparse


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from shared.reel_pipeline import (
    WORKBOOK_DEFAULT,
    clean_text,
    print_json,
    read_sheet_rows,
    run_extract,
    run_ingest,
    run_review_queue,
)


HIGH_VALUE_FIELDS = [
    "spool_diameter_mm",
    "body_material",
    "main_gear_material",
    "minor_gear_material",
    "spool_axis_type",
]

PARAMETER_SIGNAL_PATTERNS = [
    "mm",
    "材质",
    "黄铜",
    "铝合金",
    "长轴",
    "短轴",
    "直径",
    "线杯",
    "主齿",
    "小齿",
]


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


def load_recent_pending_tasks(workbook_path: Path, limit: int) -> list[dict[str, str]]:
    _, input_rows = read_sheet_rows(workbook_path, "input_videos")
    _, inventory_rows = read_sheet_rows(workbook_path, "creator_video_inventory")
    inventory_by_video_id = {
        clean_text(row.get("video_id", "")): row
        for row in inventory_rows
        if clean_text(row.get("video_id", ""))
    }
    pending_rows = []
    for row in input_rows:
        if clean_text(row.get("status", "")).lower() != "pending":
            continue
        video_id = parse_video_id(row.get("url", ""))
        inventory = inventory_by_video_id.get(video_id, {})
        sort_date = clean_text(inventory.get("publish_date", ""))
        pending_rows.append(
            {
                **row,
                "_video_id": video_id,
                "_publish_date": sort_date,
            }
        )
    pending_rows.sort(
        key=lambda row: (
            row.get("_publish_date", ""),
            row.get("updated_at", ""),
            row.get("created_at", ""),
            row.get("task_id", ""),
        ),
        reverse=True,
    )
    return pending_rows[:limit]


def classify_ingest_status(row: dict[str, str]) -> str:
    ingest_status = clean_text(row.get("ingest_status", ""))
    if ingest_status == "asset_fetch_failed":
        return "failed"
    if any(clean_text(row.get(field_name, "")) for field_name in ("description_text", "subtitle_text", "transcript_text", "page_text")):
        return "success"
    return "partial"


def count_ocr_hits(raw_rows: list[dict[str, str]]) -> int:
    count = 0
    for row in raw_rows:
        notes = clean_text(row.get("ingest_notes", ""))
        if "ocr:extracted=yes" in notes:
            count += 1
    return count


def estimate_parameter_dense_count(raw_rows: list[dict[str, str]], extract_rows: list[dict[str, str]]) -> int:
    tasks_with_high_value = {row.get("task_id", "") for row in extract_rows if clean_text(row.get("field_name", "")) in HIGH_VALUE_FIELDS}
    if tasks_with_high_value:
        return len(tasks_with_high_value)
    count = 0
    for row in raw_rows:
        text = " ".join(
            clean_text(row.get(field_name, ""))
            for field_name in ("title_detected", "description_text", "page_text", "subtitle_text", "transcript_text")
        ).lower()
        if any(pattern.lower() in text for pattern in PARAMETER_SIGNAL_PATTERNS):
            count += 1
    return count


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a minimal pending-video batch through ingest, extract, and review queue.")
    parser.add_argument("--workbook", default=str(WORKBOOK_DEFAULT))
    parser.add_argument("--limit", type=int, default=30)
    parser.add_argument("--cookies-file", default="")
    parser.add_argument("--cookies-from-browser", default="")
    parser.add_argument("--timeout-seconds", type=int, default=90)
    parser.add_argument("--page-fallback", action="store_true")
    parser.add_argument("--ocr-fallback", action="store_true")
    args = parser.parse_args()

    workbook_path = Path(args.workbook).resolve()
    selected_rows = load_recent_pending_tasks(workbook_path, args.limit)
    selected_task_ids = [clean_text(row.get("task_id", "")) for row in selected_rows if clean_text(row.get("task_id", ""))]
    selected_task_id_set = set(selected_task_ids)
    if not selected_task_ids:
        print_json({"error": "No pending tasks available for batch"})
        return 1

    _, raw_before = read_sheet_rows(workbook_path, "raw_ingest")
    _, extract_before = read_sheet_rows(workbook_path, "player_data_extract")
    _, review_before = read_sheet_rows(workbook_path, "review_queue")

    ingest_result = run_ingest(
        workbook_path,
        cookies_file=args.cookies_file,
        cookies_from_browser=args.cookies_from_browser,
        timeout_seconds=args.timeout_seconds,
        page_fallback=args.page_fallback,
        ocr_fallback=args.ocr_fallback,
        task_ids=selected_task_id_set,
    )
    extract_result = run_extract(workbook_path, task_ids=selected_task_id_set)
    review_result = run_review_queue(workbook_path, task_ids=selected_task_id_set)

    _, raw_after = read_sheet_rows(workbook_path, "raw_ingest")
    _, extract_after = read_sheet_rows(workbook_path, "player_data_extract")
    _, review_after = read_sheet_rows(workbook_path, "review_queue")

    selected_raw_rows = [row for row in raw_after if clean_text(row.get("task_id", "")) in selected_task_id_set]
    selected_extract_rows = [row for row in extract_after if clean_text(row.get("task_id", "")) in selected_task_id_set]
    selected_review_rows = [row for row in review_after if clean_text(row.get("task_id", "")) in selected_task_id_set]

    ingest_bucket = {"success": 0, "partial": 0, "failed": 0}
    for row in selected_raw_rows:
        ingest_bucket[classify_ingest_status(row)] += 1

    high_value_counts = {
        field_name: sum(1 for row in selected_extract_rows if clean_text(row.get("field_name", "")) == field_name)
        for field_name in HIGH_VALUE_FIELDS
    }
    parameter_dense_count = estimate_parameter_dense_count(selected_raw_rows, selected_extract_rows)

    summary = {
        "selected_limit": args.limit,
        "selected_task_count": len(selected_task_ids),
        "selected_tasks": [
            {
                "task_id": row.get("task_id", ""),
                "publish_date": row.get("_publish_date", ""),
                "url": row.get("url", ""),
                "title_hint": row.get("title_hint", ""),
            }
            for row in selected_rows
        ],
        "ingest": {
            "processed_count": ingest_result.get("processed_count", 0),
            "success_count": ingest_bucket["success"],
            "partial_count": ingest_bucket["partial"],
            "failed_count": ingest_bucket["failed"],
            "description_present_count": sum(1 for row in selected_raw_rows if clean_text(row.get("description_text", ""))),
            "page_text_present_count": sum(1 for row in selected_raw_rows if clean_text(row.get("page_text", ""))),
            "ocr_present_count": count_ocr_hits(selected_raw_rows),
        },
        "extract": {
            "batch_extract_count": extract_result.get("extract_count", 0),
            "new_rows_added": max(0, len(extract_after) - len(extract_before)),
            "selected_task_count": len({row.get("task_id", "") for row in selected_extract_rows}),
            "high_value_field_hits": high_value_counts,
        },
        "review_queue": {
            "batch_review_count": review_result.get("review_count", 0),
            "new_rows_added": max(0, len(review_after) - len(review_before)),
            "selected_task_count": len({row.get("task_id", "") for row in selected_review_rows}),
        },
        "parameter_dense_video_estimate": {
            "count": parameter_dense_count,
            "ratio": round(parameter_dense_count / len(selected_task_ids), 3),
            "method": "task has at least one high-value extract hit, otherwise falls back to parameter-signal text heuristic",
        },
        "throughput": {
            "batch_completed_without_abort": True,
            "raw_rows_before": len(raw_before),
            "raw_rows_after": len(raw_after),
            "extract_rows_before": len(extract_before),
            "extract_rows_after": len(extract_after),
            "review_rows_before": len(review_before),
            "review_rows_after": len(review_after),
        },
    }
    print_json(summary)
    return 0


def run_batch_for_task_ids(
    workbook_path: Path,
    selected_rows: list[dict[str, str]],
    *,
    cookies_file: str = "",
    cookies_from_browser: str = "",
    timeout_seconds: int = 90,
    page_fallback: bool = False,
    ocr_fallback: bool = False,
) -> dict[str, object]:
    selected_task_ids = [clean_text(row.get("task_id", "")) for row in selected_rows if clean_text(row.get("task_id", ""))]
    selected_task_id_set = set(selected_task_ids)
    if not selected_task_ids:
        return {"error": "No task_ids provided"}

    _, raw_before = read_sheet_rows(workbook_path, "raw_ingest")
    _, extract_before = read_sheet_rows(workbook_path, "player_data_extract")
    _, review_before = read_sheet_rows(workbook_path, "review_queue")

    ingest_result = run_ingest(
        workbook_path,
        cookies_file=cookies_file,
        cookies_from_browser=cookies_from_browser,
        timeout_seconds=timeout_seconds,
        page_fallback=page_fallback,
        ocr_fallback=ocr_fallback,
        task_ids=selected_task_id_set,
    )
    extract_result = run_extract(workbook_path, task_ids=selected_task_id_set)
    review_result = run_review_queue(workbook_path, task_ids=selected_task_id_set)

    _, raw_after = read_sheet_rows(workbook_path, "raw_ingest")
    _, extract_after = read_sheet_rows(workbook_path, "player_data_extract")
    _, review_after = read_sheet_rows(workbook_path, "review_queue")

    selected_raw_rows = [row for row in raw_after if clean_text(row.get("task_id", "")) in selected_task_id_set]
    selected_extract_rows = [row for row in extract_after if clean_text(row.get("task_id", "")) in selected_task_id_set]
    selected_review_rows = [row for row in review_after if clean_text(row.get("task_id", "")) in selected_task_id_set]

    ingest_bucket = {"success": 0, "partial": 0, "failed": 0}
    for row in selected_raw_rows:
        ingest_bucket[classify_ingest_status(row)] += 1

    high_value_counts = {
        field_name: sum(1 for row in selected_extract_rows if clean_text(row.get("field_name", "")) == field_name)
        for field_name in HIGH_VALUE_FIELDS
    }
    parameter_dense_count = estimate_parameter_dense_count(selected_raw_rows, selected_extract_rows)

    return {
        "selected_task_count": len(selected_task_ids),
        "selected_tasks": selected_rows,
        "ingest": {
            "processed_count": ingest_result.get("processed_count", 0),
            "success_count": ingest_bucket["success"],
            "partial_count": ingest_bucket["partial"],
            "failed_count": ingest_bucket["failed"],
            "description_present_count": sum(1 for row in selected_raw_rows if clean_text(row.get("description_text", ""))),
            "page_text_present_count": sum(1 for row in selected_raw_rows if clean_text(row.get("page_text", ""))),
            "ocr_present_count": count_ocr_hits(selected_raw_rows),
        },
        "extract": {
            "batch_extract_count": extract_result.get("extract_count", 0),
            "new_rows_added": max(0, len(extract_after) - len(extract_before)),
            "selected_task_count": len({row.get("task_id", "") for row in selected_extract_rows}),
            "high_value_field_hits": high_value_counts,
        },
        "review_queue": {
            "batch_review_count": review_result.get("review_count", 0),
            "new_rows_added": max(0, len(review_after) - len(review_before)),
            "selected_task_count": len({row.get("task_id", "") for row in selected_review_rows}),
        },
        "parameter_dense_video_estimate": {
            "count": parameter_dense_count,
            "ratio": round(parameter_dense_count / len(selected_task_ids), 3),
            "method": "task has at least one high-value extract hit, otherwise falls back to parameter-signal text heuristic",
        },
        "throughput": {
            "batch_completed_without_abort": True,
            "raw_rows_before": len(raw_before),
            "raw_rows_after": len(raw_after),
            "extract_rows_before": len(extract_before),
            "extract_rows_after": len(extract_after),
            "review_rows_before": len(review_before),
            "review_rows_after": len(review_after),
        },
    }


if __name__ == "__main__":
    raise SystemExit(main())
