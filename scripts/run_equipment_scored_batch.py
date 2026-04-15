from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_ROOT = Path(__file__).resolve().parent
for root in (PROJECT_ROOT, SCRIPTS_ROOT):
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

from equipment_scoring import load_scored_inventory_candidates
from run_pending_batch import run_batch_for_task_ids
from shared.reel_pipeline import WORKBOOK_DEFAULT, print_json


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a batch biased toward parameter-explicit creator inventory candidates.")
    parser.add_argument("--workbook", default=str(WORKBOOK_DEFAULT))
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--min-equipment-score", type=int, default=10)
    parser.add_argument("--cookies-file", default="")
    parser.add_argument("--cookies-from-browser", default="")
    parser.add_argument("--timeout-seconds", type=int, default=90)
    parser.add_argument("--page-fallback", action="store_true")
    parser.add_argument("--ocr-fallback", action="store_true")
    args = parser.parse_args()

    workbook_path = Path(args.workbook).resolve()
    scored_rows = load_scored_inventory_candidates(workbook_path, pending_only=True)
    selected_rows = [
        row for row in scored_rows if int(row.get("equipment_score", "0") or "0") >= args.min_equipment_score
    ][: args.limit]
    if not selected_rows:
        print_json({"error": "No scored pending tasks available"})
        return 1

    batch_summary = run_batch_for_task_ids(
        workbook_path,
        selected_rows,
        cookies_file=args.cookies_file,
        cookies_from_browser=args.cookies_from_browser,
        timeout_seconds=args.timeout_seconds,
        page_fallback=args.page_fallback,
        ocr_fallback=args.ocr_fallback,
    )
    batch_summary["selection_mode"] = "parameter_score"
    batch_summary["selected_limit"] = args.limit
    batch_summary["min_equipment_score"] = args.min_equipment_score
    batch_summary["selected_tasks"] = [
        {
            "task_id": row.get("task_id", ""),
            "video_url": row.get("video_url", ""),
            "title_detected": row.get("title_detected", ""),
            "publish_date": row.get("publish_date", ""),
            "equipment_score": row.get("equipment_score", ""),
            "parameter_score": row.get("parameter_score", ""),
            "matched_keywords": row.get("matched_keywords", ""),
        }
        for row in selected_rows
    ]
    print_json(batch_summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
