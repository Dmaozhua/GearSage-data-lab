from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_ROOT = Path(__file__).resolve().parent
for root in (PROJECT_ROOT, SCRIPTS_ROOT):
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

from equipment_scoring import load_scored_inventory_candidates, score_probe_text
from run_pending_batch import HIGH_VALUE_FIELDS, classify_ingest_status, count_ocr_hits
from shared.reel_pipeline import (
    WORKBOOK_DEFAULT,
    clean_text,
    print_json,
    read_sheet_rows,
    run_extract,
    run_ingest,
    run_review_queue,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Probe a hard-param-biased batch before full extract.")
    parser.add_argument("--workbook", default=str(WORKBOOK_DEFAULT))
    parser.add_argument("--probe-limit", type=int, default=20)
    parser.add_argument("--final-limit", type=int, default=10)
    parser.add_argument("--min-equipment-score", type=int, default=8)
    parser.add_argument("--cookies-file", default="")
    parser.add_argument("--cookies-from-browser", default="")
    parser.add_argument("--timeout-seconds", type=int, default=90)
    parser.add_argument("--page-fallback", action="store_true")
    parser.add_argument("--ocr-fallback", action="store_true")
    args = parser.parse_args()

    workbook_path = Path(args.workbook).resolve()
    scored_rows = load_scored_inventory_candidates(workbook_path, pending_only=True)
    probe_seed_rows = [
        row for row in scored_rows if int(row.get("equipment_score", "0") or "0") >= args.min_equipment_score
    ][: args.probe_limit]
    if not probe_seed_rows:
        print_json({"error": "No pending hard-param candidates available for probe"})
        return 1

    probe_task_ids = {clean_text(row.get("task_id", "")) for row in probe_seed_rows if clean_text(row.get("task_id", ""))}
    _, extract_before = read_sheet_rows(workbook_path, "player_data_extract")
    _, review_before = read_sheet_rows(workbook_path, "review_queue")

    run_ingest(
        workbook_path,
        cookies_file=args.cookies_file,
        cookies_from_browser=args.cookies_from_browser,
        timeout_seconds=args.timeout_seconds,
        page_fallback=args.page_fallback,
        ocr_fallback=args.ocr_fallback,
        task_ids=probe_task_ids,
    )

    _, raw_rows = read_sheet_rows(workbook_path, "raw_ingest")
    raw_by_task_id = {clean_text(row.get("task_id", "")): row for row in raw_rows if clean_text(row.get("task_id", ""))}

    probe_rows: list[dict[str, str]] = []
    for seed_row in probe_seed_rows:
        task_id = clean_text(seed_row.get("task_id", ""))
        raw_row = raw_by_task_id.get(task_id, {})
        probe_score, probe_signals = score_probe_text(
            raw_row.get("description_text", ""),
            raw_row.get("page_text", ""),
        )
        probe_rows.append(
            {
                "task_id": task_id,
                "video_url": clean_text(seed_row.get("video_url", "")),
                "title_detected": clean_text(raw_row.get("title_detected", "")) or clean_text(seed_row.get("title_detected", "")),
                "equipment_score": clean_text(seed_row.get("equipment_score", "")),
                "parameter_score": clean_text(seed_row.get("parameter_score", "")),
                "hard_param_score": clean_text(seed_row.get("hard_param_score", "")),
                "probe_score": str(probe_score),
                "probe_matched_signals": ", ".join(probe_signals),
                "ingest_status": clean_text(raw_row.get("ingest_status", "")),
            }
        )

    probe_rows.sort(
        key=lambda row: (
            int(row.get("probe_score", "0") or "0"),
            int(row.get("hard_param_score", "0") or "0"),
            int(row.get("parameter_score", "0") or "0"),
            int(row.get("equipment_score", "0") or "0"),
            row.get("task_id", ""),
        ),
        reverse=True,
    )

    finalists = [row for row in probe_rows if int(row.get("probe_score", "0") or "0") > 0][: args.final_limit]
    if not finalists:
        finalists = probe_rows[: args.final_limit]
    finalist_task_ids = {clean_text(row.get("task_id", "")) for row in finalists if clean_text(row.get("task_id", ""))}

    extract_result = run_extract(workbook_path, task_ids=finalist_task_ids)
    review_result = run_review_queue(workbook_path, task_ids=finalist_task_ids)

    _, raw_after = read_sheet_rows(workbook_path, "raw_ingest")
    _, extract_after = read_sheet_rows(workbook_path, "player_data_extract")
    _, review_after = read_sheet_rows(workbook_path, "review_queue")

    selected_probe_raw_rows = [row for row in raw_after if clean_text(row.get("task_id", "")) in probe_task_ids]
    selected_final_extract_rows = [
        row for row in extract_after if clean_text(row.get("task_id", "")) in finalist_task_ids
    ]
    selected_final_review_rows = [
        row for row in review_after if clean_text(row.get("task_id", "")) in finalist_task_ids
    ]

    ingest_bucket = {"success": 0, "partial": 0, "failed": 0}
    for row in selected_probe_raw_rows:
        ingest_bucket[classify_ingest_status(row)] += 1

    high_value_counts = {
        field_name: sum(
            1 for row in selected_final_extract_rows if clean_text(row.get("field_name", "")) == field_name
        )
        for field_name in HIGH_VALUE_FIELDS
    }

    summary = {
        "selection_mode": "probe_score",
        "probe_seed_count": len(probe_seed_rows),
        "probe_rows": probe_rows,
        "final_selected_count": len(finalists),
        "final_selected_rows": finalists,
        "ingest": {
            "success_count": ingest_bucket["success"],
            "partial_count": ingest_bucket["partial"],
            "failed_count": ingest_bucket["failed"],
            "description_present_count": sum(
                1 for row in selected_probe_raw_rows if clean_text(row.get("description_text", ""))
            ),
            "page_text_present_count": sum(
                1 for row in selected_probe_raw_rows if clean_text(row.get("page_text", ""))
            ),
            "ocr_present_count": count_ocr_hits(selected_probe_raw_rows),
        },
        "extract": {
            "batch_extract_count": extract_result.get("extract_count", 0),
            "new_rows_added": max(0, len(extract_after) - len(extract_before)),
            "selected_task_count": len({row.get("task_id", "") for row in selected_final_extract_rows}),
            "high_value_field_hits": high_value_counts,
        },
        "review_queue": {
            "batch_review_count": review_result.get("review_count", 0),
            "new_rows_added": max(0, len(review_after) - len(review_before)),
            "selected_task_count": len({row.get("task_id", "") for row in selected_final_review_rows}),
        },
    }
    print_json(summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
