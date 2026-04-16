from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from urllib.parse import parse_qs, urlparse


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_ROOT = Path(__file__).resolve().parent
for root in (PROJECT_ROOT, SCRIPTS_ROOT):
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

from equipment_scoring import score_text
from run_pending_batch import run_batch_for_task_ids
from shared.reel_pipeline import WORKBOOK_DEFAULT, clean_text, print_json, read_sheet_rows


LIGHTWEIGHT_KEYWORDS = {
    "直径": 6,
    "mm": 7,
    "材质": 6,
    "黄铜": 7,
    "铝合金": 7,
    "镁合金": 7,
    "速比": 6,
    "轴承": 5,
    "大齿": 6,
    "小齿": 6,
    "长轴": 6,
    "短轴": 6,
    "线杯": 6,
    "卸力": 6,
}

LIGHTWEIGHT_REGEX_PATTERNS = [
    (r"\b\d+(?:\.\d+)?\s*mm\b", 10, "数字+mm"),
    (r"\b\d+(?:\.\d+)?\s*g\b", 7, "数字+g"),
    (r"\b\d+(?:\.\d+)?\s*kg\b", 8, "数字+kg"),
    (r"\b\d+(?:\.\d+)?\s*:\s*1\b", 9, "速比句式"),
    (r"\d+\+\d+\s*轴承", 9, "轴承句式"),
    (r"(?:线杯|杯径|直径)\s*\d+(?:\.\d+)?\s*mm", 11, "线杯直径句式"),
    (r"(?:大齿|小齿)\s*(?:为|材质为|采用)\s*(?:黄铜|铝合金|镁合金|钢|锌合金|不锈钢)", 11, "齿材句式"),
    (r"三大件\s*(?:为|材质为|采用)\s*(?:铝合金|镁合金|黄铜|钢|不锈钢)", 11, "三大件句式"),
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


def extract_source_bucket(inventory_notes: str) -> str:
    for token in [part.strip() for part in clean_text(inventory_notes).split(";")]:
        if token.startswith("source_bucket="):
            return clean_text(token.split("=", 1)[1])
    return ""


def lightweight_rank_text(title_detected: str, cover_text: str) -> tuple[int, list[str]]:
    text = " ".join([clean_text(title_detected), clean_text(cover_text)])
    lowered = text.lower()
    score = 0
    matched: list[str] = []
    for keyword, weight in LIGHTWEIGHT_KEYWORDS.items():
        if keyword.lower() in lowered:
            score += weight
            matched.append(keyword)
    for pattern, weight, label in LIGHTWEIGHT_REGEX_PATTERNS:
        if re.search(pattern, text, re.I):
            score += weight
            matched.append(label)
    return score, matched


def load_bucket_candidates(
    workbook_path: Path,
    *,
    source_bucket: str,
    pending_only: bool = True,
) -> list[dict[str, str]]:
    _, inventory_rows = read_sheet_rows(workbook_path, "creator_video_inventory")
    _, input_rows = read_sheet_rows(workbook_path, "input_videos")
    input_by_video_id = {
        parse_video_id(row.get("url", "")): row
        for row in input_rows
        if parse_video_id(row.get("url", ""))
    }

    candidates: list[dict[str, str]] = []
    target_bucket = clean_text(source_bucket)
    for inventory_row in inventory_rows:
        bucket = extract_source_bucket(inventory_row.get("inventory_notes", ""))
        if bucket != target_bucket:
            continue
        video_id = clean_text(inventory_row.get("video_id", ""))
        if not video_id:
            continue
        input_row = input_by_video_id.get(video_id)
        if input_row is None:
            continue
        status = clean_text(input_row.get("status", "")).lower()
        if pending_only and status != "pending":
            continue
        title_detected = clean_text(inventory_row.get("title_detected", ""))
        cover_text = clean_text(inventory_row.get("cover_text", ""))
        lightweight_score, matched_signals = lightweight_rank_text(title_detected, cover_text)
        equipment_score, parameter_score, hard_param_score, _ = score_text(title_detected, cover_text)
        candidates.append(
            {
                "task_id": clean_text(input_row.get("task_id", "")),
                "video_id": video_id,
                "video_url": clean_text(inventory_row.get("video_url", "")),
                "title_detected": title_detected,
                "publish_date": clean_text(inventory_row.get("publish_date", "")),
                "source_bucket": bucket,
                "lightweight_param_score": str(lightweight_score),
                "lightweight_param_rank_signals": ", ".join(matched_signals),
                "equipment_score": str(equipment_score),
                "parameter_score": str(parameter_score),
                "hard_param_score": str(hard_param_score),
            }
        )

    candidates.sort(
        key=lambda row: (
            int(row.get("lightweight_param_score", "0") or "0"),
            int(row.get("hard_param_score", "0") or "0"),
            int(row.get("parameter_score", "0") or "0"),
            int(row.get("equipment_score", "0") or "0"),
            row.get("publish_date", ""),
            row.get("task_id", ""),
        ),
        reverse=True,
    )
    return candidates


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run a lightweight parameter-ranked batch from one creator source bucket."
    )
    parser.add_argument("--workbook", default=str(WORKBOOK_DEFAULT))
    parser.add_argument("--source-bucket", default="小蒋聊轮")
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--min-lightweight-score", type=int, default=0)
    parser.add_argument("--cookies-file", default="")
    parser.add_argument("--cookies-from-browser", default="")
    parser.add_argument("--timeout-seconds", type=int, default=90)
    parser.add_argument("--page-fallback", action="store_true")
    parser.add_argument("--ocr-fallback", action="store_true")
    args = parser.parse_args()

    workbook_path = Path(args.workbook).resolve()
    candidates = load_bucket_candidates(
        workbook_path,
        source_bucket=args.source_bucket,
        pending_only=True,
    )
    if args.min_lightweight_score > 0:
        candidates = [
            row
            for row in candidates
            if int(row.get("lightweight_param_score", "0") or "0") >= args.min_lightweight_score
        ]
    if not candidates:
        print_json(
            {
                "error": f"No pending candidates found for source_bucket={clean_text(args.source_bucket)}",
                "source_bucket": clean_text(args.source_bucket),
                "min_lightweight_score": args.min_lightweight_score,
            }
        )
        return 1

    selected_rows = candidates[: args.limit]
    batch_result = run_batch_for_task_ids(
        workbook_path,
        selected_rows,
        cookies_file=args.cookies_file,
        cookies_from_browser=args.cookies_from_browser,
        timeout_seconds=args.timeout_seconds,
        page_fallback=args.page_fallback,
        ocr_fallback=args.ocr_fallback,
    )
    batch_result["selection_mode"] = "source_bucket_lightweight_param_rank"
    batch_result["source_bucket"] = clean_text(args.source_bucket)
    batch_result["candidate_pool_size"] = len(candidates)
    batch_result["min_lightweight_score"] = args.min_lightweight_score
    batch_result["selected_rows"] = [
        {
            "task_id": row.get("task_id", ""),
            "video_url": row.get("video_url", ""),
            "title_detected": row.get("title_detected", ""),
            "source_bucket": row.get("source_bucket", ""),
            "lightweight_param_rank_signals": row.get("lightweight_param_rank_signals", ""),
            "lightweight_param_score": row.get("lightweight_param_score", ""),
        }
        for row in selected_rows
    ]
    print_json(batch_result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
