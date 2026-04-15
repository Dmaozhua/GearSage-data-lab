from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import parse_qs, urlparse


PROJECT_ROOT = Path(__file__).resolve().parents[1]

import sys

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from shared.reel_pipeline import clean_text, read_sheet_rows


EQUIPMENT_POSITIVE_KEYWORDS = {
    "渔轮": 5,
    "水滴轮": 5,
    "纺车轮": 5,
    "轮子": 4,
    "BFS": 4,
    "对比": 3,
    "拆解": 4,
    "参数": 4,
    "介绍": 2,
    "Abu": 4,
    "阿布": 4,
    "达瓦": 4,
    "Daiwa": 4,
    "Shimano": 4,
    "禧玛诺": 4,
    "TATULA": 4,
    "REVO": 4,
    "ZILLION": 4,
    "STEEZ": 4,
    "CONQUEST": 4,
    "CURADO": 4,
    "BLACK 10": 5,
    "BF8": 5,
}

PARAMETER_POSITIVE_KEYWORDS = {
    "线杯": 5,
    "杯重": 5,
    "杯径": 5,
    "齿轮": 4,
    "大齿": 4,
    "小齿": 4,
    "材质": 4,
    "直径": 4,
    "宽度": 4,
    "速比": 4,
    "卸力": 4,
    "摇臂": 3,
    "握丸": 3,
    "长轴": 4,
    "短轴": 4,
    "轴承": 4,
    "mm": 4,
    "g": 3,
    "kg": 3,
    "cm": 2,
    "lb": 2,
    "号": 1,
}

EQUIPMENT_NEGATIVE_KEYWORDS = {
    "鱼获": -3,
    "上鱼": -3,
    "爆护": -4,
    "出钓": -3,
    "比赛": -2,
    "作钓": -3,
    "钓场": -3,
    "日常": -2,
}

PARAMETER_NEGATIVE_KEYWORDS = {
    "拆解": -1,
    "组装": -1,
    "轮子": -1,
    "Abu": -1,
    "阿布": -1,
    "Daiwa": -1,
    "达瓦": -1,
    "Shimano": -1,
    "禧玛诺": -1,
}

PARAMETER_REGEX_PATTERNS = [
    (r"直径\s*\d+(?:\.\d+)?\s*mm", 8, "直径+mm"),
    (r"(?:线杯|杯径)\s*\d+(?:\.\d+)?\s*mm", 8, "线杯/杯径+mm"),
    (r"(?:大齿|主齿)\s*(?:为|材质为|采用)\s*(?:黄铜|铝合金|锌合金|钢|不锈钢|钛合金)", 8, "大齿材料句式"),
    (r"(?:小齿)\s*(?:为|材质为|采用)\s*(?:黄铜|铝合金|锌合金|钢|不锈钢|钛合金)", 8, "小齿材料句式"),
    (r"三大件\s*(?:为|材质为|采用)\s*(?:铝合金|镁合金|碳纤维|黄铜|钢|不锈钢)", 8, "三大件材料句式"),
    (r"速比\s*\d+(?:\.\d+)?\s*:\s*1", 7, "速比句式"),
    (r"\d+\+\d+\s*轴承", 7, "轴承配置句式"),
    (r"(?:长轴|短轴)", 5, "轴型词"),
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


def score_text(title_detected: str, cover_text: str) -> tuple[int, int, list[str]]:
    text = " ".join([clean_text(title_detected), clean_text(cover_text)])
    lowered = text.lower()
    equipment_score = 0
    parameter_score = 0
    matched_keywords: list[str] = []
    for keyword, weight in EQUIPMENT_POSITIVE_KEYWORDS.items():
        if keyword.lower() in lowered:
            equipment_score += weight
            matched_keywords.append(f"eq:{keyword}")
    for keyword, weight in EQUIPMENT_NEGATIVE_KEYWORDS.items():
        if keyword.lower() in lowered:
            equipment_score += weight
            matched_keywords.append(f"eq:-{keyword}")
    for keyword, weight in PARAMETER_POSITIVE_KEYWORDS.items():
        if keyword.lower() in lowered:
            parameter_score += weight
            matched_keywords.append(f"param:{keyword}")
    for keyword, weight in PARAMETER_NEGATIVE_KEYWORDS.items():
        if keyword.lower() in lowered:
            parameter_score += weight
            matched_keywords.append(f"param:-{keyword}")
    for pattern, weight, label in PARAMETER_REGEX_PATTERNS:
        if re.search(pattern, text, re.I):
            parameter_score += weight
            matched_keywords.append(f"param:{label}")
    return equipment_score, parameter_score, matched_keywords


def load_scored_inventory_candidates(
    workbook_path: Path,
    *,
    pending_only: bool = True,
) -> list[dict[str, str]]:
    _, inventory_rows = read_sheet_rows(workbook_path, "creator_video_inventory")
    _, input_rows = read_sheet_rows(workbook_path, "input_videos")
    input_by_video_id = {
        parse_video_id(row.get("url", "")): row
        for row in input_rows
        if parse_video_id(row.get("url", ""))
    }

    scored_rows: list[dict[str, str]] = []
    for inventory_row in inventory_rows:
        video_id = clean_text(inventory_row.get("video_id", ""))
        if not video_id:
            continue
        input_row = input_by_video_id.get(video_id)
        if input_row is None:
            continue
        status = clean_text(input_row.get("status", "")).lower()
        if pending_only and status != "pending":
            continue
        equipment_score, parameter_score, matched_keywords = score_text(
            inventory_row.get("title_detected", ""),
            inventory_row.get("cover_text", ""),
        )
        scored_rows.append(
            {
                "task_id": clean_text(input_row.get("task_id", "")),
                "video_id": video_id,
                "video_url": clean_text(inventory_row.get("video_url", "")),
                "title_detected": clean_text(inventory_row.get("title_detected", "")),
                "publish_date": clean_text(inventory_row.get("publish_date", "")),
                "equipment_score": str(equipment_score),
                "parameter_score": str(parameter_score),
                "matched_keywords": ", ".join(matched_keywords),
                "status": status,
            }
        )

    scored_rows.sort(
        key=lambda row: (
            int(row.get("parameter_score", "0") or "0"),
            int(row.get("equipment_score", "0") or "0"),
            row.get("publish_date", ""),
            row.get("task_id", ""),
        ),
        reverse=True,
    )
    return scored_rows
