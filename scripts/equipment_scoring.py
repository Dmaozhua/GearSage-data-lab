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
    "轮子": 5,
    "BFS": 4,
    "对比": 5,
    "拆解": 3,
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
    "详细": 3,
    "教程": 3,
    "新款": 5,
    "入门": 5,
    "磁力": 5,
    "离心": 5,
    "DC": 5,
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

HARD_PARAM_POSITIVE_KEYWORDS = {
    "线杯": 2,
    "杯径": 2,
    "直径": 2,
    "大齿": 2,
    "小齿": 2,
    "长轴": 2,
    "短轴": 2,
    "速比": 2,
    "卸力": 2,
    "摇臂": 2,
    "轴承": 2,
    "黄铜": 3,
    "铝合金": 3,
    "结构": 2,
    "mm": 3,
    "g": 2,
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

HARD_PARAM_REGEX_PATTERNS = [
    (r"\b\d+(?:\.\d+)?\s*mm\b", 10, "单位:mm"),
    (r"\b\d+(?:\.\d+)?\s*g\b", 8, "单位:g"),
    (r"\b\d+(?:\.\d+)?\s*kg\b", 9, "单位:kg"),
    (r"\b\d+(?:\.\d+)?\s*cm\b", 7, "单位:cm"),
    (r"\b\d+(?:\.\d+)?\s*lb\b", 7, "单位:lb"),
    (r"\b\d+(?:\.\d+)?\s*号\b", 6, "单位:号"),
    (r"\b\d+\+\d+\b", 8, "数字句式:7+1"),
    (r"\b\d+(?:\.\d+)?\s*:\s*1\b", 9, "数字句式:速比"),
    (r"(?:线杯|杯径|直径)\s*\d+(?:\.\d+)?\s*mm", 12, "数字句式:线杯直径"),
    (r"(?:摇臂)\s*\d+(?:\.\d+)?\s*mm", 10, "数字句式:摇臂"),
    (r"(?:最大)?卸力\s*\d+(?:\.\d+)?\s*(?:kg|lb)", 12, "数字句式:卸力"),
    (r"(?:主齿|大齿|小齿)\s*(?:为|材质为|采用)\s*(?:黄铜|铝合金|锌合金|钢|不锈钢|钛合金)", 12, "材料句式:齿材"),
    (r"三大件\s*(?:为|材质为|采用)\s*(?:铝合金|镁合金|碳纤维|黄铜|钢|不锈钢)", 12, "材料句式:三大件"),
    (r"(?:为黄铜|为铝合金|采用[^，。]{0,10}结构)", 9, "材料/结构句式"),
    (r"(?:长轴|短轴)", 6, "结构词:轴型"),
]

PROBE_KEYWORDS = {
    "线杯直径": 12,
    "杯径": 10,
    "直径": 8,
    "大齿": 8,
    "小齿": 8,
    "摇臂": 7,
    "卸力": 8,
    "速比": 8,
    "轴承": 7,
    "黄铜": 9,
    "铝合金": 9,
    "镁合金": 9,
    "长轴": 8,
    "短轴": 8,
}

PROBE_REGEX_PATTERNS = [
    (r"\b\d+(?:\.\d+)?\s*mm\b", 12, "单位:mm"),
    (r"\b\d+(?:\.\d+)?\s*g\b", 10, "单位:g"),
    (r"\b\d+(?:\.\d+)?\s*kg\b", 11, "单位:kg"),
    (r"\b\d+(?:\.\d+)?\s*cm\b", 8, "单位:cm"),
    (r"\b\d+(?:\.\d+)?\s*lb\b", 8, "单位:lb"),
    (r"\b\d+(?:\.\d+)?\s*号\b", 7, "单位:号"),
    (r"\b\d+\+\d+\b", 11, "数字句式:7+1"),
    (r"\b\d+(?:\.\d+)?\s*:\s*1\b", 12, "数字句式:速比"),
    (r"(?:线杯|杯径|直径)\s*\d+(?:\.\d+)?\s*mm", 14, "数字句式:线杯直径"),
    (r"(?:摇臂)\s*\d+(?:\.\d+)?\s*mm", 12, "数字句式:摇臂"),
    (r"(?:最大)?卸力\s*\d+(?:\.\d+)?\s*(?:kg|lb)", 14, "数字句式:卸力"),
    (r"\d+\+\d+\s*轴承", 13, "数字句式:轴承配置"),
    (r"速比\s*\d+(?:\.\d+)?\s*:\s*1", 13, "参数句式:速比"),
    (r"(?:主齿|大齿|小齿)\s*(?:为|材质为|采用)\s*(?:黄铜|铝合金|锌合金|钢|不锈钢|钛合金)", 15, "材料句式:齿材"),
    (r"三大件\s*(?:为|材质为|采用)\s*(?:铝合金|镁合金|碳纤维|黄铜|钢|不锈钢)", 15, "材料句式:三大件"),
    (r"(?:采用[^，。]{0,10}结构)", 10, "结构句式"),
    (r"(?:长轴|短轴)", 8, "结构词:轴型"),
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


def score_text(title_detected: str, cover_text: str) -> tuple[int, int, int, list[str]]:
    text = " ".join([clean_text(title_detected), clean_text(cover_text)])
    lowered = text.lower()
    equipment_score = 0
    parameter_score = 0
    hard_param_score = 0
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
    for keyword, weight in HARD_PARAM_POSITIVE_KEYWORDS.items():
        if keyword.lower() in lowered:
            hard_param_score += weight
            matched_keywords.append(f"hard:{keyword}")
    for pattern, weight, label in HARD_PARAM_REGEX_PATTERNS:
        if re.search(pattern, text, re.I):
            hard_param_score += weight
            matched_keywords.append(f"hard:{label}")
    return equipment_score, parameter_score, hard_param_score, matched_keywords


def extract_probe_ocr_blocks(page_text: str, limit: int = 2) -> list[str]:
    text = page_text or ""
    matches = re.findall(r"\[OCR:[^\]]+\]\s*(.*?)(?=(?:\n\[OCR:)|\Z)", text, flags=re.S)
    blocks: list[str] = []
    for match in matches[:limit]:
        cleaned = clean_text(match)
        if cleaned:
            blocks.append(cleaned)
    return blocks


def build_probe_text(description_text: str, page_text: str) -> str:
    description = clean_text(description_text)
    page = clean_text((page_text or "")[:1200])
    ocr_blocks = extract_probe_ocr_blocks(page_text, limit=2)
    parts = [part for part in [description, page, *ocr_blocks] if part]
    return "\n".join(parts)


def score_probe_text(description_text: str, page_text: str) -> tuple[int, list[str]]:
    text = build_probe_text(description_text, page_text)
    lowered = text.lower()
    probe_score = 0
    matched_signals: list[str] = []
    for keyword, weight in PROBE_KEYWORDS.items():
        if keyword.lower() in lowered:
            probe_score += weight
            matched_signals.append(keyword)
    for pattern, weight, label in PROBE_REGEX_PATTERNS:
        if re.search(pattern, text, re.I):
            probe_score += weight
            matched_signals.append(label)
    return probe_score, matched_signals


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
        equipment_score, parameter_score, hard_param_score, matched_keywords = score_text(
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
                "hard_param_score": str(hard_param_score),
                "matched_keywords": ", ".join(matched_keywords),
                "status": status,
            }
        )

    scored_rows.sort(
        key=lambda row: (
            int(row.get("hard_param_score", "0") or "0"),
            int(row.get("parameter_score", "0") or "0"),
            int(row.get("equipment_score", "0") or "0"),
            row.get("publish_date", ""),
            row.get("task_id", ""),
        ),
        reverse=True,
    )
    return scored_rows
