from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET


PROJECT_ROOT = Path(__file__).resolve().parents[1]
WORKBOOK_DEFAULT = PROJECT_ROOT / "data" / "input" / "reel_player_data_pipeline_v1.xlsx"
FIELD_WHITELIST_PATH = PROJECT_ROOT / "shared" / "reel_data_schema" / "field_whitelist.yaml"
SOURCE_AUTHORITY_PATH = PROJECT_ROOT / "shared" / "reel_data_schema" / "source_authority.yaml"
MODEL_NORMALIZATION_PATH = PROJECT_ROOT / "shared" / "reel_data_schema" / "model_normalization.yaml"
CACHE_ROOT = PROJECT_ROOT / "data" / "cache"
PLAYWRIGHT_NODE_MODULES = PROJECT_ROOT / ".tmp" / "playwright-runner" / "node_modules"
PLAYWRIGHT_CAPTURE_SCRIPT = (
    PROJECT_ROOT / "skills" / "video-link-ingest" / "scripts" / "playwright_page_capture.cjs"
)
OCR_SCREENSHOT_SCRIPT = PROJECT_ROOT / "skills" / "video-link-ingest" / "scripts" / "ocr_screenshots.swift"

MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
PACKAGE_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"
XML_NS = "http://www.w3.org/XML/1998/namespace"

ET.register_namespace("", MAIN_NS)
ET.register_namespace("r", REL_NS)

INPUT_HEADERS = [
    "task_id",
    "platform",
    "url",
    "creator",
    "title_hint",
    "content_type",
    "priority",
    "status",
    "language",
    "target_field_scope",
    "manual_notes",
    "created_at",
    "updated_at",
]

RAW_INGEST_HEADERS = [
    "task_id",
    "platform",
    "url",
    "creator_input",
    "creator_detected",
    "title_detected",
    "publish_date",
    "description_text",
    "subtitle_text",
    "transcript_text",
    "page_text",
    "comments_text",
    "audio_file_path",
    "video_file_path",
    "subtitle_file_path",
    "screenshot_dir",
    "asset_dir",
    "ingest_method",
    "ingest_status",
    "ingest_notes",
    "ingest_started_at",
    "ingest_finished_at",
]

PLAYER_EXTRACT_HEADERS = [
    "extract_id",
    "task_id",
    "platform",
    "url",
    "creator",
    "publish_date",
    "reel_model_raw",
    "reel_model_normalized",
    "reel_brand_normalized",
    "reel_type_guess",
    "field_name",
    "field_value_raw",
    "field_value_normalized",
    "unit",
    "value_type",
    "source_quote",
    "source_quote_type",
    "source_authority",
    "confidence",
    "review_required",
    "review_reason",
    "extraction_method",
    "extract_notes",
    "created_at",
]

REVIEW_QUEUE_HEADERS = [
    "review_id",
    "extract_id",
    "task_id",
    "reel_model_normalized",
    "reel_model_raw",
    "field_name",
    "candidate_value",
    "candidate_value_raw",
    "unit",
    "confidence",
    "source_quote",
    "source_quote_type",
    "source_url",
    "source_author",
    "source_authority",
    "review_action",
    "review_value",
    "review_comment",
    "reviewer",
    "reviewed_at",
]

SOURCE_PRIORITY = {
    "subtitle": 4,
    "spoken": 4,
    "description": 3,
    "page_text": 2,
    "title": 2,
    "ocr": 2,
    "comments": 1,
    "manual_note": 1,
}

BRAND_PATTERNS = [
    "Shimano",
    "Daiwa",
    "Abu Garcia",
    "Okuma",
    "Lew's",
    "禧玛诺",
    "达亿瓦",
    "达瓦",
    "阿布",
    "欧库玛",
]

MODEL_REGEX = re.compile(
    r"(?i)\b(?:shimano|daiwa|abu garcia|okuma|lew's)\b[^\n,，。;；:：]{0,24}"
)
MODEL_REGEX_CN = re.compile(r"(?:禧玛诺|达亿瓦|达瓦|阿布|欧库玛)[^\n,，。;；:：]{0,18}")
MODEL_REGEX_GENERIC = re.compile(
    r"\b(?:REVO|METANIUM|ANTARES|CURADO|ALDEBARAN|STEEZ|ZILLION|TATULA|CALDIA|CERTATE|EXIST)\b(?:[ -][A-Z0-9]+){0,3}",
    re.I,
)
URL_REGEX = re.compile(r"https?://[^\s\"'<>]+", re.I)
ACTIVE_EXTRACT_FIELDS = {
    "spool_weight_g",
    "spool_diameter_mm",
    "spool_width_mm",
    "spool_axis_type",
    "body_material",
    "main_gear_material",
    "main_gear_size",
    "minor_gear_material",
}

NUMERIC_PATTERNS = {
    "spool_diameter_mm": [
        re.compile(r"(?:线杯直径|杯径|spool diameter)\s*[:：]?\s*((\d+(?:\.\d+)?)\s*mm)", re.I),
    ],
    "spool_width_mm": [
        re.compile(r"(?:线杯宽度|杯宽|spool width)\s*[:：]?\s*((\d+(?:\.\d+)?)\s*mm)", re.I),
    ],
    "spool_weight_g": [
        re.compile(r"(?:线杯重量|杯重|spool weight)\s*[:：]?\s*((\d+(?:\.\d+)?)\s*g)", re.I),
    ],
    "main_gear_size": [
        re.compile(
            r"(?:主齿尺寸|主齿大小|main gear size)\s*[:：]?\s*([A-Za-z0-9.+-]+(?:\s*(?:mm|号))?)",
            re.I,
        ),
    ],
}

MATERIAL_PATTERNS = {
    "body_material": re.compile(
        r"(?:机身材质|body material)\s*[:：]?\s*(?:[^\s,，。;；:：]{0,8})?(铝合金|铝|镁合金|镁|碳纤维|碳|黄铜|钛合金|钛|不锈钢|钢|锌合金|aluminum|magnesium|carbon fiber|carbon|brass|titanium|stainless steel|steel|zinc alloy)",
        re.I,
    ),
    "main_gear_material": re.compile(
        r"(?:主齿材质|main gear material)\s*[:：]?\s*(?:[^\s,，。;；:：]{0,8})?(铝合金|铝|镁合金|镁|碳纤维|碳|黄铜|钛合金|钛|不锈钢|钢|锌合金|aluminum|magnesium|carbon fiber|carbon|brass|titanium|stainless steel|steel|zinc alloy)",
        re.I,
    ),
    "minor_gear_material": re.compile(
        r"(?:小齿材质|minor gear material)\s*[:：]?\s*(?:[^\s,，。;；:：]{0,8})?(铝合金|铝|镁合金|镁|碳纤维|碳|黄铜|钛合金|钛|不锈钢|钢|锌合金|aluminum|magnesium|carbon fiber|carbon|brass|titanium|stainless steel|steel|zinc alloy)",
        re.I,
    ),
}

TEXT_PATTERNS = {
    "spool_axis_type": re.compile(r"(?:线杯轴|轴型|spool axis)\s*[:：]?\s*(长轴|短轴)", re.I),
}

MATERIAL_VALUE_REGEX = (
    r"(铝合金|铝|镁合金|镁|碳纤维|碳|黄铜|钛合金|钛|不锈钢|钢|锌合金|"
    r"aluminum|magnesium|carbon fiber|carbon|brass|titanium|stainless steel|steel|zinc alloy)"
)

NATURAL_BODY_MATERIAL_PATTERNS = [
    re.compile(rf"(?:机身(?:材质)?|body)\s*(?:为|是|采用)\s*{MATERIAL_VALUE_REGEX}", re.I),
    re.compile(rf"(?:三大件)\s*(?:为|是|采用)\s*{MATERIAL_VALUE_REGEX}", re.I),
]

NATURAL_SHARED_GEAR_MATERIAL_PATTERNS = [
    re.compile(
        rf"(?:大齿、小齿(?:和主轴)?|大齿和小齿(?:及主轴)?|大齿、小齿及主轴)\s*(?:为|是|采用)\s*{MATERIAL_VALUE_REGEX}",
        re.I,
    ),
]

NATURAL_MAIN_GEAR_MATERIAL_PATTERNS = [
    re.compile(rf"(?:大齿|主齿)(?:材质)?\s*(?:为|是|采用)\s*{MATERIAL_VALUE_REGEX}", re.I),
]

NATURAL_MINOR_GEAR_MATERIAL_PATTERNS = [
    re.compile(rf"(?:小齿)(?:材质)?\s*(?:为|是|采用)\s*{MATERIAL_VALUE_REGEX}", re.I),
]

NATURAL_SPOOL_AXIS_PATTERNS = [
    {
        "regex": re.compile(r"(假长轴结构)", re.I),
        "normalized": "长轴",
        "confidence": "low",
        "extract_notes": "Conservative mapping from natural-language phrase '假长轴结构'.",
    }
]

TYPE_PATTERNS = [
    (re.compile(r"(?:纺车|spinning)", re.I), "spinning"),
    (re.compile(r"(?:水滴|鼓轮|baitcast|baitcasting)", re.I), "baitcasting"),
]


def _main(tag: str) -> str:
    return f"{{{MAIN_NS}}}{tag}"


def _rel(tag: str) -> str:
    return f"{{{PACKAGE_REL_NS}}}{tag}"


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def load_json_like_yaml(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return {}
    if text.startswith("{") or text.startswith("["):
        return json.loads(text)
    result: dict[str, Any] = {}
    current_key: str | None = None
    for raw_line in text.splitlines():
        line = raw_line.rstrip()
        if not line or line.lstrip().startswith("#"):
            continue
        if not line.startswith(" ") and ":" in line:
            key, _, value = line.partition(":")
            key = key.strip()
            value = value.strip()
            if value == "{}":
                result[key] = {}
                current_key = None
            elif not value:
                result[key] = []
                current_key = key
            else:
                result[key] = value
                current_key = None
            continue
        if current_key and line.lstrip().startswith("- "):
            result[current_key].append(line.lstrip()[2:].strip().strip('"'))
    return result


def load_field_rules() -> dict[str, Any]:
    data = load_json_like_yaml(FIELD_WHITELIST_PATH)
    return {
        "reel_player_fields": data.get("reel_player_fields", []),
        "numeric_fields": set(data.get("numeric_fields", [])),
        "material_fields": set(data.get("material_fields", [])),
        "boolean_fields": set(data.get("boolean_fields", [])),
        "text_fields": set(data.get("text_fields", [])),
    }


def load_source_authority_rules() -> dict[str, Any]:
    data = load_json_like_yaml(SOURCE_AUTHORITY_PATH)
    return {
        "source_authority_levels": data.get("source_authority_levels", []),
        "source_quote_type_defaults": data.get("source_quote_type_defaults", {}),
        "creator_overrides": data.get("creator_overrides", {}),
    }


def load_model_normalization() -> dict[str, str]:
    data = load_json_like_yaml(MODEL_NORMALIZATION_PATH)
    return data.get("model_normalization", {})


def column_letter(index: int) -> str:
    letters: list[str] = []
    current = index
    while current:
        current, remainder = divmod(current - 1, 26)
        letters.append(chr(65 + remainder))
    return "".join(reversed(letters))


def column_index(ref: str) -> int:
    letters = "".join(char for char in ref if char.isalpha()).upper()
    value = 0
    for char in letters:
        value = value * 26 + (ord(char) - 64)
    return value


def _shared_strings(zf: zipfile.ZipFile) -> list[str]:
    try:
        raw = zf.read("xl/sharedStrings.xml")
    except KeyError:
        return []
    root = ET.fromstring(raw)
    values: list[str] = []
    for item in root.findall(_main("si")):
        texts = [node.text or "" for node in item.iterfind(f".//{_main('t')}")]
        values.append("".join(texts))
    return values


def workbook_sheet_map(workbook_path: Path) -> dict[str, str]:
    with zipfile.ZipFile(workbook_path) as zf:
        workbook_root = ET.fromstring(zf.read("xl/workbook.xml"))
        rel_root = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
    rel_map = {
        rel.attrib["Id"]: rel.attrib["Target"]
        for rel in rel_root.findall(_rel("Relationship"))
    }
    sheet_map: dict[str, str] = {}
    for sheet in workbook_root.find(_main("sheets")):
        rel_id = sheet.attrib[f"{{{REL_NS}}}id"]
        target = rel_map[rel_id]
        normalized_target = target.lstrip("/")
        normalized = (
            normalized_target
            if normalized_target.startswith("xl/")
            else f"xl/{normalized_target}"
        )
        sheet_map[sheet.attrib["name"]] = normalized
    return sheet_map


def cell_value(cell: ET.Element, shared_strings: list[str]) -> str:
    cell_type = cell.attrib.get("t")
    if cell_type == "inlineStr":
        texts = [node.text or "" for node in cell.iterfind(f".//{_main('t')}")]
        return "".join(texts)
    raw = cell.find(_main("v"))
    if raw is None or raw.text is None:
        return ""
    if cell_type == "s":
        try:
            return shared_strings[int(raw.text)]
        except (IndexError, ValueError):
            return ""
    return raw.text


def read_sheet_rows(workbook_path: Path, sheet_name: str) -> tuple[list[str], list[dict[str, str]]]:
    sheet_map = workbook_sheet_map(workbook_path)
    sheet_member = sheet_map[sheet_name]
    with zipfile.ZipFile(workbook_path) as zf:
        root = ET.fromstring(zf.read(sheet_member))
        shared_strings = _shared_strings(zf)
    sheet_data = root.find(_main("sheetData"))
    if sheet_data is None:
        return [], []
    rows = sheet_data.findall(_main("row"))
    if not rows:
        return [], []
    headers_by_index: dict[int, str] = {}
    for cell in rows[0].findall(_main("c")):
        headers_by_index[column_index(cell.attrib["r"])] = cell_value(cell, shared_strings)
    if not headers_by_index:
        return [], []
    max_index = max(headers_by_index)
    headers = [headers_by_index.get(index, "") for index in range(1, max_index + 1)]
    records: list[dict[str, str]] = []
    for row in rows[1:]:
        values = {header: "" for header in headers if header}
        for cell in row.findall(_main("c")):
            index = column_index(cell.attrib["r"])
            header = headers[index - 1] if index - 1 < len(headers) else ""
            if header:
                values[header] = cell_value(cell, shared_strings)
        if any(value not in ("", None) for value in values.values()):
            records.append(values)
    return [header for header in headers if header], records


def _set_inline_text(cell: ET.Element, value: str) -> None:
    cell.attrib["t"] = "inlineStr"
    is_node = ET.SubElement(cell, _main("is"))
    t_node = ET.SubElement(is_node, _main("t"))
    if value[:1].isspace() or value[-1:].isspace() or "\n" in value:
        t_node.attrib[f"{{{XML_NS}}}space"] = "preserve"
    t_node.text = value


def _sheet_bytes_with_rows(
    source_bytes: bytes,
    headers: list[str],
    rows: list[dict[str, Any]],
) -> bytes:
    root = ET.fromstring(source_bytes)
    sheet_data = root.find(_main("sheetData"))
    if sheet_data is None:
        sheet_data = ET.SubElement(root, _main("sheetData"))
    sheet_data.clear()

    header_row = ET.SubElement(sheet_data, _main("row"), {"r": "1"})
    for index, header in enumerate(headers, start=1):
        cell = ET.SubElement(header_row, _main("c"), {"r": f"{column_letter(index)}1"})
        _set_inline_text(cell, str(header))

    active_rows: list[dict[str, Any]] = []
    for row in rows:
        if any(str(row.get(header, "")).strip() for header in headers):
            active_rows.append(row)

    for row_index, row in enumerate(active_rows, start=2):
        row_node = ET.SubElement(sheet_data, _main("row"), {"r": str(row_index)})
        for col_index, header in enumerate(headers, start=1):
            value = row.get(header, "")
            if value is None:
                continue
            text = str(value)
            if text == "":
                continue
            cell = ET.SubElement(row_node, _main("c"), {"r": f"{column_letter(col_index)}{row_index}"})
            _set_inline_text(cell, text)

    last_col = column_letter(len(headers))
    last_row = max(1, len(active_rows) + 1)
    dimension = root.find(_main("dimension"))
    if dimension is None:
        dimension = ET.Element(_main("dimension"))
        root.insert(0, dimension)
    dimension.attrib["ref"] = f"A1:{last_col}{last_row}"

    return ET.tostring(root, encoding="utf-8", xml_declaration=False)


def write_multiple_sheets(
    workbook_path: Path,
    updates: dict[str, tuple[list[str], list[dict[str, Any]]]],
) -> None:
    sheet_map = workbook_sheet_map(workbook_path)
    with zipfile.ZipFile(workbook_path) as zf:
        members = {name: zf.read(name) for name in zf.namelist()}

    for sheet_name, (headers, rows) in updates.items():
        sheet_member = sheet_map[sheet_name]
        members[sheet_member] = _sheet_bytes_with_rows(members[sheet_member], headers, rows)

    with zipfile.ZipFile(workbook_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name, content in members.items():
            zf.writestr(name, content)


def write_sheet_rows(workbook_path: Path, sheet_name: str, headers: list[str], rows: list[dict[str, Any]]) -> None:
    write_multiple_sheets(workbook_path, {sheet_name: (headers, rows)})


def records_by_key(rows: list[dict[str, str]], key: str) -> dict[str, dict[str, str]]:
    result: dict[str, dict[str, str]] = {}
    for row in rows:
        value = row.get(key, "")
        if value:
            result[value] = row
    return result


def stable_id(prefix: str, *parts: str) -> str:
    digest = hashlib.md5("||".join(parts).encode("utf-8")).hexdigest()[:12]
    return f"{prefix}_{digest}"


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def to_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="ignore")
    return str(value)


def clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", to_text(value)).strip()


def clean_multiline_text(value: str) -> str:
    lines = [clean_text(line) for line in (value or "").splitlines()]
    return "\n".join(line for line in lines if line)


def clean_vtt(value: str) -> str:
    lines: list[str] = []
    for raw_line in value.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line == "WEBVTT" or line.startswith("NOTE"):
            continue
        if "-->" in line:
            continue
        if re.fullmatch(r"\d+", line):
            continue
        line = re.sub(r"<[^>]+>", "", line)
        line = clean_text(line)
        if line:
            lines.append(line)
    deduped: list[str] = []
    for line in lines:
        if not deduped or deduped[-1] != line:
            deduped.append(line)
    return "\n".join(deduped)


def read_first_match(paths: list[Path]) -> tuple[str, str]:
    for path in paths:
        if path.exists():
            return path.read_text(encoding="utf-8", errors="ignore"), str(path)
    return "", ""


def format_upload_date(value: str | None) -> str:
    if not value:
        return ""
    if re.fullmatch(r"\d{8}", value):
        return f"{value[:4]}-{value[4:6]}-{value[6:8]}"
    return value


def preferred_subtitle_files(asset_dir: Path, language_hint: str) -> list[Path]:
    candidates = sorted(asset_dir.glob("*.vtt"))
    if not candidates:
        return []
    language_hint = language_hint.strip()
    if not language_hint:
        return candidates
    prioritized = [path for path in candidates if f".{language_hint}." in path.name or path.name.endswith(f".{language_hint}.vtt")]
    remaining = [path for path in candidates if path not in prioritized]
    return prioritized + remaining


def yt_dlp_binary() -> str:
    return "yt-dlp"


def extract_first_url(value: str) -> str:
    match = URL_REGEX.search(value or "")
    return match.group(0).rstrip(".,);]") if match else ""


def resolve_cookie_options(
    cookies_file: str = "",
    cookies_from_browser: str = "",
) -> tuple[str, str]:
    resolved_file = clean_text(cookies_file) or clean_text(os.getenv("GEARSAGE_YTDLP_COOKIES_FILE", ""))
    resolved_browser = clean_text(cookies_from_browser) or clean_text(
        os.getenv("GEARSAGE_YTDLP_COOKIES_FROM_BROWSER", "")
    )
    return resolved_file, resolved_browser


def ingest_status_from_capture(
    fetched_title: bool,
    fetched_creator: bool,
    fetched_publish_date: bool,
    fetched_description: bool,
    fetched_subtitle: bool,
    fetched_transcript: bool,
) -> str:
    if fetched_transcript:
        return "transcript_available"
    if fetched_subtitle:
        return "subtitle_available"
    if fetched_description:
        return "description_only"
    if fetched_title or fetched_creator or fetched_publish_date:
        return "metadata_only"
    return "asset_fetch_failed"


def summarize_missing_fields(payload: dict[str, str]) -> list[str]:
    tracked_fields = [
        "title_detected",
        "creator_detected",
        "publish_date",
        "description_text",
        "subtitle_text",
        "transcript_text",
    ]
    return [field_name for field_name in tracked_fields if not clean_text(payload.get(field_name, ""))]


def detect_platform_block_reason(stderr_text: str, stdout_text: str, timed_out: bool) -> str:
    combined = f"{stderr_text} {stdout_text}".lower()
    if timed_out:
        return "request_timeout"
    if "fresh cookies" in combined or "cookies are needed" in combined:
        return "fresh_cookies_required"
    if "--cookies-from-browser" in combined and ("could not" in combined or "failed" in combined):
        return "cookies_browser_unavailable"
    if "failed to parse json" in combined:
        return "platform_json_parse_failed"
    if "unsupported url" in combined or "unsupported url" in combined:
        return "unsupported_url"
    return ""


def cookies_mode_label(cookies_file_value: str, cookies_browser_value: str) -> str:
    if cookies_file_value:
        return "cookies_file"
    if cookies_browser_value:
        return f"cookies_from_browser:{cookies_browser_value}"
    return "none"


def build_ingest_diagnostic_note(
    *,
    metadata_present: bool,
    description_present: bool,
    subtitle_present: bool,
    transcript_present: bool,
    cookies_mode: str,
    platform_block_reason: str,
) -> str:
    return (
        "diag:"
        f"metadata_present={'yes' if metadata_present else 'no'}|"
        f"description_present={'yes' if description_present else 'no'}|"
        f"subtitle_present={'yes' if subtitle_present else 'no'}|"
        f"transcript_present={'yes' if transcript_present else 'no'}|"
        f"cookies_mode={cookies_mode or 'none'}|"
        f"platform_block_reason={platform_block_reason or 'none'}"
    )


def build_playwright_note(
    *,
    opened: bool,
    expand_clicked: bool,
    screenshot_count: int,
    page_text_present: bool,
    description_present: bool,
) -> str:
    return (
        "playwright:"
        f"opened={'yes' if opened else 'no'}|"
        f"expand_clicked={'yes' if expand_clicked else 'no'}|"
        f"screenshot_count={screenshot_count}|"
        f"description_present={'yes' if description_present else 'no'}|"
        f"page_text_present={'yes' if page_text_present else 'no'}"
    )


def build_ocr_note(*, extracted: bool, char_count: int) -> str:
    return f"ocr:extracted={'yes' if extracted else 'no'}|char_count={char_count}"


def page_fallback_status(payload: dict[str, str]) -> str:
    if clean_text(payload.get("transcript_text", "")):
        return "transcript_available"
    if clean_text(payload.get("subtitle_text", "")):
        return "subtitle_available"
    if clean_text(payload.get("description_text", "")) and "playwright_page" in payload.get("ingest_method", ""):
        return "page_fallback_available"
    if clean_text(payload.get("page_text", "")) and "playwright_page" in payload.get("ingest_method", ""):
        return "page_fallback_available"
    if clean_text(payload.get("description_text", "")):
        return "description_only"
    if any(
        clean_text(payload.get(field_name, ""))
        for field_name in ["title_detected", "creator_detected", "publish_date"]
    ):
        return "metadata_only"
    return "asset_fetch_failed"


def run_ocr_on_screenshots(screenshot_dir: str, timeout_seconds: int = 90) -> dict[str, Any]:
    screenshot_root = Path(screenshot_dir)
    if not screenshot_root.exists():
        return {"items": [], "combined_text": "", "char_count": 0, "notes": ["ocr screenshot directory missing"]}
    if not OCR_SCREENSHOT_SCRIPT.exists():
        return {"items": [], "combined_text": "", "char_count": 0, "notes": ["ocr screenshot script missing"]}
    screenshot_paths = [str(path) for path in sorted(screenshot_root.glob("*.png"))]
    if not screenshot_paths:
        return {"items": [], "combined_text": "", "char_count": 0, "notes": ["no screenshots found for ocr"]}

    command = ["swift", str(OCR_SCREENSHOT_SCRIPT), *screenshot_paths]
    try:
        process = subprocess.run(
            command,
            cwd=PROJECT_ROOT,
            text=True,
            capture_output=True,
            check=False,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired:
        return {"items": [], "combined_text": "", "char_count": 0, "notes": [f"ocr timeout after {timeout_seconds}s"]}

    if process.returncode != 0:
        return {
            "items": [],
            "combined_text": "",
            "char_count": 0,
            "notes": [clean_text(process.stderr) or "ocr failed"],
        }

    try:
        items = json.loads(process.stdout or "[]")
    except json.JSONDecodeError:
        return {"items": [], "combined_text": "", "char_count": 0, "notes": ["ocr returned invalid json"]}

    sections: list[str] = []
    total_chars = 0
    for item in items:
        text = clean_multiline_text(item.get("text", ""))
        if not text:
            continue
        total_chars += len(text)
        sections.append(f"[OCR:{Path(item.get('path', '')).name}]\n{text}")
    combined_text = "\n\n".join(sections)
    return {
        "items": items,
        "combined_text": combined_text,
        "char_count": total_chars,
        "notes": [],
    }


def merge_ocr_capture(payload: dict[str, str], ocr_result: dict[str, Any], note_parts: list[str]) -> dict[str, str]:
    combined_text = clean_multiline_text(ocr_result.get("combined_text", ""))
    char_count = int(ocr_result.get("char_count", 0) or 0)
    if combined_text:
        existing_page_text = clean_multiline_text(payload.get("page_text", ""))
        payload["page_text"] = (
            existing_page_text + "\n\n" + combined_text if existing_page_text else combined_text
        )
    note_parts.append(build_ocr_note(extracted=bool(combined_text), char_count=char_count))
    for note in ocr_result.get("notes", []):
        if note:
            note_parts.append(f"ocr_note={note}")
    if combined_text:
        payload["ingest_method"] = ",".join(
            part for part in [payload.get("ingest_method", ""), "macos_vision_ocr"] if part
        )
        payload["ingest_status"] = page_fallback_status(payload)
    return payload


def run_playwright_page_capture(
    task_row: dict[str, str],
    asset_dir: Path,
    timeout_seconds: int = 45,
) -> dict[str, Any]:
    if not PLAYWRIGHT_CAPTURE_SCRIPT.exists():
        return {
            "page_opened": False,
            "notes": ["playwright capture script missing"],
            "screenshot_count": 0,
            "screenshot_dir": "",
        }
    if not PLAYWRIGHT_NODE_MODULES.exists():
        return {
            "page_opened": False,
            "notes": ["playwright runtime missing under .tmp/playwright-runner"],
            "screenshot_count": 0,
            "screenshot_dir": "",
        }

    output_json = asset_dir / "playwright" / "page_capture.json"
    ensure_dir(output_json.parent)
    command = [
        "node",
        str(PLAYWRIGHT_CAPTURE_SCRIPT),
        "--url",
        extract_first_url(task_row.get("url", "")) or task_row.get("url", ""),
        "--asset-dir",
        str(asset_dir),
        "--output-json",
        str(output_json),
        "--timeout-seconds",
        str(timeout_seconds),
    ]
    env = os.environ.copy()
    env["NODE_PATH"] = str(PLAYWRIGHT_NODE_MODULES)
    try:
        process = subprocess.run(
            command,
            cwd=PROJECT_ROOT,
            env=env,
            text=True,
            capture_output=True,
            check=False,
            timeout=timeout_seconds + 30,
        )
    except subprocess.TimeoutExpired:
        return {
            "page_opened": False,
            "notes": [f"playwright timeout after {timeout_seconds + 30}s"],
            "screenshot_count": 0,
            "screenshot_dir": "",
        }

    capture: dict[str, Any] = {}
    stdout_text = clean_text(process.stdout)
    stderr_text = clean_text(process.stderr)
    if output_json.exists():
        try:
            capture = json.loads(output_json.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            capture = {}
    elif process.stdout.strip():
        try:
            capture = json.loads(process.stdout)
        except json.JSONDecodeError:
            capture = {}

    if not capture:
        capture = {
            "page_opened": False,
            "notes": ["playwright returned no structured output"],
            "screenshot_count": 0,
            "screenshot_dir": "",
        }
    notes = [clean_text(note) for note in capture.get("notes", []) if clean_text(note)]
    if process.returncode != 0 and stderr_text:
        notes.append(stderr_text[:240])
    capture["notes"] = notes
    capture["_stdout"] = stdout_text[:240]
    capture["_stderr"] = stderr_text[:240]
    return capture


def merge_playwright_capture(
    payload: dict[str, str],
    capture: dict[str, Any],
    note_parts: list[str],
    title_hint: str,
    creator_input: str,
) -> dict[str, str]:
    opened = bool(capture.get("page_opened"))
    description_visible = clean_multiline_text(capture.get("description_visible", ""))
    page_text_visible = clean_multiline_text(capture.get("page_text_visible", ""))
    visible_title = clean_text(capture.get("visible_title", ""))
    creator_visible = clean_text(capture.get("creator_visible", ""))
    publish_date_visible = clean_text(capture.get("publish_date_visible", ""))
    screenshot_dir = clean_text(capture.get("screenshot_dir", ""))
    screenshot_count = int(capture.get("screenshot_count", 0) or 0)
    expand_clicked = bool(capture.get("expand_clicked"))

    payload["ingest_method"] = ",".join(
        part
        for part in [payload.get("ingest_method", ""), "playwright_page"]
        if part
    )
    if visible_title and (
        not clean_text(payload.get("title_detected", ""))
        or clean_text(payload.get("title_detected", "")) == title_hint
    ):
        payload["title_detected"] = visible_title
        note_parts.append("title_detected updated from playwright page")
    if creator_visible and (
        not clean_text(payload.get("creator_detected", ""))
        or clean_text(payload.get("creator_detected", "")) == creator_input
    ):
        payload["creator_detected"] = creator_visible
        note_parts.append("creator_detected updated from playwright page")
    if publish_date_visible and not clean_text(payload.get("publish_date", "")):
        payload["publish_date"] = publish_date_visible
        note_parts.append("publish_date filled from playwright page")
    if description_visible and not clean_text(payload.get("description_text", "")):
        payload["description_text"] = description_visible
        note_parts.append("description_text filled from playwright page")
    if page_text_visible:
        payload["page_text"] = page_text_visible
    if screenshot_dir:
        try:
            payload["screenshot_dir"] = str(Path(screenshot_dir).resolve().relative_to(PROJECT_ROOT))
        except ValueError:
            payload["screenshot_dir"] = screenshot_dir
    note_parts.append(
        build_playwright_note(
            opened=opened,
            expand_clicked=expand_clicked,
            screenshot_count=screenshot_count,
            page_text_present=bool(page_text_visible),
            description_present=bool(description_visible),
        )
    )
    for note in capture.get("notes", []):
        if note:
            note_parts.append(f"playwright_note={note}")

    payload["ingest_status"] = page_fallback_status(payload)
    return payload


def fetch_video_assets(
    task_row: dict[str, str],
    cache_root: Path = CACHE_ROOT,
    cookies_file: str = "",
    cookies_from_browser: str = "",
    timeout_seconds: int = 90,
    page_fallback: bool = False,
    ocr_fallback: bool = False,
) -> dict[str, str]:
    task_id = task_row.get("task_id", "").strip()
    raw_url = task_row.get("url", "").strip()
    url = extract_first_url(raw_url) or raw_url
    language_hint = task_row.get("language", "").strip()
    title_hint = clean_text(task_row.get("title_hint", ""))
    creator_input = clean_text(task_row.get("creator", ""))
    asset_dir = cache_root / task_id
    ensure_dir(asset_dir)
    base_output = asset_dir / "source"
    started_at = utc_now()
    if not task_id or not url:
        return {
            "task_id": task_id,
            "url": url,
            "asset_dir": str(asset_dir.relative_to(PROJECT_ROOT)),
            "subtitle_file_path": "",
            "description_text": "",
            "subtitle_text": "",
            "transcript_text": "",
            "title_detected": "",
            "creator_detected": "",
            "publish_date": "",
            "ingest_method": "yt-dlp",
            "ingest_status": "asset_fetch_failed",
            "ingest_notes": "Missing task_id or url",
            "ingest_started_at": started_at,
            "ingest_finished_at": utc_now(),
        }

    cookies_file_value, cookies_browser_value = resolve_cookie_options(
        cookies_file=cookies_file,
        cookies_from_browser=cookies_from_browser,
    )
    cookies_mode = cookies_mode_label(cookies_file_value, cookies_browser_value)
    if cookies_file_value and not Path(cookies_file_value).exists():
        return {
            "task_id": task_id,
            "platform": task_row.get("platform", "").strip(),
            "url": url,
            "creator_input": creator_input,
            "creator_detected": "",
            "title_detected": "",
            "publish_date": "",
            "description_text": "",
            "subtitle_text": "",
            "transcript_text": "",
            "page_text": "",
            "comments_text": "",
            "audio_file_path": "",
            "video_file_path": "",
            "subtitle_file_path": "",
            "screenshot_dir": "",
            "asset_dir": str(asset_dir.relative_to(PROJECT_ROOT)),
            "ingest_method": "yt-dlp,cookies_file",
            "ingest_status": "asset_fetch_failed",
            "ingest_notes": "; ".join(
                [
                    f"cookies file not found: {cookies_file_value}",
                    build_ingest_diagnostic_note(
                        metadata_present=False,
                        description_present=False,
                        subtitle_present=False,
                        transcript_present=False,
                        cookies_mode=cookies_mode,
                        platform_block_reason="cookies_file_missing",
                    ),
                ]
            ),
            "ingest_started_at": started_at,
            "ingest_finished_at": utc_now(),
        }
    command = [
        yt_dlp_binary(),
        "--skip-download",
        "--write-info-json",
        "--write-description",
        "--write-auto-subs",
        "--write-subs",
        "--sub-langs",
        "all",
        "--sub-format",
        "vtt",
        "-o",
        str(base_output) + ".%(ext)s",
    ]
    ingest_method_parts = ["yt-dlp"]
    if cookies_file_value:
        command.extend(["--cookies", cookies_file_value])
        ingest_method_parts.append("cookies_file")
    if cookies_browser_value:
        command.extend(["--cookies-from-browser", cookies_browser_value])
        ingest_method_parts.append("cookies_from_browser")
    command.append(url)
    timed_out = False
    try:
        process = subprocess.run(
            command,
            cwd=PROJECT_ROOT,
            text=True,
            capture_output=True,
            check=False,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        timed_out = True
        process = subprocess.CompletedProcess(
            args=exc.cmd,
            returncode=124,
            stdout=exc.stdout or "",
            stderr=exc.stderr or "",
        )

    info_files = sorted(asset_dir.glob("*.info.json"))
    info: dict[str, Any] = {}
    if info_files:
        try:
            info = json.loads(info_files[0].read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            info = {}

    description_text = clean_multiline_text(str(info.get("description", "")))
    if not description_text:
        description_text, _ = read_first_match(sorted(asset_dir.glob("*.description")))
        description_text = clean_multiline_text(description_text)

    subtitle_body = ""
    transcript_body = ""
    subtitle_path = ""
    subtitle_candidates = preferred_subtitle_files(asset_dir, language_hint)
    manual_caption_map = info.get("subtitles") or {}
    auto_caption_map = info.get("automatic_captions") or {}
    if subtitle_candidates:
        subtitle_raw, subtitle_path = read_first_match(subtitle_candidates)
        cleaned_caption = clean_vtt(subtitle_raw)
        if cleaned_caption:
            if manual_caption_map:
                subtitle_body = cleaned_caption
            elif auto_caption_map:
                transcript_body = cleaned_caption
            else:
                subtitle_body = cleaned_caption

    title_detected = clean_text(str(info.get("title", "")))
    creator_detected = clean_text(
        str(info.get("uploader") or info.get("channel") or info.get("creator") or "")
    )
    publish_date = format_upload_date(str(info.get("upload_date") or ""))
    fetched_title = bool(title_detected)
    fetched_creator = bool(creator_detected)
    fetched_publish_date = bool(publish_date)
    fetched_description = bool(description_text)
    fetched_subtitle = bool(subtitle_body)
    fetched_transcript = bool(transcript_body)

    note_parts: list[str] = []
    stderr_text = clean_text(process.stderr)
    stdout_text = clean_text(process.stdout)
    platform_block_reason = detect_platform_block_reason(stderr_text, stdout_text, timed_out)
    if timed_out:
        note_parts.append(f"yt-dlp timed out after {timeout_seconds}s")
    if process.returncode != 0:
        note_parts.append(f"yt-dlp exit code {process.returncode}")
    if raw_url and raw_url != url:
        note_parts.append("canonical url extracted from share text")
    if cookies_file_value:
        note_parts.append("cookies enabled via file")
    if cookies_browser_value:
        note_parts.append(f"cookies enabled via browser:{cookies_browser_value}")
    if stderr_text:
        note_parts.append(stderr_text[:240])
    elif stdout_text and process.returncode != 0:
        note_parts.append(stdout_text[:240])

    if not title_detected and title_hint:
        title_detected = title_hint
        note_parts.append("title_detected filled from input title_hint")
    if not creator_detected and creator_input:
        creator_detected = creator_input
        note_parts.append("creator_detected filled from input creator")

    payload = {
        "task_id": task_id,
        "platform": task_row.get("platform", "").strip(),
        "url": url,
        "creator_input": creator_input,
        "creator_detected": creator_detected,
        "title_detected": title_detected,
        "publish_date": publish_date,
        "description_text": description_text,
        "subtitle_text": subtitle_body,
        "transcript_text": transcript_body,
        "page_text": "",
        "comments_text": "",
        "audio_file_path": "",
        "video_file_path": "",
        "subtitle_file_path": str(Path(subtitle_path).relative_to(PROJECT_ROOT)) if subtitle_path else "",
        "screenshot_dir": "",
        "asset_dir": str(asset_dir.relative_to(PROJECT_ROOT)),
        "ingest_method": ",".join(ingest_method_parts),
        "ingest_status": ingest_status_from_capture(
            fetched_title=fetched_title,
            fetched_creator=fetched_creator,
            fetched_publish_date=fetched_publish_date,
            fetched_description=fetched_description,
            fetched_subtitle=fetched_subtitle,
            fetched_transcript=fetched_transcript,
        ),
        "ingest_notes": "",
        "ingest_started_at": started_at,
        "ingest_finished_at": utc_now(),
    }
    captured_fields = [
        field_name
        for field_name, captured in [
            ("title_detected", fetched_title),
            ("creator_detected", fetched_creator),
            ("publish_date", fetched_publish_date),
            ("description_text", fetched_description),
            ("subtitle_text", fetched_subtitle),
            ("transcript_text", fetched_transcript),
        ]
        if captured
    ]
    missing_fields = summarize_missing_fields(payload)
    note_parts.append(
        build_ingest_diagnostic_note(
            metadata_present=bool(fetched_title or fetched_creator or fetched_publish_date),
            description_present=fetched_description,
            subtitle_present=fetched_subtitle,
            transcript_present=fetched_transcript,
            cookies_mode=cookies_mode,
            platform_block_reason=platform_block_reason,
        )
    )
    if captured_fields:
        note_parts.append("captured_fields=" + ",".join(captured_fields))
    if missing_fields:
        note_parts.append("missing_fields=" + ",".join(missing_fields))

    should_run_page_fallback = page_fallback and (
        "dou" in clean_text(task_row.get("platform", "")).lower()
        or "douyin" in url.lower()
    )
    if should_run_page_fallback and not (
        clean_text(payload.get("description_text", ""))
        or clean_text(payload.get("subtitle_text", ""))
        or clean_text(payload.get("transcript_text", ""))
        or clean_text(payload.get("page_text", ""))
    ):
        capture = run_playwright_page_capture(task_row, asset_dir, timeout_seconds=45)
        payload = merge_playwright_capture(
            payload=payload,
            capture=capture,
            note_parts=note_parts,
            title_hint=title_hint,
            creator_input=creator_input,
        )
        if ocr_fallback and clean_text(payload.get("screenshot_dir", "")):
            screenshot_dir = PROJECT_ROOT / payload["screenshot_dir"]
            ocr_result = run_ocr_on_screenshots(str(screenshot_dir))
            payload = merge_ocr_capture(payload, ocr_result, note_parts)

    payload["ingest_notes"] = "; ".join(note_parts)
    return payload


def run_ingest(
    workbook_path: Path = WORKBOOK_DEFAULT,
    cookies_file: str = "",
    cookies_from_browser: str = "",
    timeout_seconds: int = 90,
    page_fallback: bool = False,
    ocr_fallback: bool = False,
) -> dict[str, Any]:
    input_headers, input_rows = read_sheet_rows(workbook_path, "input_videos")
    _, raw_rows = read_sheet_rows(workbook_path, "raw_ingest")
    raw_by_task = records_by_key(raw_rows, "task_id")
    processed: list[dict[str, str]] = []
    for row in input_rows:
        status = row.get("status", "").strip().lower()
        if status != "pending":
            continue
        result = fetch_video_assets(
            row,
            cookies_file=cookies_file,
            cookies_from_browser=cookies_from_browser,
            timeout_seconds=timeout_seconds,
            page_fallback=page_fallback,
            ocr_fallback=ocr_fallback,
        )
        raw_by_task[result["task_id"]] = {header: result.get(header, "") for header in RAW_INGEST_HEADERS}
        row["status"] = "failed" if result["ingest_status"] == "asset_fetch_failed" else "ingested"
        row["updated_at"] = utc_now()
        if not row.get("created_at"):
            row["created_at"] = row["updated_at"]
        processed.append(
            {
                "task_id": result["task_id"],
                "ingest_status": result["ingest_status"],
                "ingest_notes": result["ingest_notes"],
            }
        )

    write_multiple_sheets(
        workbook_path,
        {
            "raw_ingest": (
                RAW_INGEST_HEADERS,
                sorted(raw_by_task.values(), key=lambda row: row.get("task_id", "")),
            ),
            "input_videos": (input_headers or INPUT_HEADERS, input_rows),
        },
    )
    return {
        "processed_count": len(processed),
        "processed_tasks": processed,
    }


def normalize_model(model: str, model_map: dict[str, str]) -> str:
    clean_model = clean_text(model)
    if not clean_model:
        return ""
    return model_map.get(clean_model, clean_model)


def infer_brand(model: str) -> str:
    lowered = model.lower()
    if "shimano" in lowered or "禧玛诺" in model:
        return "Shimano"
    if "daiwa" in lowered or "达亿瓦" in model or "达瓦" in model:
        return "Daiwa"
    if "abu garcia" in lowered or "阿布" in model:
        return "Abu Garcia"
    if "okuma" in lowered or "欧库玛" in model:
        return "Okuma"
    return ""


def infer_reel_type(text: str) -> str:
    for pattern, reel_type in TYPE_PATTERNS:
        if pattern.search(text):
            return reel_type
    return ""


def find_model_candidates(text: str) -> list[str]:
    candidates: list[str] = []
    for pattern in (MODEL_REGEX, MODEL_REGEX_CN, MODEL_REGEX_GENERIC):
        for match in pattern.findall(text or ""):
            candidate = clean_text(match)
            if 2 <= len(candidate) <= 28 and candidate not in candidates:
                candidates.append(candidate)
    return candidates


def best_model_candidate(*texts: str) -> str:
    for text in texts:
        candidates = find_model_candidates(text)
        if candidates:
            return candidates[0]
    for text in texts:
        cleaned = clean_text(text)
        if cleaned:
            return cleaned
    return ""


def split_segments(text: str) -> list[str]:
    normalized = text.replace("\r", "\n")
    parts = re.split(r"[\n]+|[。！？!?；;]+", normalized)
    segments = [clean_text(part) for part in parts]
    return [segment for segment in segments if segment]


def quote_type_for_source(source_name: str) -> str:
    return {
        "title_detected": "title",
        "description_text": "description",
        "subtitle_text": "subtitle",
        "transcript_text": "spoken",
        "page_text": "page_text",
        "comments_text": "comments",
    }.get(source_name, "manual_note")


def source_authority_for_quote(
    quote_type: str,
    creator: str,
    source_rules: dict[str, Any],
) -> str:
    creator_key = clean_text(creator)
    overrides = source_rules.get("creator_overrides", {})
    if creator_key and creator_key in overrides:
        return overrides[creator_key]
    defaults = source_rules.get("source_quote_type_defaults", {})
    return defaults.get(quote_type, "player_reported_creator")


def confidence_for_quote(quote_type: str) -> str:
    if quote_type in {"subtitle", "spoken"}:
        return "high"
    if quote_type in {"description", "page_text"}:
        return "medium"
    return "medium"


def confidence_rank(value: str) -> int:
    return {"high": 3, "medium": 2, "low": 1}.get(clean_text(value).lower(), 0)


def source_quote_type_rank(value: str) -> int:
    return SOURCE_PRIORITY.get(value, 0)


def choose_better_extract_row(current: dict[str, str], candidate: dict[str, str]) -> dict[str, str]:
    current_confidence = confidence_rank(current.get("confidence", ""))
    candidate_confidence = confidence_rank(candidate.get("confidence", ""))
    if candidate_confidence != current_confidence:
        return candidate if candidate_confidence > current_confidence else current

    current_quote_type = source_quote_type_rank(current.get("source_quote_type", ""))
    candidate_quote_type = source_quote_type_rank(candidate.get("source_quote_type", ""))
    if candidate_quote_type != current_quote_type:
        return candidate if candidate_quote_type > current_quote_type else current

    current_quote_len = len(clean_text(current.get("source_quote", ""))) or 10**9
    candidate_quote_len = len(clean_text(candidate.get("source_quote", ""))) or 10**9
    if candidate_quote_len != current_quote_len:
        return candidate if candidate_quote_len < current_quote_len else current

    return candidate if candidate.get("extract_id", "") < current.get("extract_id", "") else current


def append_record(
    records: list[dict[str, str]],
    seen: set[tuple[str, str, str, str]],
    *,
    key: tuple[str, str, str, str],
    row: dict[str, str],
) -> None:
    if key in seen:
        return
    seen.add(key)
    records.append(row)


def normalize_material_value(value: str) -> str:
    return clean_text(value)


def build_review_reasons_for_field(
    *,
    field_name: str,
    numeric_fields: list[str],
    material_fields: list[str],
    confidence: str,
) -> list[str]:
    review_reasons = ["review_required"]
    if field_name in numeric_fields:
        review_reasons.append("numeric_field")
    if field_name in material_fields:
        review_reasons.append("material_field")
    if confidence != "high":
        review_reasons.append("confidence_not_high")
    return review_reasons


def natural_material_matches(segment: str) -> list[tuple[str, str, str]]:
    matches: list[tuple[str, str, str]] = []
    for regex in NATURAL_BODY_MATERIAL_PATTERNS:
        match = regex.search(segment)
        if match:
            matches.append(("body_material", clean_text(match.group(1)), "regex-natural"))
    for regex in NATURAL_MAIN_GEAR_MATERIAL_PATTERNS:
        match = regex.search(segment)
        if match:
            matches.append(("main_gear_material", clean_text(match.group(1)), "regex-natural"))
    for regex in NATURAL_MINOR_GEAR_MATERIAL_PATTERNS:
        match = regex.search(segment)
        if match:
            matches.append(("minor_gear_material", clean_text(match.group(1)), "regex-natural"))
    for regex in NATURAL_SHARED_GEAR_MATERIAL_PATTERNS:
        match = regex.search(segment)
        if match:
            material_value = clean_text(match.group(1))
            matches.append(("main_gear_material", material_value, "regex-natural-shared"))
            matches.append(("minor_gear_material", material_value, "regex-natural-shared"))
    deduped: dict[tuple[str, str], tuple[str, str, str]] = {}
    for field_name, material_value, extraction_method in matches:
        deduped[(field_name, material_value)] = (field_name, material_value, extraction_method)
    return list(deduped.values())


def natural_spool_axis_match(segment: str) -> tuple[str, str, str] | None:
    for pattern in NATURAL_SPOOL_AXIS_PATTERNS:
        match = pattern["regex"].search(segment)
        if match:
            return (
                clean_text(match.group(1)),
                clean_text(pattern["normalized"]),
                clean_text(pattern["extract_notes"]),
            )
    return None


def normalize_value(field_name: str, value: str) -> tuple[str, str]:
    cleaned = clean_text(value)
    if field_name in {
        "spool_diameter_mm",
        "spool_width_mm",
    }:
        return re.sub(r"[^\d.]", "", cleaned), "mm"
    if field_name in {"spool_weight_g"}:
        return re.sub(r"[^\d.]", "", cleaned), "g"
    if field_name == "main_gear_size":
        return cleaned, ""
    return cleaned, ""


def extract_rows_from_record(
    raw_row: dict[str, str],
    field_rules: dict[str, Any],
    source_rules: dict[str, Any],
    model_map: dict[str, str],
) -> list[dict[str, str]]:
    allowed_fields = set(field_rules["reel_player_fields"])
    numeric_fields = field_rules["numeric_fields"]
    material_fields = field_rules["material_fields"]
    records: list[dict[str, str]] = []
    seen: set[tuple[str, str, str, str]] = set()

    source_fields = [
        "subtitle_text",
        "transcript_text",
        "description_text",
        "page_text",
    ]

    combined_text = "\n".join(raw_row.get(name, "") for name in source_fields)
    global_models = find_model_candidates(combined_text)
    fallback_model = global_models[0] if global_models else ""

    primary_model = best_model_candidate(
        raw_row.get("title_detected", ""),
        raw_row.get("description_text", ""),
        raw_row.get("subtitle_text", ""),
        raw_row.get("transcript_text", ""),
        raw_row.get("url", ""),
    )
    if primary_model and "model_cn" in allowed_fields:
        normalized_model = normalize_model(primary_model, model_map)
        brand = infer_brand(normalized_model or primary_model)
        model_source_quote = clean_text(raw_row.get("title_detected", "")) or clean_text(raw_row.get("url", ""))
        model_quote_type = "title" if clean_text(raw_row.get("title_detected", "")) else "manual_note"
        model_confidence = "medium" if model_quote_type == "title" else "low"
        model_review_reasons = ["review_required"]
        if model_confidence != "high":
            model_review_reasons.append("confidence_not_high")
        records.append(
            build_extract_row(
                raw_row=raw_row,
                field_name="model_cn",
                field_value_raw=primary_model,
                field_value_normalized=normalized_model,
                unit="",
                value_type="text",
                source_quote=model_source_quote,
                source_quote_type=model_quote_type,
                source_authority=source_authority_for_quote(
                    model_quote_type,
                    raw_row.get("creator_detected") or raw_row.get("creator_input") or raw_row.get("creator", ""),
                    source_rules,
                ),
                confidence=model_confidence,
                review_reasons=model_review_reasons,
                extraction_method="model-detection",
                reel_model_raw=primary_model,
                reel_model_normalized=normalized_model,
                reel_brand_normalized=brand,
                reel_type_guess=infer_reel_type(raw_row.get("title_detected", "") or raw_row.get("description_text", "")),
            )
        )

    for source_name in source_fields:
        source_text = raw_row.get(source_name, "")
        if not clean_text(source_text):
            continue
        quote_type = quote_type_for_source(source_name)
        segments = split_segments(source_text)
        if source_name == "title_detected" and clean_text(source_text) not in segments:
            segments.insert(0, clean_text(source_text))
        for segment in segments:
            local_models = find_model_candidates(segment)
            reel_model_raw = local_models[0] if local_models else fallback_model
            reel_model_normalized = normalize_model(reel_model_raw, model_map)
            reel_brand = infer_brand(reel_model_normalized or reel_model_raw)
            reel_type = infer_reel_type(segment or raw_row.get("title_detected", ""))

            for field_name, matchers in NUMERIC_PATTERNS.items():
                if field_name not in allowed_fields or field_name not in ACTIVE_EXTRACT_FIELDS:
                    continue
                for regex in matchers:
                    match = regex.search(segment)
                    if not match:
                        continue
                    value_raw = clean_text(match.group(1))
                    value_normalized, normalized_unit = normalize_value(field_name, value_raw)
                    final_unit = normalized_unit
                    key = (field_name, value_normalized, segment, reel_model_raw)
                    confidence = confidence_for_quote(quote_type)
                    review_reasons = build_review_reasons_for_field(
                        field_name=field_name,
                        numeric_fields=numeric_fields,
                        material_fields=material_fields,
                        confidence=confidence,
                    )
                    append_record(
                        records,
                        seen,
                        key=key,
                        row=build_extract_row(
                            raw_row=raw_row,
                            field_name=field_name,
                            field_value_raw=value_raw,
                            field_value_normalized=value_normalized,
                            unit=final_unit,
                            value_type="number",
                            source_quote=segment,
                            source_quote_type=quote_type,
                            source_authority=source_authority_for_quote(quote_type, raw_row.get("creator_detected") or raw_row.get("creator_input") or raw_row.get("creator", ""), source_rules),
                            confidence=confidence,
                            review_reasons=review_reasons,
                            extraction_method="regex-explicit",
                            reel_model_raw=reel_model_raw,
                            reel_model_normalized=reel_model_normalized,
                            reel_brand_normalized=reel_brand,
                            reel_type_guess=reel_type,
                        ),
                    )

            for field_name, regex in MATERIAL_PATTERNS.items():
                if field_name not in allowed_fields or field_name not in ACTIVE_EXTRACT_FIELDS:
                    continue
                match = regex.search(segment)
                if not match:
                    continue
                value_raw = clean_text(match.group(1))
                value_normalized, unit = normalize_value(field_name, value_raw)
                key = (field_name, value_normalized, segment, reel_model_raw)
                confidence = confidence_for_quote(quote_type)
                review_reasons = build_review_reasons_for_field(
                    field_name=field_name,
                    numeric_fields=numeric_fields,
                    material_fields=material_fields,
                    confidence=confidence,
                )
                append_record(
                    records,
                    seen,
                    key=key,
                    row=build_extract_row(
                        raw_row=raw_row,
                        field_name=field_name,
                        field_value_raw=value_raw,
                        field_value_normalized=value_normalized,
                        unit=unit,
                        value_type="material",
                        source_quote=segment,
                        source_quote_type=quote_type,
                        source_authority=source_authority_for_quote(quote_type, raw_row.get("creator_detected") or raw_row.get("creator_input") or raw_row.get("creator", ""), source_rules),
                        confidence=confidence,
                        review_reasons=review_reasons,
                        extraction_method="regex-explicit",
                        reel_model_raw=reel_model_raw,
                        reel_model_normalized=reel_model_normalized,
                        reel_brand_normalized=reel_brand,
                        reel_type_guess=reel_type,
                    ),
                )

            for field_name, material_value, extraction_method in natural_material_matches(segment):
                if field_name not in allowed_fields or field_name not in ACTIVE_EXTRACT_FIELDS:
                    continue
                value_raw = normalize_material_value(material_value)
                value_normalized, unit = normalize_value(field_name, value_raw)
                key = (field_name, value_normalized, segment, reel_model_raw)
                confidence = "medium" if quote_type in {"description", "page_text"} else "low"
                review_reasons = build_review_reasons_for_field(
                    field_name=field_name,
                    numeric_fields=numeric_fields,
                    material_fields=material_fields,
                    confidence=confidence,
                )
                append_record(
                    records,
                    seen,
                    key=key,
                    row=build_extract_row(
                        raw_row=raw_row,
                        field_name=field_name,
                        field_value_raw=value_raw,
                        field_value_normalized=value_normalized,
                        unit=unit,
                        value_type="material",
                        source_quote=segment,
                        source_quote_type=quote_type,
                        source_authority=source_authority_for_quote(quote_type, raw_row.get("creator_detected") or raw_row.get("creator_input") or raw_row.get("creator", ""), source_rules),
                        confidence=confidence,
                        review_reasons=review_reasons,
                        extraction_method=extraction_method,
                        reel_model_raw=reel_model_raw,
                        reel_model_normalized=reel_model_normalized,
                        reel_brand_normalized=reel_brand,
                        reel_type_guess=reel_type,
                    ),
                )

            for field_name, regex in TEXT_PATTERNS.items():
                if field_name not in allowed_fields or field_name not in ACTIVE_EXTRACT_FIELDS:
                    continue
                match = regex.search(segment)
                if not match:
                    continue
                value_raw = clean_text(match.group(1) if match.groups() else segment)
                value_normalized, unit = normalize_value(field_name, value_raw)
                key = (field_name, value_normalized, segment, reel_model_raw)
                confidence = confidence_for_quote(quote_type)
                review_reasons = build_review_reasons_for_field(
                    field_name=field_name,
                    numeric_fields=numeric_fields,
                    material_fields=material_fields,
                    confidence=confidence,
                )
                append_record(
                    records,
                    seen,
                    key=key,
                    row=build_extract_row(
                        raw_row=raw_row,
                        field_name=field_name,
                        field_value_raw=value_raw,
                        field_value_normalized=value_normalized,
                        unit=unit,
                        value_type="text",
                        source_quote=segment,
                        source_quote_type=quote_type,
                        source_authority=source_authority_for_quote(quote_type, raw_row.get("creator_detected") or raw_row.get("creator_input") or raw_row.get("creator", ""), source_rules),
                        confidence=confidence,
                        review_reasons=review_reasons,
                        extraction_method="keyword-explicit",
                        reel_model_raw=reel_model_raw,
                        reel_model_normalized=reel_model_normalized,
                        reel_brand_normalized=reel_brand,
                        reel_type_guess=reel_type,
                    ),
                )

            natural_axis = natural_spool_axis_match(segment)
            if natural_axis and "spool_axis_type" in allowed_fields and "spool_axis_type" in ACTIVE_EXTRACT_FIELDS:
                value_raw, value_normalized, extract_notes = natural_axis
                key = ("spool_axis_type", value_normalized, segment, reel_model_raw)
                confidence = "low"
                review_reasons = build_review_reasons_for_field(
                    field_name="spool_axis_type",
                    numeric_fields=numeric_fields,
                    material_fields=material_fields,
                    confidence=confidence,
                )
                append_record(
                    records,
                    seen,
                    key=key,
                    row=build_extract_row(
                        raw_row=raw_row,
                        field_name="spool_axis_type",
                        field_value_raw=value_raw,
                        field_value_normalized=value_normalized,
                        unit="",
                        value_type="text",
                        source_quote=segment,
                        source_quote_type=quote_type,
                        source_authority=source_authority_for_quote(quote_type, raw_row.get("creator_detected") or raw_row.get("creator_input") or raw_row.get("creator", ""), source_rules),
                        confidence=confidence,
                        review_reasons=review_reasons,
                        extraction_method="regex-natural",
                        reel_model_raw=reel_model_raw,
                        reel_model_normalized=reel_model_normalized,
                        reel_brand_normalized=reel_brand,
                        reel_type_guess=reel_type,
                        extract_notes=extract_notes,
                    ),
                )

    return records


def build_extract_row(
    raw_row: dict[str, str],
    field_name: str,
    field_value_raw: str,
    field_value_normalized: str,
    unit: str,
    value_type: str,
    source_quote: str,
    source_quote_type: str,
    source_authority: str,
    confidence: str,
    review_reasons: list[str],
    extraction_method: str,
    reel_model_raw: str,
    reel_model_normalized: str,
    reel_brand_normalized: str,
    reel_type_guess: str,
    extract_notes: str = "",
) -> dict[str, str]:
    extract_id = stable_id(
        "ext",
        raw_row.get("task_id", ""),
        reel_model_raw,
        field_name,
        field_value_normalized or field_value_raw,
        source_quote,
    )
    return {
        "extract_id": extract_id,
        "task_id": raw_row.get("task_id", ""),
        "platform": raw_row.get("platform", ""),
        "url": raw_row.get("url", ""),
        "creator": raw_row.get("creator_detected", "") or raw_row.get("creator_input", ""),
        "publish_date": raw_row.get("publish_date", ""),
        "reel_model_raw": reel_model_raw,
        "reel_model_normalized": reel_model_normalized,
        "reel_brand_normalized": reel_brand_normalized,
        "reel_type_guess": reel_type_guess,
        "field_name": field_name,
        "field_value_raw": field_value_raw,
        "field_value_normalized": field_value_normalized,
        "unit": unit,
        "value_type": value_type,
        "source_quote": source_quote,
        "source_quote_type": source_quote_type,
        "source_authority": source_authority,
        "confidence": confidence,
        "review_required": "yes" if review_reasons else "no",
        "review_reason": ",".join(review_reasons),
        "extraction_method": extraction_method,
        "extract_notes": extract_notes,
        "created_at": utc_now(),
    }


def run_extract(workbook_path: Path = WORKBOOK_DEFAULT) -> dict[str, Any]:
    _, raw_rows = read_sheet_rows(workbook_path, "raw_ingest")
    field_rules = load_field_rules()
    source_rules = load_source_authority_rules()
    model_map = load_model_normalization()
    extracted_rows: list[dict[str, str]] = []
    for raw_row in raw_rows:
        if not any(
            clean_text(raw_row.get(field_name, ""))
            for field_name in ["title_detected", "subtitle_text", "transcript_text", "description_text", "page_text"]
        ):
            continue
        extracted_rows.extend(extract_rows_from_record(raw_row, field_rules, source_rules, model_map))
    deduped: dict[tuple[str, str, str, str], dict[str, str]] = {}
    for row in extracted_rows:
        dedupe_key = (
            row.get("task_id", ""),
            row.get("reel_model_normalized", "") or row.get("reel_model_raw", ""),
            row.get("field_name", ""),
            row.get("field_value_normalized", "") or row.get("field_value_raw", ""),
        )
        existing = deduped.get(dedupe_key)
        deduped[dedupe_key] = row if existing is None else choose_better_extract_row(existing, row)
    extracted_rows = sorted(
        deduped.values(),
        key=lambda row: (row["task_id"], row["field_name"], row["extract_id"]),
    )
    write_sheet_rows(workbook_path, "player_data_extract", PLAYER_EXTRACT_HEADERS, extracted_rows)
    return {
        "extract_count": len(extracted_rows),
        "task_count": len({row["task_id"] for row in extracted_rows}),
    }


def should_review(row: dict[str, str], field_rules: dict[str, Any]) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    field_name = row.get("field_name", "")
    if row.get("review_required", "").strip().lower() == "yes":
        reasons.append("review_required")
    return bool(reasons), reasons


def build_review_rows(
    extract_rows: list[dict[str, str]],
    existing_review_rows: list[dict[str, str]],
    field_rules: dict[str, Any],
) -> list[dict[str, str]]:
    existing_by_extract = records_by_key(existing_review_rows, "extract_id")
    review_rows: list[dict[str, str]] = []
    for extract_row in extract_rows:
        include, reasons = should_review(extract_row, field_rules)
        if not include:
            continue
        existing = existing_by_extract.get(extract_row["extract_id"], {})
        candidate_value = extract_row.get("field_value_normalized", "") or extract_row.get("field_value_raw", "")
        review_id = stable_id("rev", extract_row["extract_id"])
        review_rows.append(
            {
                "review_id": review_id,
                "extract_id": extract_row["extract_id"],
                "task_id": extract_row.get("task_id", ""),
                "reel_model_normalized": extract_row.get("reel_model_normalized", ""),
                "reel_model_raw": extract_row.get("reel_model_raw", ""),
                "field_name": extract_row.get("field_name", ""),
                "candidate_value": candidate_value,
                "candidate_value_raw": extract_row.get("field_value_raw", ""),
                "unit": extract_row.get("unit", ""),
                "confidence": extract_row.get("confidence", ""),
                "source_quote": extract_row.get("source_quote", ""),
                "source_quote_type": extract_row.get("source_quote_type", ""),
                "source_url": extract_row.get("url", ""),
                "source_author": extract_row.get("creator", ""),
                "source_authority": extract_row.get("source_authority", ""),
                "review_action": existing.get("review_action", ""),
                "review_value": existing.get("review_value", ""),
                "review_comment": existing.get("review_comment", "") or ",".join(reasons),
                "reviewer": existing.get("reviewer", ""),
                "reviewed_at": existing.get("reviewed_at", ""),
            }
        )
    return review_rows


def run_review_queue(workbook_path: Path = WORKBOOK_DEFAULT) -> dict[str, Any]:
    _, extract_rows = read_sheet_rows(workbook_path, "player_data_extract")
    _, existing_review_rows = read_sheet_rows(workbook_path, "review_queue")
    field_rules = load_field_rules()
    review_rows = build_review_rows(extract_rows, existing_review_rows, field_rules)
    write_sheet_rows(workbook_path, "review_queue", REVIEW_QUEUE_HEADERS, review_rows)
    return {
        "review_count": len(review_rows),
        "task_count": len({row["task_id"] for row in review_rows}),
    }


def print_json(data: dict[str, Any]) -> None:
    print(json.dumps(data, ensure_ascii=False, indent=2))
