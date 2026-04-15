from __future__ import annotations

import hashlib
import json
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

NUMERIC_PATTERNS = {
    "model_year": [
        (re.compile(r"(?:年款|年份|model year)\s*[:：]?\s*(20\d{2})", re.I), "", "number"),
    ],
    "spool_diameter_mm": [
        (re.compile(r"(?:线杯直径|杯径|spool diameter)\s*[:：]?\s*(\d+(?:\.\d+)?)\s*mm", re.I), "mm", "number"),
    ],
    "spool_width_mm": [
        (re.compile(r"(?:线杯宽度|杯宽|spool width)\s*[:：]?\s*(\d+(?:\.\d+)?)\s*mm", re.I), "mm", "number"),
    ],
    "spool_weight_g": [
        (re.compile(r"(?:线杯重量|杯重|spool weight)\s*[:：]?\s*(\d+(?:\.\d+)?)\s*g", re.I), "g", "number"),
    ],
    "knob_size": [
        (re.compile(r"(?:握丸尺寸|knob size)\s*[:：]?\s*(\d+(?:\.\d+)?)\s*mm", re.I), "mm", "number"),
    ],
    "handle_knob_exchange_size": [
        (re.compile(r"(?:握丸互换规格|knob exchange size)\s*[:：]?\s*(\d+(?:\.\d+)?)\s*mm", re.I), "mm", "number"),
    ],
    "main_gear_size": [
        (re.compile(r"(?:主齿尺寸|main gear size)\s*[:：]?\s*(\d+(?:\.\d+)?)", re.I), "", "number"),
    ],
    "market_reference_price": [
        (re.compile(r"(?:参考价|价格|price)\s*[:：]?\s*(?:rmb|¥|￥)?\s*(\d+(?:\.\d+)?)", re.I), "CNY", "number"),
    ],
}

MATERIAL_PATTERNS = {
    "handle_knob_material": re.compile(r"(?:握丸材质|knob material)\s*[:：]?\s*([A-Za-z\u4e00-\u9fff0-9 \-/]+)", re.I),
    "body_material": re.compile(r"(?:机身材质|body material)\s*[:：]?\s*([A-Za-z\u4e00-\u9fff0-9 \-/]+)", re.I),
    "main_gear_material": re.compile(r"(?:主齿材质|main gear material)\s*[:：]?\s*([A-Za-z\u4e00-\u9fff0-9 \-/]+)", re.I),
    "minor_gear_material": re.compile(r"(?:小齿材质|minor gear material)\s*[:：]?\s*([A-Za-z\u4e00-\u9fff0-9 \-/]+)", re.I),
}

TEXT_PATTERNS = {
    "drag_click": re.compile(r"(?:drag click|泄力(?:有|带)?(?:咔嗒|哒哒)|有泄力音)", re.I),
    "spool_axis_type": re.compile(r"(?:线杯轴|spool axis)\s*[:：]?\s*([A-Za-z\u4e00-\u9fff0-9 \-/]+)", re.I),
    "knob_bearing_spec": re.compile(r"(?:握丸轴承规格|knob bearing)\s*[:：]?\s*([A-Za-z0-9xX* \-/]+)", re.I),
    "handle_knob_type": re.compile(r"(?:握丸类型|knob type)\s*[:：]?\s*([A-Za-z\u4e00-\u9fff0-9 \-/]+)", re.I),
    "handle_hole_spec": re.compile(r"(?:把手孔位|handle hole spec)\s*[:：]?\s*([A-Za-z0-9xX* \-/]+)", re.I),
    "player_environment": re.compile(r"(?:使用环境|适合环境|player environment)\s*[:：]?\s*([A-Za-z\u4e00-\u9fff0-9 ，,/\-]+)", re.I),
    "player_positioning": re.compile(r"(?:定位|player positioning)\s*[:：]?\s*([A-Za-z\u4e00-\u9fff0-9 ，,/\-]+)", re.I),
    "player_selling_points": re.compile(r"(?:卖点|selling point[s]?)\s*[:：]?\s*([A-Za-z\u4e00-\u9fff0-9 ，,/\-]+)", re.I),
}

BOOLEAN_PATTERNS = {
    "is_handle_double": re.compile(r"(?:双摇臂|双把|double handle)", re.I),
}

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


def clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


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


def fetch_video_assets(task_row: dict[str, str], cache_root: Path = CACHE_ROOT) -> dict[str, str]:
    task_id = task_row.get("task_id", "").strip()
    url = task_row.get("url", "").strip()
    language_hint = task_row.get("language", "").strip()
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
            "ingest_status": "failed",
            "ingest_notes": "Missing task_id or url",
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
        url,
    ]
    process = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=True,
        check=False,
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
    subtitle_path = ""
    subtitle_candidates = preferred_subtitle_files(asset_dir, language_hint)
    if subtitle_candidates:
        subtitle_raw, subtitle_path = read_first_match(subtitle_candidates)
        subtitle_body = clean_vtt(subtitle_raw)

    title_detected = clean_text(str(info.get("title", "")))
    creator_detected = clean_text(
        str(info.get("uploader") or info.get("channel") or info.get("creator") or "")
    )
    publish_date = format_upload_date(str(info.get("upload_date") or ""))

    note_parts: list[str] = []
    stderr_text = clean_text(process.stderr)
    stdout_text = clean_text(process.stdout)
    if process.returncode != 0:
        note_parts.append(f"yt-dlp exit code {process.returncode}")
    if not title_detected:
        note_parts.append("title missing")
    if not description_text:
        note_parts.append("description missing")
    if not subtitle_body:
        note_parts.append("subtitle missing")
    if stderr_text:
        note_parts.append(stderr_text[:240])
    elif stdout_text and process.returncode != 0:
        note_parts.append(stdout_text[:240])

    useful_payload = bool(title_detected or description_text or subtitle_body or creator_detected)
    if title_detected and (description_text or subtitle_body):
        status = "success"
    elif useful_payload:
        status = "partial"
    else:
        status = "failed"

    return {
        "task_id": task_id,
        "platform": task_row.get("platform", "").strip(),
        "url": url,
        "creator_input": task_row.get("creator", "").strip(),
        "creator_detected": creator_detected,
        "title_detected": title_detected,
        "publish_date": publish_date,
        "description_text": description_text,
        "subtitle_text": subtitle_body,
        "transcript_text": "",
        "page_text": "",
        "comments_text": "",
        "audio_file_path": "",
        "video_file_path": "",
        "subtitle_file_path": str(Path(subtitle_path).relative_to(PROJECT_ROOT)) if subtitle_path else "",
        "screenshot_dir": "",
        "asset_dir": str(asset_dir.relative_to(PROJECT_ROOT)),
        "ingest_method": "yt-dlp",
        "ingest_status": status,
        "ingest_notes": "; ".join(note_parts),
        "ingest_started_at": started_at,
        "ingest_finished_at": utc_now(),
    }


def run_ingest(workbook_path: Path = WORKBOOK_DEFAULT) -> dict[str, Any]:
    input_headers, input_rows = read_sheet_rows(workbook_path, "input_videos")
    _, raw_rows = read_sheet_rows(workbook_path, "raw_ingest")
    raw_by_task = records_by_key(raw_rows, "task_id")
    processed: list[dict[str, str]] = []
    for row in input_rows:
        status = row.get("status", "").strip().lower()
        if status != "pending":
            continue
        result = fetch_video_assets(row)
        raw_by_task[result["task_id"]] = {header: result.get(header, "") for header in RAW_INGEST_HEADERS}
        row["status"] = "ingested" if result["ingest_status"] == "success" else result["ingest_status"]
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
    for pattern in (MODEL_REGEX, MODEL_REGEX_CN):
        for match in pattern.findall(text or ""):
            candidate = clean_text(match)
            if 2 <= len(candidate) <= 28 and candidate not in candidates:
                candidates.append(candidate)
    return candidates


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


def normalize_value(field_name: str, value: str) -> tuple[str, str]:
    cleaned = clean_text(value)
    if field_name in {"market_reference_price"}:
        return re.sub(r"[^\d.]", "", cleaned), "CNY"
    if field_name in {
        "spool_diameter_mm",
        "spool_width_mm",
        "knob_size",
        "handle_knob_exchange_size",
    }:
        return re.sub(r"[^\d.]", "", cleaned), "mm"
    if field_name in {"spool_weight_g"}:
        return re.sub(r"[^\d.]", "", cleaned), "g"
    if field_name == "is_handle_double":
        lowered = cleaned.lower()
        if lowered in {"yes", "true", "1"}:
            return "yes", ""
        return "yes", ""
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
        "title_detected",
        "description_text",
        "subtitle_text",
        "transcript_text",
        "page_text",
        "comments_text",
    ]

    combined_text = "\n".join(raw_row.get(name, "") for name in source_fields)
    global_models = find_model_candidates(combined_text)
    fallback_model = global_models[0] if global_models else ""

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
                if field_name not in allowed_fields:
                    continue
                for regex, unit, value_type in matchers:
                    match = regex.search(segment)
                    if not match:
                        continue
                    value_raw = match.group(1)
                    value_normalized, normalized_unit = normalize_value(field_name, value_raw)
                    final_unit = unit or normalized_unit
                    key = (field_name, value_normalized, segment, reel_model_raw)
                    if key in seen:
                        continue
                    seen.add(key)
                    confidence = confidence_for_quote(quote_type)
                    review_reasons = []
                    if field_name in numeric_fields:
                        review_reasons.append("numeric_field")
                    if field_name in material_fields:
                        review_reasons.append("material_field")
                    if confidence != "high":
                        review_reasons.append("confidence_not_high")
                    records.append(
                        build_extract_row(
                            raw_row=raw_row,
                            field_name=field_name,
                            field_value_raw=value_raw,
                            field_value_normalized=value_normalized,
                            unit=final_unit,
                            value_type=value_type,
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
                        )
                    )

            for field_name, regex in MATERIAL_PATTERNS.items():
                if field_name not in allowed_fields:
                    continue
                match = regex.search(segment)
                if not match:
                    continue
                value_raw = clean_text(match.group(1))
                value_normalized, unit = normalize_value(field_name, value_raw)
                key = (field_name, value_normalized, segment, reel_model_raw)
                if key in seen:
                    continue
                seen.add(key)
                confidence = confidence_for_quote(quote_type)
                review_reasons = ["material_field"]
                if confidence != "high":
                    review_reasons.append("confidence_not_high")
                records.append(
                    build_extract_row(
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
                    )
                )

            for field_name, regex in TEXT_PATTERNS.items():
                if field_name not in allowed_fields:
                    continue
                match = regex.search(segment)
                if not match:
                    continue
                value_raw = clean_text(match.group(1) if match.groups() else segment)
                value_normalized, unit = normalize_value(field_name, value_raw)
                key = (field_name, value_normalized, segment, reel_model_raw)
                if key in seen:
                    continue
                seen.add(key)
                confidence = confidence_for_quote(quote_type)
                review_reasons = []
                if field_name in numeric_fields:
                    review_reasons.append("numeric_field")
                if field_name in material_fields:
                    review_reasons.append("material_field")
                if confidence != "high":
                    review_reasons.append("confidence_not_high")
                records.append(
                    build_extract_row(
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
                    )
                )

            for field_name, regex in BOOLEAN_PATTERNS.items():
                if field_name not in allowed_fields:
                    continue
                if not regex.search(segment):
                    continue
                value_raw = "yes"
                key = (field_name, value_raw, segment, reel_model_raw)
                if key in seen:
                    continue
                seen.add(key)
                confidence = confidence_for_quote(quote_type)
                review_reasons = []
                if confidence != "high":
                    review_reasons.append("confidence_not_high")
                records.append(
                    build_extract_row(
                        raw_row=raw_row,
                        field_name=field_name,
                        field_value_raw=value_raw,
                        field_value_normalized="yes",
                        unit="",
                        value_type="boolean",
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
                    )
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
        "extract_notes": "",
        "created_at": utc_now(),
    }


def run_extract(workbook_path: Path = WORKBOOK_DEFAULT) -> dict[str, Any]:
    _, raw_rows = read_sheet_rows(workbook_path, "raw_ingest")
    field_rules = load_field_rules()
    source_rules = load_source_authority_rules()
    model_map = load_model_normalization()
    extracted_rows: list[dict[str, str]] = []
    for raw_row in raw_rows:
        if raw_row.get("ingest_status", "").strip() not in {"success", "partial"}:
            continue
        extracted_rows.extend(extract_rows_from_record(raw_row, field_rules, source_rules, model_map))
    extracted_rows.sort(key=lambda row: (row["task_id"], row["field_name"], row["extract_id"]))
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
    if row.get("confidence", "").strip().lower() in {"medium", "low"}:
        reasons.append("confidence_check")
    if field_name in field_rules["numeric_fields"]:
        reasons.append("numeric_field")
    if field_name in field_rules["material_fields"]:
        reasons.append("material_field")
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
