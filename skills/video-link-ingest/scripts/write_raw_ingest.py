from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from shared.reel_pipeline import RAW_INGEST_HEADERS, WORKBOOK_DEFAULT, read_sheet_rows, write_sheet_rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Write raw ingest rows into the workbook.")
    parser.add_argument("--workbook", default=str(WORKBOOK_DEFAULT))
    parser.add_argument("--input-json", required=True, help="Path to a JSON file containing one row or a list of rows.")
    args = parser.parse_args()

    payload = json.loads(Path(args.input_json).read_text(encoding="utf-8"))
    rows_to_write = payload if isinstance(payload, list) else [payload]
    _, existing_rows = read_sheet_rows(Path(args.workbook).resolve(), "raw_ingest")
    by_task = {row.get("task_id", ""): row for row in existing_rows if row.get("task_id", "")}
    for row in rows_to_write:
        by_task[row.get("task_id", "")] = {header: str(row.get(header, "")) for header in RAW_INGEST_HEADERS}
    write_sheet_rows(Path(args.workbook).resolve(), "raw_ingest", RAW_INGEST_HEADERS, list(by_task.values()))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
