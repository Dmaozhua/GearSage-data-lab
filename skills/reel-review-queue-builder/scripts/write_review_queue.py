from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from shared.reel_pipeline import REVIEW_QUEUE_HEADERS, WORKBOOK_DEFAULT, write_sheet_rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Write review_queue rows into the workbook.")
    parser.add_argument("--workbook", default=str(WORKBOOK_DEFAULT))
    parser.add_argument("--input-json", required=True)
    args = parser.parse_args()

    payload = json.loads(Path(args.input_json).read_text(encoding="utf-8"))
    rows = payload if isinstance(payload, list) else [payload]
    normalized_rows = [
        {header: str(row.get(header, "")) for header in REVIEW_QUEUE_HEADERS}
        for row in rows
    ]
    write_sheet_rows(Path(args.workbook).resolve(), "review_queue", REVIEW_QUEUE_HEADERS, normalized_rows)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
