from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from shared.reel_pipeline import load_field_rules, read_sheet_rows, should_review


def main() -> int:
    parser = argparse.ArgumentParser(description="Preview player_data_extract rows that require review.")
    parser.add_argument(
        "--workbook",
        default=str(PROJECT_ROOT / "data" / "input" / "reel_player_data_pipeline_v1.xlsx"),
    )
    args = parser.parse_args()

    _, extract_rows = read_sheet_rows(Path(args.workbook).resolve(), "player_data_extract")
    field_rules = load_field_rules()
    preview = []
    for row in extract_rows:
        include, reasons = should_review(row, field_rules)
        if include:
            preview.append(
                {
                    "extract_id": row.get("extract_id", ""),
                    "field_name": row.get("field_name", ""),
                    "candidate_value": row.get("field_value_normalized", "") or row.get("field_value_raw", ""),
                    "reasons": reasons,
                }
            )
    print(json.dumps(preview, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
