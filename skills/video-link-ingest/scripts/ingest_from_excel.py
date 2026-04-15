from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from shared.reel_pipeline import WORKBOOK_DEFAULT, print_json, run_ingest


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the minimal video ingest workflow.")
    parser.add_argument(
        "--workbook",
        default=str(WORKBOOK_DEFAULT),
        help="Path to the Excel workbook.",
    )
    args = parser.parse_args()
    result = run_ingest(Path(args.workbook).resolve())
    print_json(result)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
