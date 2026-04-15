from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from equipment_scoring import load_scored_inventory_candidates
from shared.reel_pipeline import WORKBOOK_DEFAULT, print_json


def main() -> int:
    parser = argparse.ArgumentParser(description="Score creator_video_inventory rows for equipment and parameter relevance.")
    parser.add_argument("--workbook", default=str(WORKBOOK_DEFAULT))
    parser.add_argument("--limit", type=int, default=30)
    parser.add_argument("--include-non-pending", action="store_true")
    args = parser.parse_args()

    workbook_path = Path(args.workbook).resolve()
    scored_rows = load_scored_inventory_candidates(
        workbook_path,
        pending_only=not args.include_non_pending,
    )
    top_rows = scored_rows[: args.limit]
    print_json(
        {
            "candidate_count": len(scored_rows),
            "returned_count": len(top_rows),
            "rows": top_rows,
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
