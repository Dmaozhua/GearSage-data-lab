from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from shared.reel_pipeline import normalize_value


def main() -> int:
    parser = argparse.ArgumentParser(description="Normalize a single extracted value.")
    parser.add_argument("--field-name", required=True)
    parser.add_argument("--value", required=True)
    args = parser.parse_args()
    normalized_value, unit = normalize_value(args.field_name, args.value)
    print(
        json.dumps(
            {
                "field_name": args.field_name,
                "value_raw": args.value,
                "value_normalized": normalized_value,
                "unit": unit,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
