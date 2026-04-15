from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from shared.reel_pipeline import find_model_candidates, infer_reel_type


def main() -> int:
    parser = argparse.ArgumentParser(description="Extract conservative reel model candidates from free text.")
    parser.add_argument("--text", default="")
    parser.add_argument("--input-file", default="")
    args = parser.parse_args()

    text = args.text
    if args.input_file:
        text = Path(args.input_file).read_text(encoding="utf-8")

    print(
        json.dumps(
            {
                "models": find_model_candidates(text),
                "reel_type_guess": infer_reel_type(text),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
