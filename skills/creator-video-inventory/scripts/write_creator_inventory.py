from __future__ import annotations

import argparse
import json
import sys
import importlib.util
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

ENUMERATOR_PATH = PROJECT_ROOT / "skills" / "creator-video-inventory" / "scripts" / "enumerate_creator_inventory.py"


def load_merge_creator_inventory():
    spec = importlib.util.spec_from_file_location("creator_video_inventory_enumerator", ENUMERATOR_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load enumerator module from {ENUMERATOR_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.merge_creator_inventory


def main() -> int:
    parser = argparse.ArgumentParser(description="Write a creator inventory JSON result into workbook sheets.")
    parser.add_argument("--workbook", required=True)
    parser.add_argument("--input-json", required=True)
    parser.add_argument("--append-to-input", action="store_true")
    args = parser.parse_args()

    payload = json.loads(Path(args.input_json).read_text(encoding="utf-8"))
    merge_creator_inventory = load_merge_creator_inventory()
    summary = merge_creator_inventory(
        Path(args.workbook).resolve(),
        payload,
        append_to_input=args.append_to_input,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
