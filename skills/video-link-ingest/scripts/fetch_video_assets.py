from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from shared.reel_pipeline import CACHE_ROOT, fetch_video_assets, print_json


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch yt-dlp metadata and subtitle assets for one video.")
    parser.add_argument("--task-id", required=True)
    parser.add_argument("--platform", default="")
    parser.add_argument("--url", required=True)
    parser.add_argument("--creator", default="")
    parser.add_argument("--language", default="")
    parser.add_argument(
        "--cache-root",
        default=str(CACHE_ROOT),
        help="Directory used to store downloaded metadata and subtitles.",
    )
    args = parser.parse_args()
    result = fetch_video_assets(
        {
            "task_id": args.task_id,
            "platform": args.platform,
            "url": args.url,
            "creator": args.creator,
            "language": args.language,
        },
        cache_root=Path(args.cache_root).resolve(),
    )
    print_json(result)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
