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
    parser.add_argument("--title-hint", default="")
    parser.add_argument("--language", default="")
    parser.add_argument(
        "--cache-root",
        default=str(CACHE_ROOT),
        help="Directory used to store downloaded metadata and subtitles.",
    )
    parser.add_argument(
        "--cookies-file",
        default="",
        help="Optional Netscape cookies file for yt-dlp. Can also be set via GEARSAGE_YTDLP_COOKIES_FILE.",
    )
    parser.add_argument(
        "--cookies-from-browser",
        default="",
        help="Optional browser cookie source for yt-dlp, e.g. chrome or safari. Can also be set via GEARSAGE_YTDLP_COOKIES_FROM_BROWSER.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=90,
        help="yt-dlp timeout per fetch.",
    )
    parser.add_argument(
        "--page-fallback",
        action="store_true",
        help="Run the Playwright page fallback when yt-dlp misses page-visible text.",
    )
    parser.add_argument(
        "--ocr-fallback",
        action="store_true",
        help="Run macOS Vision OCR on Playwright screenshots and append labeled OCR text.",
    )
    args = parser.parse_args()
    result = fetch_video_assets(
        {
            "task_id": args.task_id,
            "platform": args.platform,
            "url": args.url,
            "creator": args.creator,
            "title_hint": args.title_hint,
            "language": args.language,
        },
        cache_root=Path(args.cache_root).resolve(),
        cookies_file=args.cookies_file,
        cookies_from_browser=args.cookies_from_browser,
        timeout_seconds=args.timeout_seconds,
        page_fallback=args.page_fallback,
        ocr_fallback=args.ocr_fallback,
    )
    print_json(result)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
