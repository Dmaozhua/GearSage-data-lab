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
        help="yt-dlp timeout per video row.",
    )
    parser.add_argument(
        "--page-fallback",
        action="store_true",
        help="When yt-dlp lacks正文层内容, run the Playwright page fallback for visible text and screenshots.",
    )
    parser.add_argument(
        "--ocr-fallback",
        action="store_true",
        help="Run macOS Vision OCR on Playwright screenshots and append labeled OCR text to page_text.",
    )
    args = parser.parse_args()
    result = run_ingest(
        Path(args.workbook).resolve(),
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
