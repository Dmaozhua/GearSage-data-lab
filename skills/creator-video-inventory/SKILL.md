---
name: creator-video-inventory
description: Enumerate one Douyin creator homepage, collect a deduplicated video inventory, write creator_video_inventory, and optionally append unseen video links into input_videos.
---

# Purpose
Use this skill when the goal is to inventory all visible videos for one Douyin creator before running ingest and extraction.

# When to use
Use this skill when:
- the user provides a Douyin creator homepage or a creator-scoped Douyin page
- the workbook already contains or should contain a sheet named `creator_video_inventory`
- the task is to build a reusable video queue for later ingest, not to extract reel parameters yet

# When not to use
Do not use this skill when:
- the task is only to process a small, fixed list of known video links
- the user wants direct field extraction from already-ingested raw material
- the task is to write into production systems

# Required input
- One Douyin creator homepage URL, or a creator-scoped Douyin page that can be normalized into a creator homepage
- Workbook: `data/input/reel_player_data_pipeline_v1.xlsx`
- Optional but recommended for better coverage: a local Netscape-format Douyin cookies file passed at runtime, not stored in repo code

# Output
- Write deduplicated rows into `creator_video_inventory`
- Optionally append unseen `video_url` values into `input_videos`

Primary entry point:
- `skills/creator-video-inventory/scripts/enumerate_creator_inventory.py`

# Required behavior
1. Open the creator page with Playwright.
2. Normalize to a canonical creator homepage whenever possible.
3. Scroll to trigger lazy loading.
4. Capture as many visible or page-discoverable video links as possible.
5. Deduplicate by `video_id` or `video_url`.
6. Write rows into `creator_video_inventory`.
7. Append new rows into `input_videos` only when the `video_url` is not already present.
8. Keep incomplete metadata blank instead of guessed.
9. Record blocking or instability reasons in `inventory_notes`.

# Tool preference
- Primary: Playwright, Excel, filesystem
- Secondary: OCR only if cover text is visible in screenshots
- Do not use yt-dlp as the primary homepage enumeration path
- Prefer Playwright network capture of creator works requests over brittle DOM-only card scraping

# Sheet mapping
Input:
- `input_videos`
- `creator_video_inventory`

Output:
- `creator_video_inventory`
- `input_videos` for newly discovered videos

# Validation rules
- `video_id` should be parsed from `/video/<id>`, `modal_id`, or `aweme_id` when available
- `video_url` should be canonicalized to `https://www.douyin.com/video/<id>` when `video_id` is known
- `title_detected`, `publish_date`, and `cover_text` may remain blank
- Never fabricate video titles or dates

# Failure handling
- If the creator page opens but no video list is discoverable, still record the creator-level failure reason in the command output
- If the page is blocked by verification or service errors, preserve that note for the user

# Notes
This skill inventories creator-level video links only. It does not run ingest, extraction, or review on its own.
