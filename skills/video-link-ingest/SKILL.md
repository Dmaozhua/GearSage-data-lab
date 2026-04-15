---
name: video-link-ingest
description: Read the input_videos sheet from a workbook, ingest fishing-related video links from Douyin, Bilibili, or YouTube, and write normalized raw source data into the raw_ingest sheet.
---

# Purpose
Use this skill to convert a batch of video links into normalized raw source records that can be used for downstream reel parameter extraction.

# When to use
Use this skill when:
- the user provides or maintains a workbook with a sheet named input_videos
- rows include one or more video URLs from Douyin, Bilibili, or YouTube
- the goal is to fetch title, creator, publish date, description, subtitles, transcript, page text, and local asset paths
- missing values are allowed and should remain blank rather than guessed

# When not to use
Do not use this skill when:
- the user only wants a one-off manual summary of a single video
- raw_ingest already exists and the task is only structured field extraction
- the task is to write directly into production databases

# Required input
A workbook with a sheet named input_videos and at least these columns:
- task_id
- platform
- url
- status

# Output
Write or update the raw_ingest sheet with one row per task_id.

# Required behavior
1. Read input_videos.
2. Process rows whose status is pending or failed and explicitly retried.
3. Use yt_dlp first to fetch metadata, subtitles, transcript, audio, or video if available.
4. If metadata is incomplete, use Playwright to open the page and capture title, creator, publish date, and visible description.
5. If useful, use Scrapling to capture page text.
6. Save intermediate assets under data/cache/<task_id>/.
7. Write raw_ingest with blank cells for unavailable fields.
8. Never invent titles, dates, or transcript text.
9. Record ingest method and failure reason in ingest_notes.
10. Update input_videos.status to ingested, partial, or failed where appropriate.

# Tool preference
- Primary: excel, filesystem, yt_dlp
- Secondary: playwright, scrapling
- Fallback only: ocr

# Sheet mapping
Input:
- input_videos

Output:
- raw_ingest

# Validation rules
- task_id must remain stable
- url must be preserved exactly
- publish_date should be normalized to YYYY-MM-DD when known
- transcript_text and subtitle_text may both exist and should not overwrite each other
- if no asset is available, leave the corresponding path blank

# Failure handling
- failed means nothing useful was captured
- partial means some source text or metadata was captured but not all
- success means the row contains enough raw source material for extraction

# Notes
This skill builds the evidence layer. It does not extract reel parameters.
