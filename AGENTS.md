# GearSage Data Lab - AGENTS.md

## Purpose
This workspace is for video-based reel player-data ingestion, extraction, and review workflows.
It is separate from the main GearSage product repositories.

## Core rules
- Never fabricate reel parameters when the source does not mention them.
- Preserve source traceability for every extracted non-empty field.
- Distinguish player-reported data from official data at all times.
- Numeric and material fields should default to human review.
- Missing values must remain blank rather than guessed.

## Workflow order
1. Run ingest first.
2. Run structured extraction second.
3. Build human review queue third.
4. Do not merge into production schema automatically.

## Tool preference
- Prefer yt_dlp for metadata, subtitles, transcript, audio, and video download.
- Use Playwright only when the page needs browser interaction.
- Use OCR only as fallback when the value appears on-screen but not in transcript/subtitle.
- Use Excel to read and write the workbook as the main operator-facing artifact.
