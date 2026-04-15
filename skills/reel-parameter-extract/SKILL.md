---
name: reel-parameter-extract
description: Extract reel models and player-reported reel fields from raw_ingest records, normalize field values, preserve source quotes, and write structured results into player_data_extract.
---

# Purpose
Use this skill to convert raw fishing video source data into structured reel player-data rows.

# When to use
Use this skill when:
- raw_ingest already exists
- the goal is to extract reel-related player fields from text, transcript, subtitle, or visible descriptions
- every extracted field should carry source traceability and confidence
- missing fields should be omitted rather than guessed

# When not to use
Do not use this skill when:
- raw_ingest has not been created yet
- the task is only video ingestion
- the task is direct production merge without human review

# Required input
A workbook with:
- raw_ingest
- optional existing player_data_extract for append/update logic

# Field whitelist
Only extract fields from shared/reel_data_schema/field_whitelist.yaml.

# Output
Write player_data_extract with one row per:
- reel model
- field candidate
- task_id

Primary entry point:
- `skills/reel-parameter-extract/scripts/extract_player_fields.py`

# Required behavior
1. Read raw_ingest rows whose ingest_status is success or partial.
2. Detect reel entities from title, transcript, subtitles, description_text, and page_text.
3. Normalize reel model names using model_normalization.yaml where possible.
4. Extract only whitelisted fields.
5. Preserve both raw value and normalized value.
6. Attach source_quote for every non-empty extracted field whenever possible.
7. Tag source_quote_type as spoken, subtitle, description, page_text, ocr, or manual_note.
8. Tag source_authority using shared source authority rules.
9. Score confidence as high, medium, or low.
10. Set review_required=yes for:
   - numeric fields
   - material fields
   - confidence not equal to high
   - conflict-prone rows
11. Never fabricate a value not supported by the source.
12. If a video mentions multiple reels, write separate rows for each reel and field.
13. Do not generate rows for missing values just to make the table look complete.

# Confidence policy
High:
- value explicitly stated in speech, subtitle, or visible text

Medium:
- value strongly supported by context or repeated indirectly

Low:
- ambiguous, inferred, or weakly supported

# Tool preference
- Primary: excel, filesystem
- Secondary: ocr when the key value is on-screen only
- Optional: postgres for model normalization lookup

# Sheet mapping
Input:
- raw_ingest

Output:
- player_data_extract

# Validation rules
- one row must represent exactly one field for one reel
- field_name must be from whitelist
- non-empty rows should have confidence
- source_quote should be present whenever the source supports it
- blank fields should not generate placeholder values

# Notes
This skill is the structured extraction layer. It should optimize for traceability and reviewability, not for forced completeness.
