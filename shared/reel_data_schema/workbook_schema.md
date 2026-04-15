# Workbook Schema

Workbook: `data/input/reel_player_data_pipeline_v1.xlsx`

## input_videos
Purpose: operator input queue for video links.

Headers:
- `task_id`
- `platform`
- `url`
- `creator`
- `title_hint`
- `content_type`
- `priority`
- `status`
- `language`
- `target_field_scope`
- `manual_notes`
- `created_at`
- `updated_at`

## raw_ingest
Purpose: one row per video task after ingest.

Headers:
- `task_id`
- `platform`
- `url`
- `creator_input`
- `creator_detected`
- `title_detected`
- `publish_date`
- `description_text`
- `subtitle_text`
- `transcript_text`
- `page_text`
- `comments_text`
- `audio_file_path`
- `video_file_path`
- `subtitle_file_path`
- `screenshot_dir`
- `asset_dir`
- `ingest_method`
- `ingest_status`
- `ingest_notes`
- `ingest_started_at`
- `ingest_finished_at`

## player_data_extract
Purpose: long-table extraction output. One row represents one reel field candidate only.

Headers:
- `extract_id`
- `task_id`
- `platform`
- `url`
- `creator`
- `publish_date`
- `reel_model_raw`
- `reel_model_normalized`
- `reel_brand_normalized`
- `reel_type_guess`
- `field_name`
- `field_value_raw`
- `field_value_normalized`
- `unit`
- `value_type`
- `source_quote`
- `source_quote_type`
- `source_authority`
- `confidence`
- `review_required`
- `review_reason`
- `extraction_method`
- `extract_notes`
- `created_at`

Rules:
- Do not use a wide-table layout here.
- Do not generate placeholder rows for missing values.
- Non-empty rows should carry `source_quote` whenever the source makes that possible.

## review_queue
Purpose: human review and approval queue only. Not a production merge table.

Headers:
- `review_id`
- `extract_id`
- `task_id`
- `reel_model_normalized`
- `reel_model_raw`
- `field_name`
- `candidate_value`
- `candidate_value_raw`
- `unit`
- `confidence`
- `source_quote`
- `source_quote_type`
- `source_url`
- `source_author`
- `source_authority`
- `review_action`
- `review_value`
- `review_comment`
- `reviewer`
- `reviewed_at`
