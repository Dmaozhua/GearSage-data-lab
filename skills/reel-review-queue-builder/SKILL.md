---
name: reel-review-queue-builder
description: Build a human review queue from player_data_extract by selecting rows that require manual confirmation and writing them into review_queue.
---

# Purpose
Use this skill to prepare a clean, operator-friendly review sheet for manual approval, rejection, or editing of extracted reel player-data.

# When to use
Use this skill when:
- player_data_extract already exists
- the user wants a review-ready queue for manual confirmation
- extracted values should not be merged directly into official datasets

# When not to use
Do not use this skill when:
- extraction has not been completed
- the task is only to inspect raw_ingest
- the user wants a direct summary instead of a review workflow

# Required input
A workbook with:
- player_data_extract

# Output
Write review_queue with one row per reviewable extracted field.

# Required behavior
1. Read player_data_extract.
2. Include rows in review_queue when:
   - review_required=yes
   - confidence is medium or low
   - field is numeric or material related
   - field is manually marked as important
3. Copy source traceability fields into review_queue.
4. Pre-fill candidate_value from field_value_normalized when available, else use field_value_raw.
5. Leave review_action and review_value blank for human editing.
6. Do not remove existing human review notes unless explicitly rebuilding from scratch.
7. Keep extract_id and task_id stable for traceability.

# Review action policy
Allowed review_action values:
- approve
- reject
- edit
- hold

# Tool preference
- Primary: excel, filesystem

# Sheet mapping
Input:
- player_data_extract

Output:
- review_queue

# Validation rules
- each review row must map back to exactly one extract_id
- candidate_value should prefer normalized value
- source_url and field_name must be preserved
- human-editable fields must remain editable

# Notes
This skill builds the manual review layer. It does not decide final production merge.
