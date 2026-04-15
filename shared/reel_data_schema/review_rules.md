# Review Rules

Rows should enter `review_queue` when any of the following is true:
- `review_required = yes`
- `confidence` is `medium` or `low`
- `field_name` belongs to the numeric field set
- `field_name` belongs to the material field set

Human-editable columns that the pipeline must leave alone on rebuild when `extract_id` is unchanged:
- `review_action`
- `review_value`
- `review_comment`
- `reviewer`
- `reviewed_at`

Allowed `review_action` values:
- `approve`
- `reject`
- `edit`
- `hold`
