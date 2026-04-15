# Confidence Rules

`high`
- The candidate value is explicitly stated in a subtitle line or spoken transcript line.
- The extracted value can be copied directly from the source quote without guessing.

`medium`
- The candidate value is explicitly stated in title, description, or page text.
- The quote is usable, but the source is not as strong as subtitle or speech.

`low`
- The quote is incomplete, ambiguous, or only weakly related to the field.
- Minimal pipeline should avoid generating rows at this level unless a human specifically asks for looser extraction.

Default review trigger:
- All numeric fields require review.
- All material fields require review.
- Any row with confidence other than `high` requires review.
