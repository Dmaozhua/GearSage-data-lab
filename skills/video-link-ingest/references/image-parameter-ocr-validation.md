# Image Parameter OCR Validation Notes

## Scope
This note records the current conclusion for the Douyin image-parameter OCR path.

Test pool:
- Creator: `路亚老王`
- Bucket: `300以上水滴轮`
- Inventory size: `59` videos

Rules during validation:
- Do not change workbook schema.
- Do not change extract rules.
- Do not run the full extract/review flow for this experiment.
- Use page fallback and screenshots only.
- Keep OCR output separate from `page_text` when validating image parameters.

## Validation 1: `spool_weight_g`
Sample size:
- `10` videos

Method:
- Enumerate the bucket inventory.
- Select `10` videos by publish date.
- Run page fallback.
- Capture screenshots, with emphasis on the middle parameter image.
- Run OCR on screenshots without merging OCR text back into `page_text`.
- Look only for `number + g`.

Observed result:
- `4/10` videos contained OCR-readable `g` values in image-like frames.
- `0/10` videos produced a credible `spool_weight_g` hit.

Why it failed:
- OCR was able to see numbers such as `1g`, `3g`, `5g`, `68g`, `339g`.
- These values mostly came from:
  - casting test weight ranges
  - ranking/performance comparison charts
  - unrelated recommended content before screenshot tightening
- They did not behave like spool weight labels.

Conclusion:
- Image OCR is not currently reliable enough to use `spool_weight_g` as a primary validated field.

## Validation 2: anchored image extraction
Target fields:
- `gear_ratio`
- `bearing_count_roller`
- `spool_diameter_mm`

Sample size:
- `8` videos

Method:
- Use multi-frame screenshots instead of a single middle screenshot.
- Hide login overlays before capture where possible.
- Prefer capturing the current video frame for the third screenshot.
- Run OCR on screenshots.
- Require anchor-to-value pairing:
  - `速比/齿比` -> `x.x:1`
  - `轴承` -> `7+1`
  - `线杯直径/杯径/直径` -> `33mm`

Observed result:
- `gear_ratio`: `0`
- `bearing_count_roller`: `0`
- `spool_diameter_mm`: `0`
- Videos with any credible hit: `0/8`

Why it failed:
- The sampled frames were usually:
  - performance charts
  - ranking charts
  - comparison graphics
  - spoken commentary frames
- They were not stable specification cards.
- OCR could read numbers, but field attribution was too weak.

## Current decision
Stop this path for now.

Interpretation:
- The problem is not pure OCR visibility.
- The problem is weak field attribution from image content.
- For this bucket, image frames are usually not specification cards.

Operational conclusion:
- Do not promote image-parameter OCR to a second primary data source yet.
- Keep the code available for future validation, but treat it as exploratory only.

## If this path is resumed later
Resume only when at least one of these conditions is true:
- the sampled videos clearly show specification cards or parameter tables
- the screenshots can be cropped to the parameter card region only
- anchor-to-value pairing can be made stronger than generic numeric OCR

Suggested restart priority:
1. Prefer videos that visibly contain spec cards or comparison tables.
2. Re-test `gear_ratio` first, because `x.x:1` is the cleanest image-side pattern.
3. Only then revisit `bearing_count_roller` and `spool_diameter_mm`.
