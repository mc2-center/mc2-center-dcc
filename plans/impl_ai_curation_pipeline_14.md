# Implementation plan: ai-curation-pipeline alignment with data model 14.0.0

**Repo:** `mc2-center/ai-curation-pipeline`, from `main` @ `fa7b977`. Work on branch `model-14-alignment`.
**Analysis and background:** `plans/ai_curation_pipeline_alignment.md`. Master plan: `plans/cde_model_alignment_harmonized.md`. The DCC counterpart for the hand-off is task **D8** in `plans/impl_dcc_14_alignment.md`. The Synapse side is in `plans/impl_synapse_14_infrastructure.md`.

## Ground rules
- **Model pin.** Every data-models reference goes through one `MODEL_REF`. Use `c234c467` until the tag exists, then bump to `v14.0.0` in one commit. Never point at `main`.
- **Output contract (master decision 5).** Manifest outputs use **exactly** the headers of data-models `templates/<Component>.csv` at `MODEL_REF`, in the same order. Pipeline-internal fields go to a **separate sidecar file**, not into the manifest.
- **Commits.** One commit per task. `pytest tests/` must be green before each one. The message ends with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- **Not for subagents:** no pushes, no PRs. Opus reviews each diff, an independent review runs before `gh pr create`, and opening the PR needs the owner's go-ahead.
- **Tiers:** Sonnet throughout. Opus reviews.

## What the pipeline produces today (for reference)
- **`publication_metadata.csv`:** Step-0 columns (`Component`, `Publication Grant Number`, `Publication Consortium Name`, `Publication Theme Name`, … `Publication Assay/Tumor Type/Tissue`, `Publication Dataset Alias`, `Publication Accessibility`), plus `keywords` and `_download_status`, `_download_source`, `_pdf_path`, `_parse_error`, `_tool_availability_text`. One file covers all grants.
- **`datasets_publication_metadata.csv`:** `DATASET_FIELDNAMES` (13.1.0 DatasetView shape).
- **`tools_<stem>.xlsx`:** Sheet1 has `TOOL_FIELDNAMES` (13.1.0 ToolView shape); Sheet2 has the template's valid values.
- **Downstream,** DCC runs `split_manifest_grants.py` → `gen-mp-csv.py` → `processing-splits.py` (template reshaping, 500-character cap, drops non-template columns) → `create_entity_links.py` → upload into the grant projects.

## Dependency graph
```
P0 (incident fix, now) ─ P1 (output contract; ships with DCC D8) ─ P2 (validation; after infra I2)
                                                                └─ P3 (schema-sourced vocab; after infra I2)
P4 (branches/docs): before any unmerged branch merges
```

---

### P0. Incident fix: vocabulary loading (now; independent of the tag)
- **Files:** `ai_curation_pipeline/parse_publication_pdfs.py`, `ai_curation_pipeline/run_publication_pipeline.py`, `tests/test_vocab_jsonld.py`, and a new vendored fixture `tests/fixtures/mc2.model.14.subset.jsonld`.
- **Spec:**
  - **Pin the URL.** `_MC2_MODEL_JSONLD_URL` points at `MODEL_REF`, not `main`. An env var `MC2_MODEL_REF` overrides the ref; `--model-jsonld` still takes precedence.
  - **One shared loader.** Add `load_field_vocab(model, field) -> list[str]`. It tries the 14.0.0 field name first, then the 13.1.0 one: `("Assay", "Publication Assay")`, `("Tissue", "Publication Tissue")`, `("Tumor Type", "Publication Tumor Type")`. Use it at both call sites (`parse_publication_pdfs.py:549-553` and `run_publication_pipeline.py:937-943`).
  - **Fail loudly on a thin vocabulary.** If a vocabulary has fewer than its minimum (Assay 300, Tissue 80, Tumor Type 150), exit non-zero. The message names the source URL or path and the field. Never continue with an empty vocabulary.
  - **Startup log.** At startup, log the source, the ref and the term counts.
- **Tests:**
  - 13.1.0-shape and 14.0.0-shape fixtures both load.
  - An empty-vocabulary fixture raises.
  - The supplemental alias targets and the noise and collapse terms (`Pan-cancer`, `Not Applicable`) are all in the 14.0.0 fixture vocabulary. All targets are already valid as of `c234c467`.
- **Check:** `run-publication-pipeline manifest.csv --no-claude --limit 10` with the default source loads 391, 114 and 186 terms.
- **Commit:** `Pin data-models vocab source and fail on empty vocab`.
- **Ships on its own as a PR,** with the owner's go-ahead.

### P1. Output contract: template-exact manifests and a sidecar
- **Pairs with** DCC **D8**; run both against the same fixtures. Ship it in the same release window as the DCC PR, before 2026-11-01.
- **Files:**
  - a new `ai_curation_pipeline/manifest_fields.py`
  - `run_publication_pipeline.py` (`_write_output`, the Step-0 table builder at `:320-340`, `DATASET_FIELDNAMES`, `generate_tools_manifest`)
  - `tool_detection.py` (`TOOL_FIELDNAMES`, `empty_tool_row`)
  - `repo_metadata.py`
  - tests
- **`manifest_fields.py`:**
  - `MODEL_REF`.
  - `PUBLICATION_FIELDS`, `DATASET_FIELDS` and `TOOL_FIELDS`: vendored copies of the `v14.0.0` template header rows.
  - `fetch_template_headers(component)`, used only by a test that compares the vendored copies with the pinned remote.
- **PublicationView output:**
  - **Header order** is exactly `PUBLICATION_FIELDS`: `Component, PublicationView_id, Study Key, GrantView Key, Publication Doi, Publication Journal, Pubmed Id, Pubmed Url, Publication Title, Publication Year, Publication Keywords, Publication Authors, Publication Abstract, Assay, Tumor Type, Tissue, Publication Accessibility, Publication Dataset Alias`.
  - **`GrantView Key`** holds the comma-joined related grants. It replaces `Publication Grant Number`.
  - **`PublicationView_id`** equals `Pubmed Id`.
  - **`Study Key`** is blank.
  - **Gone:** `Publication Consortium Name` and `Publication Theme Name`. The portal sync derives these from the grants table.
  - **Keywords** go into `Publication Keywords`; drop the extra `keywords` column.
- **Sidecar file:** `publication_metadata.pipeline.csv`, keyed by `Pubmed Id`. It holds `_download_status`, `_download_source`, `_pdf_path`, `_parse_error`, `_tool_availability_text`, plus the consortium and theme columns for review use. Step 5 (tools) reads `_tool_availability_text` from here.
- **DatasetView output:**
  - Headers are exactly `DATASET_FIELDS`. `Dataset Assay`, `Dataset Species`, `Dataset Tumor Type` and `Dataset Tissue` become `Assay`, `Species`, `Tumor Type`, `Tissue`. `Data Use Codes` becomes **`Dataset Data Use Codes`**.
  - The DUO value is `Pending Annotation` unless one is explicitly known; then it must be a `DUO:` ID (master decision O10). Never infer it.
- **ToolView output:**
  - Headers are exactly `TOOL_FIELDS`, so `Tool License` becomes `License`.
  - **SPDX normalization** in `repo_metadata.py`:
    - Use GitHub's `spdx_id`, unless it's `NOASSERTION`.
    - For the GitLab and Bitbucket name fallback, map names to SPDX IDs with a small table (`"MIT License"` → `MIT`, `"Apache License 2.0"` → `Apache-2.0`, …).
    - If the result isn't in the 14.0.0 `License` set, leave it **blank**.
  - **Multiple licenses** are written as one comma-joined cell.
  - **Workbook:** Sheet1 is named `manifest`, so DCC D8 can read the `.xlsx` directly. Sheet2 holds the valid values, taken from `all_valid_values.csv` at `MODEL_REF` (the `tool_*` categories plus `license`). That replaces copying them from the external `Tool_manifest_*.xlsx`; the template file becomes optional.
- **Tests:**
  - Each output's header equals the vendored template header.
  - The vendored headers equal the pinned remote (network test, marked `@pytest.mark.network`).
  - A sidecar join round-trips.
  - GitLab "MIT License" becomes `MIT`, and an unknown license becomes blank.
  - `Restricted Access` rows still get `Pending Annotation` for Assay, Tumor Type and Tissue.
- **Check:**
  - A 10-paper `--no-claude` run produces the same Assay, Tissue and Tumor Type values as a P0-only run, under the new headers.
  - DCC D8's `processing-splits.py` takes the outputs with **no renames and no drops** logged.

### P2. Validate against the 14.0.0 schemas before hand-off
- **Depends on** P1. Use the `v14.0.0` `json_schemas/*.json` files at `MODEL_REF` until infrastructure step I2 registers 14.0.0; after that, switch the default to `GET /schema/type/registered/MC2Center-<Type>-14.0.0`.
- **Spec:**
  1. Convert template headers to schema keys through `mc2.model.jsonld` (`sms:displayName` → `rdfs:label`). That's the same mapping as DCC `model_contract`; share the code when `cckp_metadata` exists.
  2. Split on `, ` **only** for keys whose schema type is `array`. `GrantView Key` is a comma-joined `string` (data-models DM-15) and must stay unsplit.
  3. Validate each row with `jsonschema`.
  4. Write `validation_report.csv` (row key, field, message).
  5. **Flag failing rows, don't drop them.**
- **Check:** the 10-paper run reports only known model gaps, and each one is listed.

### P3. Vocabulary from the registered schema
- **Depends on** P2 and infrastructure step I2.
- **Spec:**
  - The default vocabulary source becomes the registered 14.0.0 schema enums: Assay, Tissue and Tumor Type from PublicationView; Species from DatasetView; the ToolView enums for tool fields.
  - The JSON-LD path stays as the `--model-jsonld` fallback.
  - The Sheet2 valid values come from the ToolView schema.
- **Check:** term counts equal the JSON-LD counts at `MODEL_REF`.

### P4. Branches and docs
- **Rebase branches:**
  - `feature/manifest-review-app` (33+ references to `Publication Assay` across the review app, review mining and tests) is rebased onto the aligned `main`, with the P1 headers applied.
  - `cckp-dataset-linkage` and `refactor/extract-metadata-fill-logic` get the same grep before they merge.
- **Update docs.** `CLAUDE.md` and `README.md` get the pinned model ref, the template-exact outputs and sidecar, and the validation report. Remove the "JSON schemas are camelCase, so use JSON-LD" rationale once P3 lands.

## Done when
- P0 is merged; it can go first.
- P1 is merged together with DCC D8, before 2026-11-01.
- P2 and P3 are merged after infrastructure step I2.
- An implementation report is saved at `plans/impl_ai_curation_pipeline_14_report.md` in this repo, and mirrored into the pipeline repo's docs if the owner wants.
