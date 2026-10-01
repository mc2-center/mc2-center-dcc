# Companion plan: align `mc2-center/ai-curation-pipeline` with the CDE-revised data models

> **The implementation spec is `plans/impl_ai_curation_pipeline_14.md`.** This file keeps the analysis; where they differ, the implementation plan wins.

Companion to `plans/cde_model_alignment_harmonized.md`, referred to below as **[H]**, whose phase numbers are referenced below. The pipeline was reviewed at `main` @ `fa7b977`, read-only. Its unmerged branches were checked for coupling only. **Revised 2026-10-01** against `data-models/main` after #262 merged (`c234c467`). The pipeline's `main` is unchanged since `fa7b977`.

## Verdict: the pipeline is broken now, and updates are required

- **#262 merged on 2026-10-01, so the break is live.** The pipeline pulls its vocabulary at runtime from `data-models/main/mc2.model.jsonld`, which is unpinned, by the class names `Publication Assay`, `Publication Tissue` and `Publication Tumor Type`.
- **Tested against the current `main`:** those names return **0, 0 and 0** terms, with no error. `Assay`, `Tissue` and `Tumor Type` return 391, 114 and 186.
- **Effect of an empty vocabulary:**
  - Claude is constrained to verbatim vocabulary values, so it has nothing valid to return.
  - Alias matching falls back to only the hardcoded supplemental aliases.
  - **Every run since the merge produces blank or near-blank Assay, Tissue and Tumor Type.**
- **Interim workaround, until P0 ships:** run with `--model-jsonld` pointing at a `v13.1.0` copy of `mc2.model.jsonld`, from `git show 13.1.0:mc2.model.jsonld` in data-models.
- **Output headers are the downstream contract.** The pipeline's CSV and XLSX outputs feed DCC's `processing-splits.py` and then the `sync_*.py` scripts. On `main`, `templates/*.csv` are regenerated from the model with 14.0.0 display-name headers. The pipeline's outputs should match those templates exactly ([H] decision 5).
- **X1 (squashed camelCase enums) is fixed upstream.** On `main` the schemas have class-label keys and display-form enums (`10-cell RNA Sequencing`). That unblocks P2 and P3, once 14.0.0 is registered.

## Where the pipeline depends on the model

| Where | What it assumes | Impact of #262 |
|---|---|---|
| `parse_publication_pdfs.py:65-67` `_MC2_MODEL_JSONLD_URL` | `data-models/main/mc2.model.jsonld`, unpinned | The model changes under the pipeline the moment #262 merges |
| `parse_publication_pdfs.py:549-553`, `run_publication_pipeline.py:937-943` | `_extract_jsonld_terms(model, "Publication Assay" / "Publication Tissue" / "Publication Tumor Type")` → `rdfs:subClassOf bts:PublicationAssay` | **Breaks: 0 terms.** The new classes are `bts:Assay`, `bts:Tissue`, `bts:TumorType`. |
| `parse_publication_pdfs.py:90-111` | Terms are read from `sms:displayName`, and `rdfs:subClassOf` is a list or a dict | Still valid. The curator-generated JSON-LD keeps both (checked on `bts:RNASequencing`). |
| `run_publication_pipeline.py:320-340` (PublicationView rows) | Columns `Publication Assay`, `Publication Tumor Type`, `Publication Tissue`, plus the intermediate columns `Publication Grant Number`, `Publication Consortium Name` and `Publication Theme Name` | Must rename to `Assay`, `Tumor Type`, `Tissue`. The intermediate columns aren't template attributes on either branch. DCC's `processing-splits.py:24,41` converts them to `GrantView Key` or drops them. |
| `run_publication_pipeline.py:644-650` `DATASET_FIELDNAMES`, `:806-816` | `Dataset Assay`, `Dataset Species`, `Dataset Tumor Type`, `Dataset Tissue`, `Data Use Codes` | Rename to `Assay`, `Species`, `Tumor Type`, `Tissue`, **`Dataset Data Use Codes`**: on `main`, DatasetView uses its own DUO attribute, validated by the pattern `^(DUO:\d{7}\|DUOPlus\d+\|Pending Annotation)$`. The conditional DUO fields are gone from DatasetView. |
| `tool_detection.py:14-26` `TOOL_FIELDNAMES`; `repo_metadata.py:64,66,111` | `Tool License` | Rename to `License`, which is now a string list using the SPDX list. |
| `repo_metadata.py:66,111` | Falls back to the repo API's license **name** (e.g. "MIT License") for GitLab and Bitbucket, and when GitHub has no SPDX ID | Already produces invalid values, and they'd be enforced once schemas are the source of truth. Normalize to an SPDX ID or leave blank. |
| `run_publication_pipeline.py:652-660` `_load_tool_valid_values` (Sheet2 of `Tool_manifest_*.xlsx`) | Valid values and headers for the ToolView come from an external template file | The template has to be regenerated from the revised model, or the dependency replaced (P3). |
| `metadata_merge.py` `_SUPPLEMENTAL_*_ALIASES`, `TUMOR/TISSUE_NOISE_TERMS`, collapse values `Pan-cancer` and `Not Applicable`, sentinels `Pending Annotation` and `Restricted Access` | Targets must be canonical terms | **No change needed.** Checked: all 31 assay, 15 tissue and 40 tumor alias targets, all noise terms and all sentinels are in the revised vocabulary. |
| `download_publication_pdfs.py:509-513` species mapping | GEO taxon → Species label. Unknown taxa fall back to `p.title()`. | The value set only grew (29 to 32 values), so nothing is dropped. The `title()` fallback can produce invalid values; P2 validation catches them. |
| `run_publication_pipeline.py:96-133` (Step 0) | Grants portal table syn21918972 columns `grantNumber`, `consortium`, `theme` | **No change needed**, under [H] decisions 1 and 2 (portal columns stay; consortium keeps display names). |
| `categorize_publications.py` | Publications portal table syn21868591 columns `pubMedId`, `publicationTitle`, `abstract`, `assay` | **No change needed** (portal columns stay). |
| `tests/test_vocab_jsonld.py` | Fixture uses `bts:PublicationAssay` / `PublicationTissue` | Add fixtures in the revised shape (P0). |
| Unmerged branch `feature/manifest-review-app` | 33+ references to `Publication Assay` (review app, review mining, tests) | Has to be rebased onto the aligned `main` and renamed before it merges (P4). |

## X1: squashed enum labels in the schemas (resolved upstream)

- **The problem was:** schema enum values squashed to camelCase (`RNASequencing`), in the generated `json_schemas/*.json` and in the live registered `MC2Center-PublicationView-13.0.0` and `-15.0.0`. Every data source stores the spaced form.
- **Resolution, now on `data-models/main`:**
  - Keys are class labels, because curator doesn't allow spaces (O7).
  - `create_json_from_model.py` post-processes the enums back to display form, using each attribute's own CSV Valid Values (`scripts/enum_display_labels.py`, DM-7).
  - `scripts/check_json_schemas.py` enforces it.
- **What's left:** registering 14.0.0 in Synapse ([H] S1). The live registered versions still have camelCase enums until then.
- **For the pipeline:**
  - Output headers are the **template display names** (P1, [H] decision 5).
  - Validation converts them to class labels before checking against the schema (P2).
  - The vocabulary can come from the registered 14.0.0 schema (P3).

---

## Plan

### P0 — Incident fix: harden vocabulary loading (do now; #262 is already merged)
Small and backward-compatible. It closes the silent-failure window whichever order things merge in.
- **Field-name fallback.** `_extract_jsonld_terms` callers try the revised field name first, then the legacy one: `("Assay", "Publication Assay")`, `("Tissue", "Publication Tissue")`, `("Tumor Type", "Publication Tumor Type")`. Put this behind one helper in `parse_publication_pdfs.py`, reused by both call sites (`parse_publication_pdfs.py:549-553`, `run_publication_pipeline.py:937-943`).
- **Fail loudly.** If any vocabulary loads fewer than N terms (e.g. under 50 for Assay), exit non-zero with a message naming the source and the field. Never run with an empty vocabulary.
- **Pin the model source.** The default URL points at a data-models **tag**, not `main`. Add a `MC2_MODEL_REF` env var or CLI override next to the existing `--model-jsonld`.
- **Tests.** `tests/test_vocab_jsonld.py` gets revised-shape fixtures (`bts:Assay`), plus a test that an empty vocabulary raises.
- **Alias check.** Add a unit test that every `_SUPPLEMENTAL_*_ALIASES` target and every noise or collapse term is in the loaded vocabulary. Run it against a vendored copy of the pinned JSON-LD, so a later model change fails in CI, not in production.

### P1 — Output contract: headers match `data-models` `templates/*.csv` at `v14.0.0` ([H] decision 5)
Template headers at `v14.0.0` are the contract, with no renames downstream. Keep a vendored copy of the three relevant template header rows, so a test fails if the pipeline's fieldnames drift.
- **PublicationView:**
  - Headers are exactly `templates/PublicationView.csv`: `Component, PublicationView_id, Study Key, GrantView Key, Publication Doi, Publication Journal, Pubmed Id, Pubmed Url, Publication Title, Publication Year, Publication Keywords, Publication Authors, Publication Abstract, Assay, Tumor Type, Tissue, Publication Accessibility, Publication Dataset Alias`.
  - Emit `GrantView Key` (the joined related grants) in place of `Publication Grant Number`.
  - Stop emitting `Publication Consortium Name` and `Publication Theme Name`; the portal sync derives them from the grants table.
  - `Study Key` stays blank.
  - This lets [H] B4 reduce `processing-splits.py` to splitting only.
- **DatasetView (`DATASET_FIELDNAMES`):**
  - Headers are exactly `templates/DatasetView.csv`. `Dataset Assay`, `Dataset Species`, `Dataset Tumor Type` and `Dataset Tissue` become `Assay`, `Species`, `Tumor Type`, `Tissue`. `Data Use Codes` becomes `Dataset Data Use Codes`. `DatasetView_id` stays.
  - Write DUO values only as `DUO:` IDs or `Pending Annotation` (O10). Never infer data-use conditions.
- **ToolView (`TOOL_FIELDNAMES`, `repo_metadata.py`):**
  - Headers are exactly `templates/ToolView.csv`, so `Tool License` becomes `License`.
  - Normalize license values to SPDX IDs. Leave the value blank rather than emit a non-SPDX name; the GitLab and Bitbucket name fallback gives names like "MIT License".
  - Emit multiple licenses as a list-formatted cell.
- **Define the columns once.** Move the fieldname lists into `ai_curation_pipeline/manifest_fields.py`, stamped `model_version="14.0.0"`.
- **ToolView xlsx template.** It must be regenerated from `templates/ToolView.csv` at `v14.0.0`, or replaced by P3.

### P2 — Validate against the source of truth before hand-off
- **New step.** After Steps 3–5, validate `publication_metadata.csv`, `datasets_publication_metadata.csv` and `tools_*.xlsx` against the registered `MC2Center` 14.0.0 schemas, fetched with `GET /schema/type/registered/MC2Center-<Type>-14.0.0`. Use the `jsonschema` library, row by row.
- **Report.** Write a per-row `validation_report.csv` next to the outputs. Rows that fail validation are flagged, not dropped, so they feed the review app's flagged queue on `feature/manifest-review-app`.
- **Key conversion.** Before validating, convert the template headers to schema keys, using the display name ↔ label pairs in `mc2.model.jsonld` at `v14.0.0`. That's the same mapping as DCC's `normalize_headers` ([H] B1); share it through `cckp_metadata` once that package exists.
- **Dependency.** X1 is fixed on `main`; validation needs 14.0.0 registered ([H] S1). Until then it can run against the `v14.0.0` `json_schemas/*.json` files directly.

### P3 — Vocabulary from the registered schema (after [H] S1 registers 14.0.0; X1 is already fixed upstream)
- **Primary source.** Switch the vocabulary to the registered schema's `enum` lists: Assay, Tissue, Tumor Type and Species from PublicationView and DatasetView, and the ToolView enums.
- **Fallback.** Keep the JSON-LD path (`--model-jsonld`) as an offline fallback.
- **Drop the xlsx dependency.** Tool valid values come from the ToolView schema, which removes the silent "no `Tool_manifest*.xlsx` in CWD → skip Step 5" failure. The template xlsx becomes optional, used for output formatting only.

### P4 — Branches and docs
- **Rebase branches.** Rebase `feature/manifest-review-app` onto the aligned `main` and rename its references to `Publication Assay` (review app, review mining, tests). Check `cckp-dataset-linkage` and `refactor/extract-metadata-fill-logic` for the same names before they merge.
- **Update docs.** In `CLAUDE.md` and `README.md`: the vocabulary source, the field names, the pinned model ref, and the removal of the "use JSON-LD because the JSON schemas are camelCase" rationale once X1 lands.

## Sequencing relative to [H]
1. **P0, now:** an incident fix. Until it ships, use the `--model-jsonld` v13.1.0 workaround.
2. **After the `v14.0.0` tag:** bump P0's pin from `main` to `v14.0.0`.
3. **P1:** in the same release window as the DCC [H] section B PR. Pipeline output and `processing-splits.py` change together, tested against the same fixture outputs. Both merge before 2026-11-01.
4. **P2 and P3:** after [H] S1 registers 14.0.0.
5. **P4:** before any of the unmerged branches merge.

## Delegation
- **P0:** one Sonnet SME, immediately, on a branch in the ai-curation-pipeline repo. Opus reviews it. Opening the PR needs your go-ahead.
- **P1:** Sonnet, one commit per module.
- **P2 and P3:** Sonnet.
- **X1:** done upstream (data-models DM-7). Nothing left for this pipeline beyond P2 and P3.
- **Opening PRs:** needs your go-ahead, after an independent pre-PR review.

## Verification
- **Vocabulary loading.** `pytest tests/` passes with both the legacy-shape and revised-shape JSON-LD fixtures. An empty-vocabulary fixture exits non-zero.
- **End-to-end comparison.** `run-publication-pipeline manifest.csv --no-claude --limit 10` against the `v14.0.0` JSON-LD loads 391, 114 and 186 terms. On the same 10 papers it produces the same Assay, Tissue and Tumor Type values as a pre-change run, under the new headers.
- **Validation.** P2 reports zero failures on those 10 rows, apart from known model gaps, which are listed.
- **Template parity.** A test asserts that the PublicationView, DatasetView and ToolView fieldnames equal the vendored `v14.0.0` template headers.
- **Downstream hand-off.** DCC `processing-splits.py` plus `sync_publications.py --dryrun` (from [H]) take the new outputs with no renames. The `--output_csv` diff against the current portal table shows only the intended changes.
- **Tools.** A tool row from a GitLab-hosted repo gets an SPDX `License` or a blank one, never a license name.
