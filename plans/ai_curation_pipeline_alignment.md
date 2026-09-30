# Companion plan: align `mc2-center/ai-curation-pipeline` with the CDE-revised data models

Companion to `plans/cde_model_alignment_harmonized.md`, referred to below as **[H]**, whose phase numbers are referenced below. The pipeline was reviewed at `main` @ `fa7b977`, read-only. Its unmerged branches were checked for coupling only.

## Verdict: updates are required

- **One silent, critical break when #262 merges.** The pipeline pulls its controlled vocabulary at runtime from `data-models/main/mc2.model.jsonld`, which is unpinned. It selects terms by the class names `Publication Assay`, `Publication Tissue` and `Publication Tumor Type`.
- **Tested against both files:**
  - The pre-merge JSON-LD (`origin/main`) returns 381, 114 and 186 terms.
  - The revised JSON-LD returns **0, 0 and 0**, with no error.
  - The terms now sit under `Assay`, `Tissue` and `Tumor Type`: 391, 114 and 186.
- **Effect of an empty vocabulary:**
  - Claude is constrained to "values that appear verbatim in the vocab lists", so it has nothing valid to return.
  - Alias matching falls back to only the hardcoded supplemental aliases.
  - Every run after the merge would quietly produce blank or near-blank Assay, Tissue and Tumor Type.
- **Output headers are the downstream contract.** The pipeline's output CSV and XLSX files are the input to DCC's `annotations/processing-splits.py` and then the `sync_*.py` scripts. They use the old attribute names, so they must change in step with [H] Phase 2.
- **The registered schemas can't validate what the pipeline produces (cross-cutting).** See X1 below. It affects [H] as much as this pipeline.

## Where the pipeline depends on the model

| Where | What it assumes | Impact of #262 |
|---|---|---|
| `parse_publication_pdfs.py:65-67` `_MC2_MODEL_JSONLD_URL` | `data-models/main/mc2.model.jsonld`, unpinned | The model changes under the pipeline the moment #262 merges |
| `parse_publication_pdfs.py:549-553`, `run_publication_pipeline.py:937-943` | `_extract_jsonld_terms(model, "Publication Assay" / "Publication Tissue" / "Publication Tumor Type")` → `rdfs:subClassOf bts:PublicationAssay` | **Breaks: 0 terms.** The new classes are `bts:Assay`, `bts:Tissue`, `bts:TumorType`. |
| `parse_publication_pdfs.py:90-111` | Terms are read from `sms:displayName`, and `rdfs:subClassOf` is a list or a dict | Still valid. The curator-generated JSON-LD keeps both (checked on `bts:RNASequencing`). |
| `run_publication_pipeline.py:320-340` (PublicationView rows) | Columns `Publication Assay`, `Publication Tumor Type`, `Publication Tissue`, plus the intermediate columns `Publication Grant Number`, `Publication Consortium Name` and `Publication Theme Name` | Must rename to `Assay`, `Tumor Type`, `Tissue`. The intermediate columns aren't template attributes on either branch. DCC's `processing-splits.py:24,41` converts them to `GrantView Key` or drops them. |
| `run_publication_pipeline.py:644-650` `DATASET_FIELDNAMES`, `:806-816` | `Dataset Assay`, `Dataset Species`, `Dataset Tumor Type`, `Dataset Tissue`; `Data Use Codes` is already new-style | Rename to `Assay`, `Species`, `Tumor Type`, `Tissue`. The 15 new optional access-condition (DUO) fields plus `License` stay blank. |
| `tool_detection.py:14-26` `TOOL_FIELDNAMES`; `repo_metadata.py:64,66,111` | `Tool License` | Rename to `License`, which is now a string list using the SPDX list. |
| `repo_metadata.py:66,111` | Falls back to the repo API's license **name** (e.g. "MIT License") for GitLab and Bitbucket, and when GitHub has no SPDX ID | Already produces invalid values, and they'd be enforced once schemas are the source of truth. Normalize to an SPDX ID or leave blank. |
| `run_publication_pipeline.py:652-660` `_load_tool_valid_values` (Sheet2 of `Tool_manifest_*.xlsx`) | Valid values and headers for the ToolView come from an external template file | The template has to be regenerated from the revised model, or the dependency replaced (P3). |
| `metadata_merge.py` `_SUPPLEMENTAL_*_ALIASES`, `TUMOR/TISSUE_NOISE_TERMS`, collapse values `Pan-cancer` and `Not Applicable`, sentinels `Pending Annotation` and `Restricted Access` | Targets must be canonical terms | **No change needed.** Checked: all 31 assay, 15 tissue and 40 tumor alias targets, all noise terms and all sentinels are in the revised vocabulary. |
| `download_publication_pdfs.py:509-513` species mapping | GEO taxon → Species label. Unknown taxa fall back to `p.title()`. | The value set only grew (29 to 32 values), so nothing is dropped. The `title()` fallback can produce invalid values; P2 validation catches them. |
| `run_publication_pipeline.py:96-133` (Step 0) | Grants portal table syn21918972 columns `grantNumber`, `consortium`, `theme` | **No change needed**, under [H] decisions 1 and 2 (portal columns stay; consortium keeps display names). |
| `categorize_publications.py` | Publications portal table syn21868591 columns `pubMedId`, `publicationTitle`, `abstract`, `assay` | **No change needed** (portal columns stay). |
| `tests/test_vocab_jsonld.py` | Fixture uses `bts:PublicationAssay` / `PublicationTissue` | Add fixtures in the revised shape (P0). |
| Unmerged branch `feature/manifest-review-app` | 33+ references to `Publication Assay` (review app, review mining, tests) | Has to be rebased onto the aligned `main` and renamed before it merges (P4). |

## X1 — Cross-cutting problem: registered schema enums use squashed camelCase

- **What's wrong.** Enum values in the JSON schemas are squashed to camelCase: `RNASequencing`, `AcinarCellCarcinoma`, `10-cellRNASequencing`. That's true of the generated `json_schemas/*.json` on both branches, and of the **live** registered `MC2Center-PublicationView-13.0.0` and `-15.0.0`.
- **What everything else uses.** Portal tables, curated manifests and this pipeline's output all use the spaced display values: `RNA Sequencing`.
- **Consequence.** Once "schemas are the source of truth", essentially every existing spaced value fails validation. This is also why the pipeline reads the JSON-LD instead of the JSON schemas (its CLAUDE.md notes the camelCase problem).
- **Cause.** `data-models/create_json_from_model.py` calls `synapseclient.extensions.curator.generate_jsonschema` with the default `data_model_labels="class_label"`.
- **Checked fix.** In a scratchpad venv (synapseclient 4.13.0 with curator extras), `data_model_labels="display_label"` produces:
  - Enums in spaced form: `10-cell RNA Sequencing`, `Direct Long-Read RNA Sequencing`.
  - Property keys equal to the template display names: `Assay`, `GrantView Key`, `Publication Doi`, `PublicationView_id`, `Pubmed Id`.
  - `required` lists that match.
- **Recommendation.** Treat this as a canonical fix, not a workaround. Generate and register every schema with `display_label`. Then:
  - The pipeline's output headers, DCC's schematic-style manifests and `sync_publications.py` (which already uses display names) all line up with the schema keys directly.
  - The pipeline can read its vocabulary from the registered schema itself.
- **Proposed changes to [H]:**
  - **Phase 0:** add item 0.11, "generate JSON schemas with `data_model_labels="display_label"`; confirm that curator validation, RecordSets and curation tasks accept spaced property keys."
  - **Phase 2a:** schema keys in the crosswalk become the display names.
  - **Phase 2c:** `sync_datasets.py`, `sync_tools.py` and `sync_education.py` currently read class-label keys (`DatasetAssay`, `ToolLicense`). They switch to display names (`Assay`, `License`) rather than class labels (`TumorType`), and `DatasetView_id` stays as it is instead of becoming `DatasetViewId`.
  - **Phase 1:** registration at 16.0.0 uses the display-label schemas.
  - **Open question for [H] (O7):** whether Synapse RecordSets and Grid handle property keys that contain spaces. If they don't, the fallback is `class_label` keys with display-form enums. That needs a curator option or a post-processing step, which requires explicit approval per the no-silent-workarounds rule.

---

## Plan

### P0 — Harden the pipeline now (can merge **before** #262)
Small and backward-compatible. It closes the silent-failure window whichever order things merge in.
- **Field-name fallback.** `_extract_jsonld_terms` callers try the revised field name first, then the legacy one: `("Assay", "Publication Assay")`, `("Tissue", "Publication Tissue")`, `("Tumor Type", "Publication Tumor Type")`. Put this behind one helper in `parse_publication_pdfs.py`, reused by both call sites (`parse_publication_pdfs.py:549-553`, `run_publication_pipeline.py:937-943`).
- **Fail loudly.** If any vocabulary loads fewer than N terms (e.g. under 50 for Assay), exit non-zero with a message naming the source and the field. Never run with an empty vocabulary.
- **Pin the model source.** The default URL points at a data-models **tag**, not `main`. Add a `MC2_MODEL_REF` env var or CLI override next to the existing `--model-jsonld`.
- **Tests.** `tests/test_vocab_jsonld.py` gets revised-shape fixtures (`bts:Assay`), plus a test that an empty vocabulary raises.
- **Alias check.** Add a unit test that every `_SUPPLEMENTAL_*_ALIASES` target and every noise or collapse term is in the loaded vocabulary. Run it against a vendored copy of the pinned JSON-LD, so a later model change fails in CI, not in production.

### P1 — Output contract (merge **together with** [H] Phase 2, after #262)
The pipeline emits headers that equal the registered schema's property keys, which are display labels under X1.
- **PublicationView rows:**
  - `Publication Assay/Tumor Type/Tissue` become `Assay`, `Tumor Type`, `Tissue`.
  - Emit `GrantView Key` (the joined related grants) directly in place of `Publication Grant Number`.
  - Stop emitting `Publication Consortium Name` and `Publication Theme Name`. The portal sync derives these from the grants table (`sync_publications.add_missing_info`).
  - Add `Study Key` as a blank column for template parity.
  - This lets [H] 2d reduce `processing-splits.py` to splitting only: no renames, no drops.
- **DatasetView (`DATASET_FIELDNAMES`):**
  - `Dataset Assay`, `Dataset Species`, `Dataset Tumor Type` and `Dataset Tissue` become `Assay`, `Species`, `Tumor Type`, `Tissue`.
  - Keep `DatasetView_id` (display label).
  - Leave the new DUO and `License` columns out, or blank. Never infer data-use conditions.
- **ToolView (`TOOL_FIELDNAMES`, `repo_metadata.py`):**
  - `Tool License` becomes `License`.
  - Normalize license values to SPDX IDs (a small name→SPDX map for the GitLab/Bitbucket fallback). Leave the value blank rather than emit a non-SPDX name.
  - Emit multiple licenses as a list-formatted cell.
- **Define the columns once.** Move `DATASET_FIELDNAMES`, `TOOL_FIELDNAMES` and the PublicationView keys into one module (e.g. `ai_curation_pipeline/manifest_fields.py`). Stamp the model version they target, matching [H] 2a's crosswalk version.

### P2 — Validate against the source of truth before hand-off
- **New step.** After Steps 3–5, validate `publication_metadata.csv`, `datasets_publication_metadata.csv` and `tools_*.xlsx` against the registered `MC2Center` 16.0.0 schemas, fetched with `GET /schema/type/registered/MC2Center-<Type>-16.0.0`. Use the `jsonschema` library, row by row.
- **Report.** Write a per-row `validation_report.csv` next to the outputs. Rows that fail validation are flagged, not dropped, so they feed the review app's flagged queue on `feature/manifest-review-app`.
- **Dependency.** This depends on X1, because validating spaced values against camelCase enums would flag every row.

### P3 — Vocabulary from the registered schema (after X1 is fixed and 16.0.0 is registered)
- **Primary source.** Switch the vocabulary to the registered schema's `enum` lists: Assay, Tissue, Tumor Type and Species from PublicationView and DatasetView, and the ToolView enums.
- **Fallback.** Keep the JSON-LD path (`--model-jsonld`) as an offline fallback.
- **Drop the xlsx dependency.** Tool valid values come from the ToolView schema, which removes the silent "no `Tool_manifest*.xlsx` in CWD → skip Step 5" failure. The template xlsx becomes optional, used for output formatting only.

### P4 — Branches and docs
- **Rebase branches.** Rebase `feature/manifest-review-app` onto the aligned `main` and rename its references to `Publication Assay` (review app, review mining, tests). Check `cckp-dataset-linkage` and `refactor/extract-metadata-fill-logic` for the same names before they merge.
- **Update docs.** In `CLAUDE.md` and `README.md`: the vocabulary source, the field names, the pinned model ref, and the removal of the "use JSON-LD because the JSON schemas are camelCase" rationale once X1 lands.

## Sequencing relative to [H]
1. **P0:** now, independent of everything.
2. **X1:** proposed as [H] 0.11; decide O7 alongside it.
3. **#262 merges** and the release is tagged. Bump the P0 pin to the tag.
4. **P1:** in the same release window as the DCC [H] Phase 2 PR. Pipeline output and `processing-splits.py` must change together. Run P1 and DCC 2d against the same fixture outputs.
5. **P2 and P3:** after [H] Phase 1 registers 16.0.0.
6. **P4:** before any of those branches merge.

## Delegation
- **P0 and P1:** Sonnet SMEs in the ai-curation-pipeline repo, on a branch, one commit per module. P0 goes first and alone.
- **P2 and P3:** Sonnet.
- **X1:** a design change in data-models. Opus owns the decision and the O7 check. Implementation is a one-line generator change plus regenerating the schemas (Sonnet).
- **Opening PRs:** needs your go-ahead, after an independent pre-PR review.

## Verification
- **Vocabulary loading.** `pytest tests/` passes with both the legacy-shape and revised-shape JSON-LD fixtures. An empty-vocabulary fixture exits non-zero.
- **End-to-end comparison.** `run-publication-pipeline manifest.csv --no-claude --limit 10` against the pinned revised JSON-LD loads 391, 114 and 186 terms. On the same 10 papers it produces the same Assay, Tissue and Tumor Type values as a pre-change run, under the new headers.
- **Validation.** P2 reports zero failures on those 10 rows, apart from known model gaps, which are listed.
- **Downstream hand-off.** DCC `processing-splits.py` plus `sync_publications.py --dryrun` (from [H]) take the new outputs with no renames. The `--output_csv` diff against the current portal table shows only the intended changes.
- **Tools.** A tool row from a GitLab-hosted repo gets an SPDX `License` or a blank one, never a license name.
