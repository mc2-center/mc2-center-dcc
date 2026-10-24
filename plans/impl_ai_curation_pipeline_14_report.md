# Implementation report: ai-curation-pipeline alignment with data model 14.0.0

Executed against `plans/impl_ai_curation_pipeline_14.md`, 2026-10-23. Sonnet subagents implemented; Opus reviewed each diff and re-ran the tests; independent reviews ran on P0 and on P1–P3. Nothing is pushed.

## Branches (repo `mc2-center/ai-curation-pipeline`, from `main` @ `fa7b977`)
| Branch | Base | Tasks | Commits |
|---|---|---|---|
| `model-14-alignment` | `main` | P0 | `935d558` vocab pin + fail on empty vocab; `f6647d2` review hardening |
| `model-14-output-contract` | `model-14-alignment` | P1 | `b6e500f`, `0c42bbc`, `96945dd`, `4d2212e` |
| `model-14-validation` | `model-14-output-contract` | P2, P3 | `215d76f`, `56e1462`, `51375f2`, `4f8e709` |

Ship order: P0 PR first (independent of everything); P1 together with the DCC PR (D8); P2/P3 after.

## P0. Vocabulary loading
- One pin, `MODEL_REF = "v14.0.0"`; `MC2_MODEL_REF` overrides; `--model-jsonld` overrides both.
- One loader tries the 14.0.0 name, then the 13.1.0 `Publication X` name only when the 14.0.0 name gives zero terms (a thin 14.0.0 list fails closed).
- Below 300 / 80 / 150 terms the run exits 2 with the field and source named. 60-second fetch timeout; malformed models and dropped connections give the same clean error.
- **Verified:** default source loads 391 Assay, 114 Tissue, 186 Tumor Type; `MC2_MODEL_REF=13.1.0` loads through the fallback names (the data-models tag is `13.1.0`, no `v`).

## P1. Output contract
- PublicationView, DatasetView and ToolView outputs have exactly the `v14.0.0` template headers (vendored; a network test compares them with the pinned remote).
- Bookkeeping columns go to `<stem>.pipeline.csv`. `GrantView Key` replaces `Publication Grant Number`; `PublicationView_id` = `Pubmed Id`; consortium/theme columns removed from the manifest.
- Tools workbook: Sheet1 is `manifest`; licenses normalized to SPDX and blanked (with a log line) when not in the 14.0.0 set.
- **Verified:** a 10-paper `--no-claude` run gives the same Assay/Tissue/Tumor Type values as P0 under the new headers. Opus ran the real P1 output through DCC D8's `processing-splits`: no renames, no drops, headers equal the templates.
- Internal row keys still use the legacy names; template names are applied on write. `--tools-template` is accepted but ignored, with a note.

## P2. Validation
- Rows are validated against the registered `MC2Center-<Type>-14.0.0` schemas (identical to GitHub `v14.0.0` apart from `$id` and property order). `--schema-source github|<dir>` selects another source.
- Only array-typed keys are split; `GrantView Key` stays whole. Failing rows are flagged in `validation_report.csv`, never dropped. `format: uri` and `format: date` are enforced; non-finite numbers are flagged.
- A tools-step failure no longer skips validation; exit codes are documented in `CLAUDE.md`/`README.md`.
- Bugs fixed on the way: `Tool Entity Type` `project` → `Project`; tool dates truncated to `YYYY-MM-DD`.

## P3. Vocabulary from the registered schemas
- Default source: the registered 14.0.0 schema enums (Assay/Tissue/Tumor Type from PublicationView, Species from DatasetView, ToolView enums and Sheet2). JSON-LD remains via `--model-jsonld`; minimum-term checks apply to both.
- **Verified:** counts equal the JSON-LD (391 / 114 / 186). Only set difference: `Immunoassay` (schema) vs `immunoassay` (JSON-LD).
- Bug fixed: Tumor Type has both `Not Applicable` and `Not-Applicable`; sorted schema order made the alias map pick the hyphenated form and collapse one paper to `Pan-cancer`. The spaced form now wins.

## Open items
- **DOI form (owner decision):** `Publication Doi` and `Tool Doi` are `format: uri`, and the PublicationView description asks for `https://www.doi.org/{doi}`; the pipeline writes bare DOIs, so every publication row is now flagged. Dataset DOIs are already URLs.
- **Upstream model gaps:** duplicate Tumor Type values `Not Applicable` / `Not-Applicable`; `immunoassay` casing in the JSON-LD.
- **Not done:** P4 branch rebases (`feature/manifest-review-app`, `cckp-dataset-linkage`, `refactor/extract-metadata-fill-logic`) rewrite shared branches and need the owner's go-ahead.
- `DatasetView_id` / `ToolView_id` are reported missing on every row (the DCC assigns them), which adds noise to the report.
