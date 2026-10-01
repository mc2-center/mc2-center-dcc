# Plan: data-models changes to finish the CDE model revision (PR #262) before release 14.0.0

**Repo:** `mc2-center/data-models`. Work on `cde-model-revisions` (PR #262); the local checkout is `kg-docs-restructure`, which tracks it.
**Why this plan exists:** it holds the data-models part of `mc2-center-dcc/plans/cde_model_alignment_harmonized.md`, the master plan, as a plan that can be executed on its own in this repo. Downstream work in `mc2-center-dcc`, the curation pipeline, and on Synapse waits on the release this plan produces.
**Related plans in this repo:** `plans/cde_model_revisions_integration.md`, the upstream integration plan, and `plans/cckp_metadata_and_cli_consolidation.md`, the future `cckp_metadata` package that will absorb `create_json_from_model.py` and related scripts. Make the changes below in the current scripts now; the package absorbs them later.

## Status as of 2026-10-01: `main` after #262 (`c234c467`)
| Item | Status |
|---|---|
| DM-1 to DM-10, DM-12 | **Done.** Recorded in `plans/data_models_cde_alignment_report.md` in data-models. Re-checked on `main`: `scripts/check_json_schemas.py` finds 0 failures across 38 schemas. |
| DM-11 | **Open**: issue #273. The notes are in `plans/release_14.0.0_notes.md`. Tag `v14.0.0` and publish the release (owner's go-ahead). |
| DM-13 | Open, deferred: issue #274. |
| DM-14 | **New, open:** a patch. See below. |
| DM-15 | **New, low priority**: issue #275. See below. |
| DM-16 | **New:** no `templates/*.csv` for ProjectView, Consortium, Institution or Theme. `make templates` should cover every class in `DATA` that has a manifest, or document the exclusions as `check_template_list.py` does. |

Items DM-1 to DM-12 below are kept as the record of what was asked; the report documents how each was done and where it differed.

## Context
PR #262 consolidates about 70 per-entity attributes into shared ones: `Assay`, `Species`, `Tissue`, `Tumor Type`, `License`, `Investigator`, `Sex`, and others. It moves sample and assay fields to NCIt or UBERON reference validation, adds foreign keys (`Consortium Key`, `Institution Key`, `PersonView Key`), and moves model generation to `synapseclient.extensions.curator`. Review across repos found defects and gaps that must be fixed **before** the JSON schemas are registered in Synapse as the source of truth. The latest tag is `13.1.0` and #262 is labelled `major`, so this release is **14.0.0**. The Synapse schema version must match the release version.

## Decisions already made (source: the owner's notes in the master plan)
| # | Decision |
|---|---|
| O1 | The "no grant" sentinel `Affiliated/Non-Grant Associated` is **explicitly allowed**. |
| O2 | `ImagingChannel` is **added to the Makefile `DATA` list**. Make sure the CSV reflects which classes are templates. |
| O4 | Moving to reference validation is accepted **if it doesn't touch portal templates**. **Checked:** the fields that lost their closed lists are Therapeutic Agent, Primary Diagnosis, Primary Site, Site of Origin, Known Metastasis Sites, Biospecimen Anatomic Site, Biospecimen Site of Resection or Biopsy, File Longitudinal Event Type and File Anatomic Site. They appear only in Biospecimen, Individual, Model, File View and the assay-level templates. None are in Publication, Dataset, Tool, Grant, Educational Resource, Person, Project, Theme, Consortium or Institution. The change is accepted. |
| O5 | Restore a CI gate for `make all` and `kg-pipeline`, **but after** the items below (DM-13). |
| O6 | Legacy study-license tokens are **mapped** to SPDX (DM-3). |
| O7 | Curator **does not allow spaces in property keys**, so property keys stay as `class_label`. |
| O8 | Enums get into display form through a **post-processing step** in `create_json_from_model.py` (DM-7). |
| 0.2 | `license` (DUOPlus6) and `License` **should be the same attribute**. |
| 0.3 | `dataCatalogLicense` **aligns to SPDX**. It's the same concept as `License` but stays a distinct annotation. |
| 0.6 | **DCA is deprecated.** Remove DCA-related content. |
| 1.2 | **Don't register `10x*` schema names**; they would be invalid. Follow the existing pattern of dropping `10x`, and change the name at the source (DM-6). |

## Work items

### DM-1 Restore Species enforcement
- **Problem.** `Species` is `Required=True` in the CSV, but it's missing from `required` in the DatasetView, FileView, Biospecimen, Model and Sequencing*.json schemas. The per-entity attributes it replaced (`Dataset Species` and the others) were enforced.
- **Work.** Find the cause in curator generation. Check whether a conditional `DependsOn Component` or a `Required` value on a template-specific row overrides the shared attribute. Fix it in the CSV, not by hand-editing the JSON.
- **Check.** `Species` is listed in `required` in every template that includes it.

### DM-2 Merge `license` into `License`
- **Problem.** The DUOPlus6 row `license` and the shared `License` both become the JSON key `License` in DataCatalog.json and Study.json.
- **Work.** Delete the `license` row. Point every template that listed `license` at `License`. Remove the `- name: license` entry from `modules/mapping.yaml`, whose source is `shared/studyLicense.csv`.
- **Check.** No duplicate property keys in any generated schema (add this to the DM-12 checks).

### DM-3 SPDX licenses everywhere, with the legacy tokens mapped (O6, 0.3)
- **Point `dataCatalogLicense` at the SPDX list.** In `mapping.yaml`, change its `src` from `shared/studyLicense.csv` to the SPDX list. Also move `tool/tool_license.csv` to `shared/license.csv`, since it now serves `License` for every entity. Update both `mapping.yaml` entries.
- **Record the legacy tokens.** Add each old token as a `Nonpreferred Terms` value on its SPDX row, so the downstream remap (master plan S4.3) is driven by the data:

  | Legacy | SPDX | Note |
  |---|---|---|
  | `CC0` | `CC0-1.0` | |
  | `CC_BY` | `CC-BY-4.0` | Matches the existing CV's own `SPDX:CC-BY-4.0` |
  | `CC_BY_NC` | `CC-BY-NC-4.0` | The existing CV says `SPDX:CC-BY-NC-3.0`. Owner-confirmed `CC-BY-NC-4.0`. |
  | `CC_BY_ND` | `CC-BY-ND-4.0` | |
  | `CC_BY_SA` | `CC-BY-SA-4.0` | |
  | `CC_BY_NC_ND` | `CC-BY-NC-ND-4.0` | |
  | `CC_BY_NC_SA` | `CC-BY-NC-SA-4.0` | |
  | `Apache_2` | `Apache-2.0` | |
  | `MIT` | `MIT` | |
  | `GPL_3` | `GPL-3.0` | The list only has the deprecated SPDX ID `GPL-3.0`, not `-only` or `-or-later`. Keep it as is. |
  | `BSD_3_Clause` | `BSD-3-Clause` | |

- **Retire `shared/studyLicense.csv`** once nothing references it.
- **Check.** All 11 SPDX targets are in the `License` value set (already verified).

### DM-4 Allow the "no grant" sentinel (O1)
- **Work.** Change the `Pattern` on `Grant Number` and `grantNumber` from `^CA\d{6}$` to `^(CA\d{6}|Affiliated/Non-Grant Associated)$`. Check for any other grant-number-shaped attribute that carries the same pattern.
- **Check.** The generated GrantView and DataCatalog schemas accept both `CA123456` and the sentinel, and reject `CA12345`.

### DM-5 ImagingChannel and the template flags (O2)
- **Makefile.** Add `ImagingChannel` to `DATA` in the `Makefile`.
- **Template flags.** Check `IsTemplate` against the generation list in both directions:
  - `Imaging Channel` is already `True`.
  - **`10x Visium RNA Level 1` has a blank `IsTemplate` but is in `DATA`.** Set it to `True`, or take it out of `DATA`.
  - Every class in `DATA` must be `IsTemplate=True`, and every `IsTemplate=True` class must either be in `DATA` or be listed as excluded, with the reason.
- **Check.** A small script that diffs `DATA` against the classes with `IsTemplate=True` reports no differences.

### DM-6 Drop the `10x` prefix at the source (1.2)
- **Work.** Rename the template classes:
  - `10x Visium Auxiliary Files` becomes `Visium Auxiliary Files`.
  - `10x Visium RNA Level 1` through `4` become `Visium RNA Level 1` through `4`.
  - Their `*_id` keys lose the prefix too: `10xVisiumRNALevel2_id` becomes `VisiumRNALevel2_id`.
  - Keep "10x Genomics" in the descriptions.
- **Also update:** the `Makefile` `DATA` list, the `json_schemas/` filenames, `templates/*.csv` and any `DependsOn` references.
- **Result.** Class labels become `VisiumRNALevel1`, which is valid and matches the existing registered names. That removes the need for the special case in `mc2-center-dcc/utils/csv_to_ttl.py` (lines 258-259 and 476).
- **Check.** No template name, class label or key starts with a digit.

### DM-7 Enum labels (X1), with keys kept as `class_label` (O7). Decided: post-processing step (O8)
- **Problem.** With `class_label` (required by O7), enum values are squashed to camelCase: `RNASequencing`, `AcinarCellCarcinoma`. That's true of the files in `json_schemas/` on both branches and of the live registered `MC2Center-PublicationView-13.0.0`. Portal tables, manifests and the curation pipeline all store the spaced value `RNA Sequencing`.
- **No built-in fix.** Curator 4.13 has no option for class-label keys with display-label enums. The single `use_display_labels` flag passed to `create_json_schema()` in `synapseclient/extensions/curator/schema_generation.py` controls both.
- **Work (owner decision O8).** Add a deterministic post-processing step to `create_json_from_model.py`, after `generate_jsonschema`:
  1. Build a class-label → `sms:displayName` map from the JSON-LD produced in the same build (`mc2.model.jsonld`). The map is one to one.
  2. Rewrite every `enum` value in each generated schema: top level, `items`, and any `if`/`then`/`oneOf`/`anyOf` branches.
  3. Fail if any enum value has no display name, rather than leaving it squashed.
  4. Leave property keys and `required` untouched (class labels).
- **Label it.** Mark the step as compensating for a missing curator option.
- **Optional follow-up.** File a request on `Sage-Bionetworks/synapsePythonClient` for a separate enum-label option (e.g. `enum_labels="display_label"`), and remove the step once that ships.
- **Where it lives.** When `cckp_metadata`'s `generate_json_schemas` absorbs this script (per the CLI consolidation plan), the step moves with it.
- **Tests.**
  - A fixture schema with nested `items` and conditional enums comes out in display form.
  - An unmapped value raises an error.
- **Check.** Every enum in `json_schemas/*.json` is in display form: `RNA Sequencing` is present and `RNASequencing` absent. The keys are class labels. The clean set of existing portal values validates.

### DM-8 Generate the missing portal schemas
- **Work.** Add `PersonView`, `ProjectView`, `Consortium`, `Institution` and `Theme` to `DATA`. None of them has a schema today, so their tables have no source of truth.
- **Check.** Each is generated and passes the DM-12 checks.

### DM-9 Remove DCA (0.6)
- **Work.** Delete `dca_config/`, and remove DCA references from `README.md`, `CLAUDE.md`, `.github/ISSUE_TEMPLATE/prepare-data-release.md` and anything the docs build links to (`nav.yml`, `docs/`).
- **Check.** `git grep -i "dca\|data curator"` returns only historical plans.

### DM-10 Confirm nothing uses the deleted consortium file
- **Work.** Run `git grep consortium_name`. The only hits should be history. This is the file that broke `crosswalk_scdm.py`, which `191c8f1` fixed.

### DM-11 Release 14.0.0
- **Release notes:**
  - The consolidated attributes, as an old-to-new table.
  - The fields moved to reference validation (sample and assay only; see O4).
  - Value-set narrowings: Sex, Tumor Grade, Acquisition Method, Composition, Preservation Method and Medium, File Format.
  - The legacy license mapping (DM-3).
  - The Visium renames (DM-6).
  - The DCA removal (DM-9).
  - `make all` now runs `convert`.
  - `schematicpy` and the `make qc` target are removed. `qc_model/qc_attribute_mapping.csv` stays, because `union_qc.py` uses it.
  - The 18 `templates/*.csv` files have renamed headers.
- **Tag and handoff.** Tag the release `v14.0.0`. Registering the schemas at `14.0.0` happens downstream, in master plan S1.

### DM-12 Build and schema checks (before the tag)
- **Builds.** `make all` and, in kg-pipeline, `make schema && make test` pass from a clean clone.
- **Schema checks.** A small check script, also usable as the first step of the DM-13 CI gate, confirms for every schema in `json_schemas/`:
  - no duplicate keys
  - no key contains a space
  - no key or name starts with a digit
  - enums are in display form
  - `required` includes every CSV `Required=True` attribute of that template

### DM-13 CI gate (O5, deferred until DM-1 to DM-12 are done)
- **Work.** A workflow that runs `make all`, the DM-12 check script, and `kg-pipeline`'s `make test` on PRs. Coordinate with Phase 0 of `cckp_metadata_and_cli_consolidation.md`, which adds pytest and ruff CI in this repo, so it's one workflow, not two.

### DM-14 Fix the `qc_attribute_mapping.csv` DatasetView row (new)
- **Problem.** The DatasetView row in `qc_model/qc_attribute_mapping.csv` still says `Data Use Codes`, but on `main` the DatasetView template uses `Dataset Data Use Codes`. `mc2-center-dcc/portal_tables/union_qc.py` reads this file (`-p`) to aggregate duplicate rows, so it won't aggregate that column.
- **Work.** Rename the attribute in that row, and leave its `"",".join` mapping as is.
- **Check.** Extend `scripts/check_template_list.py`, or add a small test, so that every `qc_attribute_mapping.csv` attribute (apart from the `entityId` bookkeeping column) is a header in `templates/<component>.csv`. Today that check finds exactly this one mismatch.

### DM-15 Make `GrantView Key` a list (new, low priority, for a later release)
- **Problem.** `GrantView Key` is a `string` holding comma-separated grant numbers, with the unanchored pattern `(CA\d{6}|Affiliated/Non-Grant Associated)`. JSON Schema patterns are unanchored, so any string that *contains* one valid number passes, e.g. `CA123456, junk`.
- **Option.** Make it a `string_list` with an anchored per-item pattern.
- **Why later.** This changes its type in every portal template's schema and in downstream parsing, so it goes in a future release, not a patch.

## Order of work
1. DM-1, DM-2, DM-3, DM-4, DM-5, DM-6, DM-8, DM-9 and DM-10 are independent CSV and Makefile edits. They can go to parallel Sonnet tasks, one commit each. DM-2 and DM-3 both touch `mapping.yaml`, so the same agent does them.
2. DM-7, the post-processing step (O8 decided). Do it after DM-2 and DM-6, which change labels it maps.
3. Regenerate everything (`make all`), then run DM-12.
4. DM-11: release notes are done; tag `v14.0.0` (needs explicit go-ahead). **This is the next step.** DM-14 can go in before the tag or as 14.0.1.
5. DM-13 comes later.

## What happens after the tag (not this repo)
- **Synapse:** register at 14.0.0, after the ≥14.0.0 deletions (master plan S1, O9).
- **`mc2-center-dcc`:** the sync and crosswalk updates (master plan, section B).
- **`ai-curation-pipeline`:** bump its pinned model ref to `v14.0.0` (`mc2-center-dcc/plans/ai_curation_pipeline_alignment.md`).
