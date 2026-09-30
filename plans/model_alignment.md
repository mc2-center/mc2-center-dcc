# Plan: Align mc2-center-dcc and CCKP Synapse tables with the CDE-revised data models

## Context
data-models branch `cde-model-revisions` (PR #262, `major`; local checkout `kg-docs-restructure`) changes about 70 attributes compared with `origin/main`:
- Per-entity prefixes are dropped: `Publication/Dataset Assay` → `Assay`, `Dataset Species` → `Species`, `Tool/Resource License` → `License`, `Grant/Project Investigator` → `Investigator`.
- Consortium names are replaced by the `Consortium Key` foreign key, and `consortium_id` values are now `program.*` IDs.
- New foreign keys are added: `Institution Key`, `PersonView Key`, `Project Short Name`.
- Several value sets are revised: Sex, Tumor Grade, License, and anatomic sites (now `^UBERON:\d+$`).

All schemas will be registered in Synapse (org `MC2Center`) as the source of truth. Today:
- Nothing is bound to the portal project (syn21498902) or its tables.
- The sync scripts in this repo hardcode the old attribute names.
- Several portal and manifest table columns have the wrong types or names for the new model.

Intended outcome: schemas are registered cleanly, the sync pipeline reads the new attribute keys, and the portal tables hold schema-valid values. The portal's app config doesn't need to change.

Decisions made (by the user):
1. **Portal columns stay camelCase as a display layer.** A versioned crosswalk maps schema keys to portal columns. Portal tables are not renamed to schema keys.
2. **Consortium columns keep display names** (`CSBC`, `Sage Bionetworks`). The sync resolves `Consortium Key` IDs through the Consortium table.
3. This plan is also saved as `mc2-center-dcc/plans/model_alignment.md`.

Principle: binding a Synapse JSON schema validates entity annotations, not table rows. So validation happens upstream (RecordSets and curation tasks bound to schemas), and portal tables are derived from validated records.

Evidence for everything below is in the session scratchpad: `model_diff.md`, `dcc_inventory.md` and `synapse_cols.json`.

---

## Phase 0 — Fix the model upstream before registering (data-models repo, separate PR)
These block "schemas as source of truth" and should land before any version is registered.
- **Restore Species enforcement.** `Species` is Required in the CSV but missing from `required` in DatasetView, FileView, Biospecimen, Model and Sequencing*.json. Find why the curator JSON generation drops it and fix it.
- **Fix the key collision.** `license` (the DUOPlus6 row) and `License` both become the JSON key `License` in DataCatalog.json and Study.json. Rename one of them.
- **Align the DataCatalog license list.** `dataCatalogLicense` still uses the 11-token `studyLicense.csv`. Switch it to the SPDX list, or document why it differs.
- **Decide how to handle the `Affiliated/Non-Grant Associated` sentinel.** It fails `^CA\d{6}$`, and `sync_projects.py`, `sync_people.py` and `gen-mp-csv.py` use it. Either add an explicit allowed value or model "no grant" as an empty `GrantView Key`.
- **Generate and register JSON schemas for Person, Project, Consortium, Institution and Theme** (`create_json_from_model.py`). They don't exist yet, so those tables have no source of truth.
- **Tag the release** (e.g. `v14.0.0`) so downstream consumers can pin to it.

## Phase 1 — Clean up the schema registry (Synapse, `MC2Center` org)
- **Fix version ordering.** PublicationView `15.0.0` (Jun) sorts above `13.0.0` (Jul), so "highest version" resolves to a stale schema. Register every schema from this release at **16.0.0**, above every existing version, so ordering is unambiguous.
- **Visium names:** register `10xVisium*` (the renamed Visium schemas). Mark `VisiumRNALevel1-4`, `VisiumAuxiliaryFiles` and `ImagingChannel` deprecated in the registry table syn69735275. Don't delete them, because tasks may still reference them.
- **Update the registration script.** `local_inputs/Makefile` hardcodes `CURRENT_VERSION := 13.0.0`. Move the registration target into a tracked script, e.g. a `make register-json VERSION=16.0.0` wrapper around `utils/synapse_json_schema_bind.py --no_bind`.
- **Fill the registry ID.** `SCHEMA_REGISTRY_TABLE_ID` in `curator_tools/query_schema_registry.py` is blank; set it to syn69735275.
- **Keep the registry table current.** Make sure syn69735275 rows are appended for the new versions.

## Phase 2 — Update this repo's code (the main implementation)
Pattern for every sync script:
- Replace the old attribute keys with the new ones in rename maps, string-list lists and `col_order`.
- Keep the camelCase portal column targets and the positional `col_order`.
- Reuse `portal_tables/utils.py`: `convert_to_stringlist`, `update_table`, `get_args`, `syn_login`.

**2a. Add a crosswalk module.** New file `portal_tables/crosswalk.py`:
- Per component, a dict mapping schema key → portal column (e.g. `"Assay": "assay"`), plus the model version it targets.
- It replaces the scattered rename dicts and `annotations/attribute_dictionary.py`, which is re-exported from it or deleted after its callers move.
- Add a small `check_crosswalk(table_id, component)` helper. It asserts that the live Synapse column order equals the crosswalk's order, to guard the positional `df.values.tolist()` write in `utils.update_table`.

**2b. Add a consortium lookup.** New helper in `portal_tables/utils.py`: `consortium_display_names(syn, ids) -> list[str]`, read from the Consortium table.
- Used by grants, people, projects and `create_grant_projects.py`.
- Unknown IDs fail loudly, never silently drop.

**2c. Script updates.** Each is independent after 2a/2b, so they can run in parallel.

| Script | Changes |
|---|---|
| `portal_tables/sync_publications.py` | `Publication Assay/Tumor Type/Tissue` become `Assay`, `Tumor Type`, `Tissue`. Replace positional `row[4]` (`:34`) with `row["GrantView Key"]`. |
| `portal_tables/sync_datasets.py` | `DatasetAssay/Species/Tissue/TumorType` become `Assay`, `Species`, `Tissue`, `TumorType`. `DatasetView_id` becomes `DatasetViewId`. Fix `:117-123`: an unknown DUO code must not skip `sourceRepository`/`downloadType`/`downloadSynId`; log it and continue with the remaining fields. |
| `portal_tables/sync_tools.py` | `ToolLicense` becomes `License`; add it to the string-list columns. |
| `portal_tables/sync_education.py` | `ResourceLicense` becomes `License`; add it to the string-list columns. |
| `portal_tables/sync_grants.py`, `create_grant_projects.py` | `Grant Investigator` becomes `Investigator` (list). `Grant Consortium Name` becomes `Consortium Key`, resolved via the 2b lookup. Fix `sync_grants.py:81` shared `updated_scope` list. |
| `portal_tables/sync_projects.py` | `ProjectInvestigator` becomes `Investigator`. `ProjectGrantNumber` becomes `GrantViewKey`. Consortia come from `ConsortiumKey` via the lookup. Emit a new `projectShortName`. |
| `portal_tables/sync_people.py` | `personConsortiumName` becomes `ConsortiumKey`, resolved via the lookup. `personGrantNumber` becomes `GrantViewKey`. |

**2d. Update the annotation and utility scripts.**
- Scripts: `annotations/processing-splits.py`, `add_cols.py`, `schema_update.py`, `edit_legacy_annotations.py`, `split_manifest_grants.py`, `utils/merge_and_correct_manifests.py`, `utils/table_to_annotations.py`, `utils/check_publications_status.py`.
- Change: same old-to-new rename, sourced from `crosswalk.py`.
- Pin the `all_valid_values.csv` URL (`split_manifest_grants.py:117`, `edit_legacy_annotations.py:38`) to the release tag, not `main`.
- Also fix the existing bugs:
  - `table_to_annotations.py:525` `keys_to_drop=None` TypeError.
  - `check_publications_status.py`: `pubMedUrl` should be `pubMedLink`.

**2e. Workflows.** `.github/workflows/publications-status-check.yml` and `update-theme-graphs.yml` run on a monthly cron. Make sure the scripts they call are updated in the same PR.

**2f. Manifest validation path.** `annotations/upload-manifests.py` and `union_qc.py` shell out to `schematic` (`-tcn display_name`). Change them to validate through `synapseclient.extensions.curator` against the registered 16.0.0 schemas, matching `utils/create_curation_task.py`. Keep schematic only if curator has no equivalent for the step, and flag that for approval (no silent workaround).

## Phase 3 — Synapse table changes (Opus runs these with explicit go-ahead; snapshot tables first)
Snapshot each table (query to CSV and store it as a table version) before altering it.

**Portal tables** (keep names; fix types and add columns):
- Tools syn26127427 `license`: STRING becomes STRING_LIST. Split the 4 comma-joined values, e.g. `GPL-3.0, MIT`.
- Education syn51497305 `license`: STRING becomes STRING_LIST.
- Grants syn21918972 and Projects syn21868602 `investigator`: STRING becomes STRING_LIST. Split on `, `.
- Projects syn21868602: add `projectShortName` (STRING).
- Grants, People and Projects `consortium`: unchanged; they still hold display names (decision 2).
- Datasets syn21897968: no change now. Record the 15 new DUO fields plus `License` as a follow-up for whether the portal shows them.

**Manifest tables:** rename and add columns to match the new schema keys, or better, replace them with RecordSets bound to the 16.0.0 schemas.
- Grant syn53259587:
  - `Grant Investigator` becomes `Investigator`.
  - `Grant Consortium Name` becomes `Consortium Key`, with values mapped to `program.*`.
  - Add `Institution Key` and `PersonView Key`.
- Project syn59074382:
  - `Project Investigator` becomes `Investigator`.
  - `Project Consortium Name` becomes `Consortium Key`.
  - `Project Grant Number` becomes `GrantView Key`.
  - Add `PersonView Key` and `Project Short Name`.
- PersonView syn38301033:
  - `personConsortiumName` becomes `ConsortiumKey`.
  - `personGrantNumber` becomes `GrantViewKey`.
  - Add `InstitutionKey`.
- Publication, dataset, tool and education manifest CSV files (syn53478776, syn53478774, syn53479671, syn53651540): regenerate them with the new keys, then point the sync `--manifest_id` defaults at them.

**Bindings:** bind the 16.0.0 schemas to the RecordSets or folders that feed each manifest (`utils/synapse_json_schema_bind.py`). Portal tables stay unbound (see Principle).

## Phase 4 — Clean existing values (curation, after Phase 3)
Values in live portal data that fail the new enums. For each, fix the source record, or propose adding the term to the model.
- **Publications syn21868591:**
  - 60 `assay` values: typos such as `Artificial Intelliegnce`, leading spaces, synonyms like `ChIP-seq` and `ATAC-seq`.
  - 5 `tissue` values: `Not Applicale`, `Not Appplicable`, `Prostate`, `Breast Neoplasm`, `Pan-cancer`.
  - 1 `tumorType` value: `Pan-Cancer`.
- **Datasets syn21897968:** 3 `assay` values: `ELISpot`, `Micro-CT`, `snATAC-seq`.
- **Curated annotations:** labels in the replaced lists (anatomic site, therapeutic agent, primary diagnosis) need UBERON/NCIt IDs. Sex and Tumor Grade values need remapping (e.g. `G1` becomes `G1 Low Grade`). Script this through `annotations/update_pending_annotations.py`-style batch edits.

---

## Execution / delegation
- **Phase 0:** a Sonnet SME in the data-models repo, as its own PR.
- **Phase 1** and **Phase 3:** Opus directly, with explicit approval. They write to Synapse, so they're outward-facing.
- **Phase 2:** first 2a+2b (one Sonnet task), then 2c rows plus 2d as parallel Sonnet tasks on `model-alignment`.
- **Commits:** one commit per script group.
- **Review:** an independent code review before `gh pr create`.
- **Phase 4:** a curation worklist; Haiku can generate the correction CSV mechanically, and a human reviews it.

## Verification
- **Dry run each sync.** Run each sync with `--dryrun` (already supported via `utils.get_args`) against the regenerated manifests. Diff `--output_csv` against a pre-change dump of the same portal table; the only differences should be the intended type and value changes.
- **Column-order check.** `check_crosswalk()` passes for all 7 portal tables. The live column order is read back through `/entity/{id}/column`.
- **Upstream validation.** Validate each regenerated manifest or RecordSet against its 16.0.0 schema through `synapseclient.extensions.curator` with zero errors. The Phase 4 values are the expected exceptions until curated.
- **Registry.** `POST /schema/version/list` for each MC2Center schema shows 16.0.0 as the highest version, and syn69735275 has matching rows.
- **Workflows.** Trigger `sync-to-portal.yml` with `workflow_dispatch` against a staging copy of one portal table before running it on production.
- **Portal smoke test.** Filters and facets on cancercomplexity.synapse.org (Publications, Datasets, Tools, Grants) still render after Phase 3.
