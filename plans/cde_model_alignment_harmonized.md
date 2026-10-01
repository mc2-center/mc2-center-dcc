# Master plan: land the CDE model revisions (data-models PR #262 → release 14.0.0) across data-models, mc2-center-dcc and CCKP Synapse

**Sources:**
- **[U]** `data-models/plans/cde_model_revisions_integration.md`
- **[D]** `plans/model_alignment.md`
- **[C]** `plans/ai_curation_pipeline_alignment.md`
- the owner's review notes on the previous revision of this file, captured in the decision log below
- the code merged into `model-alignment` from the `synapse-client-oop-refresh` and `build-cli-tool` branches (`plans/synapseclient_oop_migration*.md`, `plans/cckp_metadata_and_cli_consolidation.md`)

**Structure:**
- **Section A: data-models.** Summary only. The work plan is its own document, **`plans/data_models_cde_alignment.md`**, to be carried out in that repo.
- **Section B: mc2-center-dcc code.**
- **Section C: Synapse tables and operations.**

The cross-repo sequencing and verification come after section C.

## Context
PR #262 makes three kinds of change:
- It consolidates about 70 per-entity attributes into shared ones.
- It moves sample and assay fields to NCIt or UBERON reference validation.
- It adds foreign keys: `Consortium Key` (whose values are now `program.*` IDs), `Institution Key` and `PersonView Key`.

It also moves model generation to `synapseclient.extensions.curator`.

Target state:
- The data-models schemas are registered in Synapse (org `MC2Center`, version **14.0.0**, matching the release) as the source of truth.
- The DCC sync pipeline and the AI curation pipeline read the new attribute keys.
- The portal tables hold values that are valid against the schemas, with no change to the portal app config.

## Decision log
| # | Decision | Status |
|---|---|---|
| 1 | Portal tables keep their camelCase columns. A versioned crosswalk maps schema keys to portal columns. | Decided |
| 2 | Portal consortium columns keep display names, resolved from `Consortium Key` through the Consortium table. | Decided |
| 3 | Validation happens upstream (RecordSets and curation tasks bound to schemas). Portal tables are derived from validated records and aren't bound. | Decided |
| 4 | **Schema property keys are `class_label`** (`DatasetViewId`, `GrantViewKey`, `TumorType`), because curator doesn't allow spaces in keys (O7). **Enums must be in display form** (X1); how is decided in O8. This replaces the earlier `display_label` decision. | Decided (O8 open) |
| O1 | The "no grant" sentinel is allowed explicitly: pattern `^(CA\d{6}\|Affiliated/Non-Grant Associated)$`. | Decided → DM-4 |
| O2 | Add `ImagingChannel` to the Makefile `DATA` list, and make the CSV's template flags consistent. | Decided → DM-5 |
| O3 | The portal doesn't get the 15 new DatasetView DUO fields. `sync_datasets.py` keeps deriving portal access info from DUO codes (`DUO_DICT`). | Decided → B3 |
| O4 | Reference validation is accepted. **Checked:** it affects only Biospecimen, Individual, Model, File View and the assay templates, with no portal template involved. | Decided, verified |
| O5 | Restore a CI gate in data-models, after the higher-priority work. | Decided, deferred → DM-13 |
| O6 | Map legacy license tokens to SPDX. | Decided → DM-3, S4.3 |
| O7 | Curator doesn't allow spaces in property keys. | Answered → decision 4 |
| O8 | **Enums in display form, keys as class labels.** Curator 4.13 has a single `use_display_labels` flag for keys and enums. **Decision (owner): add the post-processing step** to `create_json_from_model.py`, which rewrites each enum from its class label to its `sms:displayName`. Filing an upstream synapsePythonClient request for a separate enum-label option is an optional follow-up, so the step can be retired later. | Decided → DM-7 |
| O9 | **Which ≥14.0.0 versions to delete.** **Decision (owner):** delete the stray data-model versions (PublicationView 15.0.0; Biospecimen, Model and SequencingRNALevel1 15.0.0 and 15.0.1), plus **`StudyAR`** (14.0.0–30.0.0) and **`AccessRequirementCA123`** (17.0.0–18.0.0). **Keep every `AccessRequirementCA261841` schema.** Before the delete, confirm whether "remove" means only their ≥14.0.0 versions or the whole schemas: `StudyAR` also has 1.0.0–3.0.0, and lower `AccessRequirementCA123` versions may exist. | Decided → S1.1 (scope confirmed at execution) |
| 0.2 / 0.3 | `license` merges into `License`. `dataCatalogLicense` stays a separate annotation on the SPDX list. | Decided → DM-2, DM-3 |
| 0.6 | DCA is deprecated; remove it. | Decided → DM-9 |
| 1.2 | No `10x*` schema names. Drop `10x` at the source. | Decided → DM-6 |
| 1.4 | Ignore `curator_tools/`. The OOP refresh and the CLI plan cover schema tooling; the CLI plan deletes `curator_tools/`. | Decided → B6 |

---

## Section A: data-models (the work plan is `plans/data_models_cde_alignment.md`)
Summary of the items, all of which must be done before the `v14.0.0` tag:

| Item | What |
|---|---|
| DM-1 | Restore `Species` in `required` |
| DM-2 | Merge `license` into `License` (clears the duplicate key) |
| DM-3 | SPDX list for `License` and `dataCatalogLicense`; legacy tokens recorded as Nonpreferred Terms |
| DM-4 | Sentinel allowed in the grant-number pattern |
| DM-5 | `ImagingChannel` added to `DATA`; `IsTemplate` matches `DATA`. **Found:** `10x Visium RNA Level 1` is generated but not flagged as a template. |
| DM-6 | Drop `10x` from the Visium template names at the source |
| DM-7 | Display-form enums with class-label keys, via a post-processing step (O8) |
| DM-8 | Generate PersonView, ProjectView, Consortium, Institution and Theme schemas |
| DM-9 | Remove DCA |
| DM-10 | Check for leftover `consortium_name` references |
| DM-11 | Release notes; tag `v14.0.0` |
| DM-12 | Build and schema checks: no duplicate keys, no spaces, no leading digits, display enums, `required` matches the CSV |
| DM-13 | CI gate (deferred; merged with the CLI plan's CI Phase 0) |

---

## Section B: mc2-center-dcc code
Everything below runs against the code **as merged**, after the synapseclient OOP migration. The old positional `df.values.tolist()` write no longer exists.

### B0. Hotfix: `update_table` now matches columns by name (**done**, in the commit that follows this plan revision)
- **What changed.** `portal_tables/utils.py:update_table` now calls `Table.store_rows(values=df)`. synapseclient uploads the DataFrame as CSV **with a header row**, via `_chunk_and_upload_df`, `header=(start == 0)`. Synapse then matches columns **by name**, not by position.
- **What that breaks.** Before, positional writes let the scripts keep manifest-style names in `col_order`. Comparing each script's `col_order` with the live portal columns:

  | Script | Names that match the live table |
  |---|---|
  | `sync_publications.py` | 3 of 20 (`Publication Doi` vs `doi`) |
  | `sync_datasets.py` | 5 of 24 (`DatasetView_id` vs `datasetId`) |
  | `sync_tools.py` | 3 of 46 |
  | `sync_education.py` | 1 of 27 |
  | `sync_projects.py` | 2 of 9 |
  | `sync_people.py` | 12 of 18 (the table has 20 columns) |
  | `sync_grants.py` | 17 of 17 (fine) |

- **Why it's dangerous.** `update_table` **deletes every row first**, then stores. So the first non-dryrun run of any of those six scripts empties the portal table, and the store then fails or maps columns wrongly. Only the snapshot `update_table` takes first would allow recovery. The OOP migration report itself notes that this write path was never exercised live, and its dry runs skip it.
- **Fix:**
  1. In `update_table`, read the live column names (`Table(id).get(include_columns=True)`).
  2. Rename `df` columns to them explicitly. For now, assert the column counts are equal and map by position; once B1 lands, use the crosswalk.
  3. Do this **before** truncating, and raise an error if it fails.
  4. Tables with extra columns (`test_publicationId` and `test_toolId` in People) are left blank, not an error.
- **Why order was already right but the write still breaks.** The `col_order` lists already put columns in the live table's order, but they're labelled with manifest-style names. The old `syn.store(Table(id, df.values.tolist()))` sent rows with no header, so only order mattered. `store_rows(df)` sends the DataFrame's column names as a header, so the labels now matter too. The fix relabels by position, using the hardcoded order the scripts already maintain.
- **Status: fixed.** `update_table` reads the live columns first. It refuses, before the snapshot or truncate, if the DataFrame has more columns than the table. Otherwise it relabels the DataFrame columns by position to the live names; trailing live columns it doesn't cover are left empty, as before.
- **Verified:**
  - Dry-run outputs of publications, tools, education, projects, people and grants were passed through `update_table`, with snapshot, delete and store mocked and the real live column fetch. Every row and value came through, and the columns arrived under the live names.
  - A forced extra column raised an error before `delete_rows`.
  - `sync_datasets.py` was checked statically only (24 columns, matching the live table); its full dry run takes about 25 minutes.
- **Still to check:** one real non-dryrun write to a **scratch copy** of a portal table, to confirm Synapse accepts the header-matched upload end to end. This needs go-ahead to create, and later delete, the scratch table.

### B1. Crosswalk and consortium lookup
- **Crosswalk.** A new `portal_tables/crosswalk.py` holds, per component, an ordered mapping from schema key (**class label**, decision 4) to portal column, plus the model version it targets.
  - It replaces the scattered rename maps and `annotations/attribute_dictionary.py`.
  - `check_crosswalk(table_id, component)` asserts the live column order. B0's rename uses it.
  - It moves into `mc2dcc/sync/_shared.py` when the CLI plan's Phase 2 lands.
- **Consortium lookup.** `consortium_display_names(syn, ids)` goes in `portal_tables/utils.py`, reads the Consortium table, and fails loudly on an unknown ID.
- **Reuse:** `portal_tables/utils.py` `convert_to_stringlist`, `update_table`, `get_args`, `syn_login` (now on the OOP `Synapse().login()`).

### B2. Sync scripts: move input keys to class labels
Several scripts already normalize manifest headers with `manifest.columns.str.replace(" ", "")`: `sync_datasets.py:263`, `sync_education.py:115` and `sync_people.py:84`. That produces nearly the class label, except `DatasetView_id` becomes `DatasetViewId` in the class label. Replace the space-stripping with the crosswalk's key mapping.

| Script | Changes |
|---|---|
| `sync_publications.py` | Read class-label keys: `Assay`, `TumorType`, `Tissue`, `GrantViewKey`. `PublicationAssay` and the other prefixed keys become the shared keys. Replace positional `row[4]` with `row["GrantViewKey"]`. |
| `sync_datasets.py` | `DatasetAssay`, `DatasetSpecies`, `DatasetTissue` and `DatasetTumorType` become `Assay`, `Species`, `Tissue`, `TumorType`. `DatasetView_id` becomes `DatasetViewId`. Fix the unknown-DUO-code `continue` at `:117-123` that skips `sourceRepository`, `downloadType` and `downloadSynId`. |
| `sync_tools.py` / `sync_education.py` | `ToolLicense` / `ResourceLicense` become `License`, as a string list. |
| `sync_grants.py`, `create_grant_projects.py` | `Grant Investigator` becomes `Investigator`, as a list. `Grant Consortium Name` becomes `ConsortiumKey`, resolved via B1. The shared `updated_scope` bug is fixed on the `EntityView` OOP path from the OOP refresh; recheck it. |
| `sync_projects.py` | `ProjectInvestigator` becomes `Investigator`. `ProjectGrantNumber` becomes `GrantViewKey`. Consortia come via B1. Add `ProjectShortName`, going to `projectShortName`. |
| `sync_people.py` | `personConsortiumName` becomes `ConsortiumKey`, resolved via B1. `personGrantNumber` becomes `GrantViewKey`. |

### B3. Datasets: access info stays DUO-derived (O3)
- No new portal columns.
- `DUO_DICT` stays the source for `accessInformation` / `sourceRepository`.
- Log unknown DUO codes, and don't let them skip the row (the fix is in B2).

### B4. Annotation and utility scripts
- **Scripts:** `annotations/processing-splits.py` (including `"Tool License"`), `add_cols.py` (the CLI plan flags it as possibly dead; confirm before editing), `schema_update.py`, `edit_legacy_annotations.py`, `split_manifest_grants.py`, `utils/merge_and_correct_manifests.py`, `utils/table_to_annotations.py`, `utils/check_publications_status.py`.
- **Change:** rename via `crosswalk.py`.
- **Pin** the `all_valid_values.csv` URL (`split_manifest_grants.py:117`, `edit_legacy_annotations.py:38`) to `v14.0.0`.
- **`processing-splits.py`** only splits by component once the curation pipeline emits schema-key headers ([C] P1).
- **Existing bugs to fix:**
  - `table_to_annotations.py:525` `keys_to_drop=None` TypeError.
  - `check_publications_status.py`: `pubMedUrl` should be `pubMedLink`.
  - `File Assay Category` is now split out of `Assay`. Check any File-level annotation writes.

### B5. `csv_to_ttl.py`
- Check that it still parses the revised `Properties` (CDE tags) and `DependsOn`.
- Remove the `10x_` stripping at lines 258-259 and 476 once DM-6 lands.
- Fix the `is_enum` inconsistency (lines 196 and 199).

### B6. Schema tooling: follow the OOP refresh and CLI plan; ignore `curator_tools/` (decision 1.4)
- **Registration** uses the rewritten `utils/synapse_json_schema_bind.py`, now on the JSON Schema OOP models. Add a tracked `register-json VERSION=14.0.0` entry point for it (a Makefile target now, `mc2dcc curation register` later). This replaces the untracked `local_inputs/Makefile`, which hardcodes 13.0.0.
- **Validation:** manifest validation moves off `schematic` through the CLI plan's `cckp_metadata` jsonschema validator (`union_qc.py`, `upload-manifests.py`, `upload-workflow.sh`). Don't build a separate curator-validation path here.

### B7. Workflows
- **Cron deadline.** `update-theme-graphs.yml` and `publications-status-check.yml` run on `0 0 1 * *`, and now `pip install -r requirements.txt`. The scripts they call must be updated **before the first of the month after the 14.0.0 cutover**.
- **Cron scripts are untested.** `tally_themes.py` and `check_publications_status.py` have no `--dryrun` and weren't run live after the OOP migration. Add a `--dryrun` to each before the first cron run.

### B8. Order relative to the CLI consolidation plan
- **Model alignment lands first**, in the current scripts: B0–B5 are urgent (B0 and the cron deadline) and small.
- **Then the CLI moves the aligned code.** The CLI plan's Phase 2 (`mc2dcc sync …`) moves the aligned scripts over; it doesn't rewrite the model logic.
- **The crosswalk becomes shared** as `mc2dcc/sync/_shared.py` at that point.

### B9. AI curation pipeline (companion plan [C])
- **Now:** P0, vocabulary hardening. It can land now, and **must land before #262 merges**.
- **With B2:** P1, the output headers. Update P1 to emit **class-label** keys per decision 4, replacing the display-label headers.
- **Later:** P2 and P3 follow the S1 registration.

---

## Section C: Synapse tables and operations (Opus runs these with explicit approval; snapshot first)

### S0. Before anything else
- **B0 has to ship before anyone runs a non-dryrun sync.**
- `sync-to-portal.yml` is `workflow_dispatch` only. Tell the people who run it to hold off until B0 is merged.

### S1. Schema registry (`MC2Center`)
- **S1.1 Delete** the stray data-model versions ≥14.0.0:
  - `PublicationView` 15.0.0
  - `Biospecimen` 15.0.0 and 15.0.1
  - `Model` 15.0.0 and 15.0.1
  - `SequencingRNALevel1` 15.0.0 and 15.0.1
  - `StudyAR` 14.0.0–30.0.0 and `AccessRequirementCA123` 17.0.0–18.0.0 (O9). Confirm at execution whether their lower versions go too.
  - **Keep** every `AccessRequirementCA261841` version.
  - This needs explicit go-ahead; deletion can't be undone.
- **S1.2 Register** every schema from the `v14.0.0` tag at **14.0.0**: keys are class labels and enums are in display form (DM-7). Use the B6 entry point.
  - Visium schemas register under the names without `10x` (DM-6), continuing the existing `VisiumRNALevel*` names.
  - `ImagingChannel` gets a 14.0.0 version (DM-5).
  - PersonView, ProjectView, Consortium, Institution and Theme are new (DM-8).
- **S1.3 Registry table.** Add 14.0.0 rows to syn69735275, and mark earlier data-model versions superseded (their enums are camelCase, so they can't validate current data).

### S2. Table changes
**Portal tables** (names stay; decision 1):
- `license` in Tools syn26127427 and Education syn51497305: STRING becomes STRING_LIST. Split the 4 comma-joined tool values.
- `investigator` in Grants syn21918972 and Projects syn21868602: STRING becomes STRING_LIST.
- Projects: add `projectShortName`.
- `consortium`: unchanged (decision 2).
- Datasets: unchanged (O3).
- B0's rename works against these names; after S2, update the crosswalk to the changed types.

**Manifest tables:** keys are class labels (decision 4). The preferred path is to replace them with RecordSets bound to 14.0.0.
- Grant syn53259587:
  - `Grant Investigator` becomes `Investigator`.
  - `Grant Consortium Name` becomes `ConsortiumKey`, with values mapped to `program.*`.
  - Add `InstitutionKey` and `PersonViewKey`.
  - The remaining spaced columns become class labels.
- Project syn59074382:
  - `Project Investigator` becomes `Investigator`.
  - `Project Consortium Name` becomes `ConsortiumKey`.
  - `Project Grant Number` becomes `GrantViewKey`.
  - Add `PersonViewKey` and `ProjectShortName`.
- PersonView syn38301033:
  - `personConsortiumName` becomes `ConsortiumKey`.
  - `personGrantNumber` becomes `GrantViewKey`.
  - Add `InstitutionKey`.
- Manifest CSVs syn53478776, syn53478774, syn53479671 and syn53651540: regenerate them with class-label keys.

### S3. Bindings
- Bind 14.0.0 to the RecordSets and folders that feed each manifest.
- Portal tables stay unbound (decision 3).

### S4. Live-data audit and curation
- **S4.1 Portal values that fail the new enums** (already audited):
  - Publications `assay`: 60 values.
  - Publications `tissue`: 5 values.
  - Publications `tumorType`: 1 value.
  - Datasets `assay`: 3 values.
  - Tools `license`: 4 comma-joined values.
  - For each, fix the source record or propose adding the term to the model.
- **S4.2 Assay-level annotations** (still open; blocks the 14.0.0 cutover):
  - Scope: Biospecimen, Individual and Model annotations in the MC2 assay file views.
  - What to check:
    - Sex: 9 values become 3.
    - Acquisition Method: 20 become 10.
    - Composition: 21 become 11.
    - Preservation Method: 20 become 14.
    - Preservation Medium: 24 become 17.
    - Tumor Grade: `G1` becomes `G1 Low Grade`.
    - Labels in the fields that now need UBERON or NCIt IDs.
  - Map acquisition methods by the CSV labels, not assumed one-to-one renames. `Core needle biopsy`, `Forceps Biopsy`, `Punch biopsy` and `Shave biopsy` were removed outright, so a curator decides where they go.
- **S4.3 Legacy licenses.** Remap the study-license tokens with DM-3's Nonpreferred Terms. A curator confirms the `CC_BY_NC` version.
- **S4.4 Worklists.** A Haiku SME generates the correction CSVs, a curator reviews them, and they're applied with batch edits in the style of `update_pending_annotations.py`, now on the OOP API.

### S5. Communication
- **Curators:** the 18 renamed template headers, the Visium renames, and DCA's removal.
- **Whoever runs the syncs:** hold until B0.
- **Data-models owners:** the CI gap (O5).

---

## Cross-repo sequencing
1. **Now, in parallel:**
   - B0 hotfix (DCC).
   - [C] P0, curation-pipeline vocabulary hardening.
   - DM-7 post-processing step (O8), in data-models.
   - Optional: file the upstream enum-label request.
2. **data-models:** DM-1 to DM-10 and DM-12. #262 is ready. **Don't merge yet.**
3. **DCC and pipeline:** B1–B5 and [C] P1 on branches, dry-run against a clone of #262. S4.2 is triaged.
4. **data-models:** merge #262, then DM-11 (tag `v14.0.0`).
5. **Synapse registry:** S1 (delete, then register 14.0.0).
6. **Merge:** the DCC PR and [C] P1, **before the next `0 0 1 * *` cron**.
7. **Synapse tables:** S2 and S3, then the first non-dryrun sync (`sync-to-portal.yml`).
8. **Curation:** S4.1, S4.3 and S4.4, then re-sync.
9. **Later:** the CLI plan's Phase 2 moves the aligned scripts; DM-13 CI gate; [C] P2 and P3.

## Delegation
- **data-models:** Sonnet SMEs, following `plans/data_models_cde_alignment.md`.
- **DCC:** B0 is one small Sonnet task, done first, with Opus reviewing it against a scratch table. B1 is one Sonnet task. B2–B5 run as parallel Sonnet tasks, one commit per script group.
- **Synapse writes (S1–S3):** Opus, with explicit go-ahead each time.
- **Before any `gh pr create`:** an independent code review.

## Verification
- **B0:** a non-dryrun write to scratch copies of all 7 portal tables leaves row counts and values identical to `--output_csv`, and a forced column mismatch raises an error **before** truncation.
- **Dry runs:** `sync_*.py --dryrun` against the regenerated manifests shows only the intended type and value changes against pre-change snapshots. `check_crosswalk()` passes on all 7 tables.
- **Registry:** `POST /schema/version/list` shows 14.0.0 as the highest version for every data-model schema. No `10x*` names. Enums are spot-checked for display form (`RNA Sequencing`).
- **Validation:** manifests and RecordSets validate against 14.0.0 through `cckp_metadata`, or jsonschema until that exists. The only errors are known S4 items.
- **data-models:** DM-12 passes, and `make all` plus kg-pipeline `make test` pass from a clean clone.
- **Workflows:** the cron scripts' new `--dryrun` runs cleanly. After S2, `sync-to-portal.yml` runs against a staging copy first. Portal facets for Publications, Datasets, Tools and Grants still render.
