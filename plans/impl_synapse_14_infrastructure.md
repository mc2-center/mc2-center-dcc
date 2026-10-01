# Implementation plan: Synapse and infrastructure changes for data model 14.0.0

**Scope:** everything that changes state on Synapse for the 14.0.0 cutover:
- the `MC2Center` schema registry
- the 532 grant-level manifest tables and their replacement by RecordSets
- the admin manifest tables and merged manifest files
- the portal tables
- schema bindings
- curation of existing values
- the first post-cutover sync

**Master plan:** `plans/cde_model_alignment_harmonized.md`, sections C and S. Code tasks are `D*` in `plans/impl_dcc_14_alignment.md`; pipeline tasks are `P*` in `plans/impl_ai_curation_pipeline_14.md`.

## Execution rules
- **Every step that writes to Synapse is run by Opus, with the owner's explicit go-ahead for that step.** Subagents may only prepare dry-run artifacts and read-only reports.
- **Before any write:**
  1. Snapshot every table it touches (`Table.snapshot(label="pre-14.0.0-<step>")`).
  2. Export it to CSV under `output/pre14/`.
  3. Run the step's `--dryrun` and review the diff.
- **Deletions can't be undone** (I1, the archiving in I5): list exactly what will be deleted and confirm it at execution time.
- **Record outcomes as you go.** Keep a running log in `plans/impl_synapse_14_infrastructure_report.md`: step, entity IDs, snapshot version, counts before and after.

## Current state (read-only checks, 2026-10-01)
| Item | State |
|---|---|
| `MC2Center` registry | 38 schemas; no 14.0.0 versions. Stray versions ≥14 exist. Live data-model versions have **camelCase enums**. |
| Grant-level tables (project_tables view syn52750482) | 532 tables: 151 `publicationview_…`, 141 `grantview_…`, 111 `datasetview_…`, 74 `toolview_…`, 4 each of educationalresource, fileview, study and biospecimen `_synapse_storage_manifest_table`. Checked a sample of 30 publication tables: one identical 13.1.0 display-name schema, mostly STRING. |
| RecordSets | **None** found (`MC2Center_PublicationView_RecordSet` was searched for in all 160 grants' `publications` folders). |
| Admin manifests | Grant syn53259587 (table, display names), Project syn59074382 (table), PersonView syn38301033 (table, camelCase). Merged CSV files: publications syn53478776, datasets syn53478774 (`DatasetView_tagged_Assay_20261001.csv`; DUO short codes `RTN` ×5, `NPUNCU` ×5, the rest `Pending Annotation`), tools syn53479671, education syn53651540. |
| Portal tables | Unbound. Columns as in the master plan. Datasets is 24 columns, and so on. |

## Dependency graph
```
I0 holds ─────────────────────────────────────────────────────────────┐
DM-11 tag ─ I1 delete ─ I2 register 14.0.0 ─┬─ I3 RecordSet spike ─ (decision gate G1)
                                            │        ├─ G1 = RecordSets: I5a migrate → RecordSets
                                            │        └─ G1 = tables:     I5b migrate tables in place
                                            ├─ I7 bindings (after I5)
D2–D7 merged ─ I4 portal type changes ──────┤
I8 value curation (worklists can start now; applied during or after I5)
I5 + I6 + I4 + D* merged ─ I9 first sync + verification ─ I10 comms ─ I11 archive old tables
```

---

### I0. Holds and communication (now)
- **Tell the people who run the syncs and the curation pipeline:**
  - No non-dryrun `sync-to-portal.yml` until I9.
  - The curation pipeline only runs with the `--model-jsonld` pointing at a v13.1.0 copy until pipeline task P0 ships.
  - Pause new curator edits to grant tables of a type while I5 migrates that type.
- **Check:** acknowledged in the curation channel. Link it in the report.

### I1. Registry cleanup (after the `v14.0.0` tag; deletion can't be undone)
- **Delete:**
  - `PublicationView` 15.0.0
  - `Biospecimen` 15.0.0 and 15.0.1
  - `Model` 15.0.0 and 15.0.1
  - `SequencingRNALevel1` 15.0.0 and 15.0.1
  - `AccessRequirementCA123` 17.0.0 and 18.0.0
  - **all of `StudyAR`** (1.0.0–30.0.0, owner decision O9)
- **Keep:** every `AccessRequirementCA261841` version.
- **Before deleting:**
  - For each `StudyAR` and `AccessRequirementCA123` version, check that nothing is bound to it. If any binding exists, stop and report it.
  - Print the final delete list and get confirmation.
- **Tool:** the OOP `JSONSchema` delete calls, following `utils/synapse_json_schema_bind.py`.
- **Check:** `POST /schema/version/list` for each of those schemas shows only what's being kept.

### I2. Register the 14.0.0 schemas
- **Spec:**
  - Check out data-models at `v14.0.0` and run `make register-json VERSION=14.0.0 SCHEMAS=…/json_schemas` (task D14). That registers 38 schemas.
  - The Visium schemas keep their existing names (`VisiumRNALevel1-4`, `VisiumAuxiliaryFiles`).
  - New schemas: PersonView, ProjectView, Consortium, Institution, Theme.
  - Add 14.0.0 rows to the registry table syn69735275, and mark earlier data-model versions `superseded` (their enums are camelCase).
- **Check:**
  - 14.0.0 is the highest version for all 38.
  - Spot-check the PublicationView 14.0.0 enum: `RNA Sequencing` is present and `RNASequencing` absent.
  - Keys are class labels.

### I3. RecordSet spike (scratch project; leads to decision gate G1)
- **Setup:** create a scratch project `MC2 14.0.0 RecordSet spike`, owned by the owner's account. Bind PublicationView 14.0.0 and DatasetView 14.0.0 to RecordSets created with `create_record_based_metadata_task` (as in `utils/create_curation_task.py`). Load 20 rows from a real grant table, converted with `model_contract`.
- **Questions to answer, each with evidence in the report:**
  1. Does `RecordSet.store()` with a new CSV version keep the **schema binding**, the **CurationTask** and an **open Grid session**? What happens to Grid edits made after the session opened?
  2. Does validation re-run on each new version? How long until `validation_summary` and `get_detailed_validation_results` are populated?
  3. Is a primary-key upsert (download, merge, store) idempotent? Run it twice and confirm there are no duplicate rows.
  4. Can a MaterializedView or SQL query reference a RecordSet directly? This is expected to be **no**; confirm it.
  5. How long does it take, and what are the size limits, for a 1,200-row DatasetView RecordSet (the current merged size)?
  6. Do list-valued fields (`Assay`) round-trip in the CSV form that curator and Grid expect?
- **Gate G1.** If 1–3 all pass, **G1 = RecordSets** (I5a). Otherwise, **G1 = tables** (I5b), and RecordSets stay limited to curator Grid tasks.
- **Cleanup:** delete the scratch project after the report, with go-ahead.

### I4. Portal table changes (with or right after the D2–D7 merge)
- **Changes:**
  - `license` in Tools syn26127427 and Education syn51497305: STRING becomes STRING_LIST. Re-store the 4 comma-joined tool values as lists.
  - `investigator` in Grants syn21918972 and Projects syn21868602: STRING becomes STRING_LIST.
  - Add `projectShortName` (STRING) to Projects.
  - No name changes (decision 1). Datasets and the `consortium` columns are unchanged.
- **Order:** D2's `to_portal` adapts to the live types, so code and schema changes don't have to happen at the same moment. Re-run `check_crosswalk` after each change.
- **Check:** `check_crosswalk` passes on all 7 tables. Portal facets render for Tools, Grants and Projects.

### I5a. Grant-level migration to RecordSets (G1 = RecordSets)
- **Order by type:** PublicationView, DatasetView, ToolView, EducationalResource, then GrantView. Then the assay-level types (FileView, Study, Biospecimen), coordinated with the I8.2 audit.
- **For each grant and type:**
  1. Export the `<type>_synapse_storage_manifest_table`.
  2. Rename the headers with `model_contract.RENAMES_13_TO_14`.
  3. Apply the I8 value mappings (licenses → SPDX, DUO short codes → IDs, consortium names → `program.*`).
  4. Normalize to class-label keys.
  5. Create `MC2Center_<Type>_RecordSet` in the grant's data-type folder, bound to `<Type>` 14.0.0, using `create_record_based_metadata_task`.
  6. Store the rows.
  7. Collect the validation results.
- **Script:** `annotations/migrate_grant_tables_14.py`, written as a D-task follow-up. It's dry-run by default, and its output is a per-grant diff plus the expected validation failures.
- **Run it** one type at a time. After each type, run D12's RecordSet-mode union and compare its row count and IDs with the legacy MaterializedView union for that type. They must be equal.
- **Admin tables:**
  - Grant syn53259587, Project syn59074382 and PersonView syn38301033 become RecordSets the same way. Project gets `PersonView Key` and `Project Short Name`. PersonView moves from camelCase to the template headers.
  - Point `portal_tables/utils.py` `CONFIG` at them. This needs D16, because the sync scripts read these with SQL today.
- **Check:** for every type, union row count and primary keys equal the legacy union. The only validation failures are known I8 items.

### I5b. Grant-level migration in place (G1 = tables)
- **What:** rename the columns of all 532 grant tables, and the admin tables, to the 14.0.0 display names. Use `annotations/schema_update.py` (the OOP table-schema transaction) with `RENAMES_13_TO_14`, add the new columns, and apply the I8 value mappings.
- **One fixed schema per type.** Every grant table of a type must end with **identical** columns and types; that's what the MaterializedView UNION needs. Derive the column types from the 14.0.0 JSON schema, not `INFER_FROM_DATA`.
- **Rebuild** each type's MaterializedView with `merge_tables.py` legacy mode.
- **Check:** the MV rebuilds for each type, and its row count equals the pre-migration count.

### I6. Merged manifests consumed by the sync scripts
- **What:** regenerate the merged manifest files (syn53478776, syn53478774, syn53479671, syn53651540) from the I5 union, through `union_qc.py`, with the 14.0.0 template headers.
- **Validation in `union_qc.py`:** with G1 = RecordSets, use Synapse's RecordSet validation results. With tables, use jsonschema against the 14.0.0 schemas. Remove the `schematic` subprocess (D-task follow-up, coordinated with the CLI plan's `cckp_metadata` validator).
- **`qc_attribute_mapping.csv`:** wait for the data-models DM-14 fix (`Data Use Codes` → `Dataset Data Use Codes`).
- **Optional:** publish one browsable union table per type (`merge_tables.py --publish`).
- **Check:** each sync script's `--dryrun` against the new files differs from the pre-change portal snapshot only in the intended ways.

### I7. Bindings
- **What:** bind 14.0.0 to each RecordSet (done in I5a) or each grant data-type folder (I5b), and to the admin manifests.
- **Portal tables stay unbound** (decision 3).
- **Check:** `get_schema` on a sample of 10 grants returns the 14.0.0 `$id`.

### I8. Value curation (worklists can start now; applied during I5)
- **I8.1 Portal enum failures** (audited 2026-09-30):
  - Publications: 60 `assay`, 5 `tissue`, 1 `tumorType`.
  - Datasets: 3 `assay`.
  - For each value, fix the source record or file a model request.
- **I8.2 Assay-level annotations** (blocks the assay-level part of I5):
  - Sex: 9 values become 3.
  - Acquisition Method: 20 become 10; a curator decides where the removed methods go.
  - Composition: 21 become 11.
  - Preservation Method: 20 become 14.
  - Preservation Medium: 24 become 17.
  - Tumor Grade: `G1` becomes `G1 Low Grade`.
  - Labels in the fields that now need UBERON or NCIt IDs.
- **I8.3 Licenses:** legacy study-license tokens become SPDX, using the Nonpreferred Terms in `shared/license.csv`.
- **I8.4 DUO short codes become IDs,** via the `duo.csv` `Properties` column. `NPUNCU` becomes two IDs (NPU and NCU). Apply this to the manifests and to any DUO annotations on Dataset entities.
- **I8.5 Consortium names become `program.*` IDs** in the GrantView, PersonView and Project sources.
- **Method:** a Haiku SME generates correction CSVs, a curator reviews them, and they're applied as part of the I5 transform. Annotation edits on entities use OOP batch edits in the style of `update_pending_annotations.py`.

### I9. First post-cutover sync and verification
- **Order:** run the sync scripts **non-dryrun one at a time**, in the `sync-to-portal.yml` order (publications, datasets, tools), then grants, projects, people and education.
- **Each run:**
  1. Snapshot (`update_table` already does this).
  2. Run.
  3. Compare the row count and a 20-row sample with `--output_csv`.
  4. Smoke-test the portal facets.
- **First:** the B0 scratch-table write test (D-plan carry-over), before the first production table.
- **Check:** all 7 tables are populated with the expected row counts, and the facets render.

### I10. Communication
- **Curators:**
  - the 14.0.0 templates (display-name headers)
  - RecordSets and Grid (if G1 = RecordSets)
  - the Visium renames
  - DUO values as IDs
  - the DCA removal
- **People who run the syncs and the pipeline:** the I0 holds are lifted.

### I11. Archive the legacy grant tables (after one full sync cycle with no regressions)
- **G1 = RecordSets:** move the 532 `*_synapse_storage_manifest_table`s to an `archive_13.1.0` folder per grant, or delete them. **Deletion needs explicit go-ahead.** Also delete the old per-type MaterializedViews.
- **Update scopes:** the project_tables view syn52750482 and the "All Files V2" view syn27210848 need their scopes or filters updated.

## Done when
- I1–I9 and I11 are complete.
- The report lists every entity ID touched, its snapshot version, and counts before and after.
- `sync-to-portal.yml` runs unattended and correctly.
- The cron jobs (2026-11-01) succeed.
