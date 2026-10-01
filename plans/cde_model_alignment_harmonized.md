# Master plan: land the CDE model revisions (data-models 14.0.0) across data-models, mc2-center-dcc, CCKP Synapse and the AI curation pipeline

**Status as of 2026-10-01:**
- **PR #262 is merged** into `data-models/main` (`c234c467`, labelled `major`). It includes all the DM-1–DM-10 and DM-12 work: `plans/data_models_cde_alignment_report.md` in data-models, with the checks re-run against `main`. **No `14.0.0` tag or release exists yet**; the latest is `v13.1.0`.
- **Synapse:** the `MC2Center` registry is unchanged. Nothing is deleted and nothing is registered at 14.0.0.
- **mc2-center-dcc:** only B0 (`3b35c2b`) has landed.
- **ai-curation-pipeline:** P0 has **not** landed (`main` is still `fa7b977`). Its vocabulary loader now reads 0 terms (see B9).

**Sources:**
- **[U]** `data-models/plans/cde_model_revisions_integration.md`
- **[D]** `plans/model_alignment.md`
- **[C]** `plans/ai_curation_pipeline_alignment.md`
- the data-models implementation report and `release_14.0.0_notes.md` on `main`
- the owner's review notes on earlier revisions (decision log)
- the merged synapseclient OOP migration and CLI consolidation plans

**Implementation plans (executable specs, one per area):**
- `plans/impl_dcc_14_alignment.md`: mc2-center-dcc code (D1–D15).
- `plans/impl_synapse_14_infrastructure.md`: Synapse and infrastructure (I0–I11), including the grant-level manifest layer.
- `plans/impl_ai_curation_pipeline_14.md`: ai-curation-pipeline (P0–P4).
- data-models has `plans/data_models_cde_alignment.md` (DM items) plus GitHub issues #273 (DM-11 release), #274 (DM-13 CI) and #275 (DM-15).

This master plan keeps the decisions, the reasoning, and the order across repos. **Where they disagree, the implementation plans win on detail.**

**Structure:** section A covers data-models (work plan: `plans/data_models_cde_alignment.md`), section B the mc2-center-dcc code, and section C Synapse tables and operations. The cross-repo order and the checks follow.

## Context
Release 14.0.0 makes these changes:
- It consolidates about 70 per-entity attributes into shared ones.
- It moves sample and assay fields to NCIt or UBERON reference validation.
- It adds the foreign keys `Consortium Key` (whose values are `program.*` IDs), `Institution Key` and `PersonView Key`.
- It generates JSON schemas with `synapseclient.extensions.curator`: property keys are class labels, and a post-processing step puts enum values back into display form.

Target state:
- The schemas are registered in Synapse (org `MC2Center`, version **14.0.0**) as the source of truth.
- The DCC sync pipeline and the AI curation pipeline read the 14.0.0 attribute names.
- The portal tables hold values that are valid against the schemas, with no change to the portal app config.

## What changed on `data-models/main` since the last revision of this plan
These came in with #262 on top of what the earlier plans assumed. They're folded into the sections below.

| Change on `main` | Effect downstream |
|---|---|
| **Dataset View uses `Dataset Data Use Codes`** (key `DatasetDataUseCodes`), not the shared `Data Use Codes`. Values are checked against the pattern `^(DUO:\d{7}\|DUOPlus\d+\|Pending Annotation)$`, not a closed list. The conditional access-condition (DUO) fields were removed from DatasetView, which now has 16 keys. | **The live dataset manifest (syn53478774, refreshed today) stores DUO short codes**: `RTN`, `NPUNCU` and `Pending Annotation`. Those **fail** the new pattern. `sync_datasets.py`'s `DUO_DICT` is keyed by those same short codes, so once manifests carry `DUO:` IDs every lookup raises `KeyError`, and that row's repository and download fields are skipped (B3, S4.5). |
| **`templates/*.csv` are regenerated from the model** (`make templates`), with **display-name headers** (`Dataset Data Use Codes`, `GrantView Key`, `DatasetView_id`). Schema keys are class labels (`DatasetDataUseCodes`, `GrantViewKey`, `DatasetViewId`). | Two header forms coexist on purpose: templates and manifests use display names, schemas use class labels. Decision 5 sets the contract between them. |
| **DataCatalog:** `species` becomes `Species`, `grantNumber` becomes `GrantView Key`, `studyId` becomes `dataCatalogStudyId`, `contributor` becomes `dataCatalogContributor`. DataCatalog now requires `Species`. | Not used by the DCC sync. kg-pipeline's `extract_datacatalog.py` already maps them. DataCatalog.json can't validate the live camelCase Dataset annotations; this was already true. |
| **`GrantView Key` pattern** is `(CA\d{6}\|Affiliated/Non-Grant Associated)`. It's unanchored, because the cell holds a comma-separated list. | JSON Schema patterns are unanchored anyway, so any string *containing* one valid grant number passes. That's acceptable for now; see DM-15. |
| **Visium schemas** are now `VisiumAuxiliaryFiles` and `VisiumRNALevel1-4`, with no `10x`. | B5: remove the `10x_` stripping in `csv_to_ttl.py`. S1.2 registers them under the existing names. |
| **`qc_model/qc_attribute_mapping.csv` is kept** (`union_qc.py` uses it). **Its DatasetView row still says `Data Use Codes`**, but the template header is now `Dataset Data Use Codes`. | `union_qc.py` won't aggregate that column. Fix it upstream (DM-14). |
| **`scripts/check_json_schemas.py`** (DM-12 checks, 0 failures across 38 schemas) and **`scripts/check_template_list.py`** exist, with tests in `tests/`. | They can run in CI (DM-13), and DCC can reuse their logic for its own checks. |

## Decision log
| # | Decision | Status |
|---|---|---|
| 1 | Portal tables keep their camelCase columns. A versioned crosswalk maps schema keys to portal columns. | Decided |
| 2 | Portal consortium columns keep display names, resolved from `Consortium Key` through the Consortium table. | Decided |
| 3 | Validation happens upstream (RecordSets and curation tasks bound to schemas). Portal tables are derived from validated records and aren't bound. | Decided |
| 4 | Schema property keys are class labels, because curator doesn't allow spaces in keys (O7). Enums are in display form via post-processing (O8). | Decided, **done in data-models** |
| 5 | **Header contract:** **template display names are the manifest form**. That's what curators fill in, what `templates/*.csv` ships, and what the AI curation pipeline should output. **Class labels are the schema and RecordSet form.** Convert between them **in one place**, using the display name ↔ label pairs in `mc2.model.jsonld`, not a reimplemented camel-case rule. This replaces the earlier instruction that the pipeline outputs class labels. | **Decided** (owner, 2026-10-01) |
| O1 | The "no grant" sentinel is allowed explicitly. | Done (DM-4) |
| O2 | `ImagingChannel` is in `DATA`, and the template flags match. `Collection` is excluded on purpose. | Done (DM-5) |
| O3 | The portal doesn't get the 15 DUO fields. Access info comes from DUO codes. (On `main` those fields are also gone from DatasetView.) | Decided → B3 |
| O4 | Reference validation affects only sample, assay and File templates. | Verified |
| O5 | CI gate in data-models. | Deferred → DM-13 |
| O6 | Legacy license tokens are mapped to SPDX. `CC_BY_NC` becomes `CC-BY-NC-4.0` (owner-confirmed). | Done (DM-3); data remap → S4.3 |
| O7 / O8 | Keys are class labels; enum display form comes from post-processing. | Done (DM-7) |
| O9 | Delete the stray data-model versions ≥14.0.0, `AccessRequirementCA123` 17.0.0–18.0.0, and **all of `StudyAR`** (every version, including 1.0.0–3.0.0). Keep every `AccessRequirementCA261841` version. | Decided (owner, 2026-10-01) → S1.1 |
| O10 | **DUO value form.** The schema requires `DUO:` IDs (`DUO:0000042`), but the live manifests hold short codes (`RTN`, `NPUNCU`). Recommendation: **the data stores IDs, and the portal keeps showing the existing access-information text**. `sync_datasets.py` translates each ID to its short code using `modules/shared/duo.csv`, where the `Properties` column holds `GRU`, `PS` and so on, then uses `DUO_DICT`. | **Decided** (owner, 2026-10-01) |
| 0.2 / 0.3 / 0.6 / 1.2 / 1.4 | `license` merged into `License`; `dataCatalogLicense` on SPDX; DCA removed; no `10x` names; ignore `curator_tools/`. | Done or decided |

---

## Section A: data-models (work plan: `plans/data_models_cde_alignment.md`)
| Item | Status on `main` |
|---|---|
| DM-1 to DM-10, DM-12 | **Done** in #262 (`c234c467`). DM-12 re-checked here: `scripts/check_json_schemas.py` finds 0 failures across 38 schemas. Spot checks: `Species` is in DatasetView's `required`; enums use the display form (`10-cell RNA Sequencing`); no key contains a space or starts with a digit. |
| **DM-11 Release** | **Open.** The notes are drafted (`plans/release_14.0.0_notes.md`). Tag `v14.0.0` and publish the release; needs the owner's go-ahead. Everything in sections B and C that pins to a model version waits on this tag. |
| DM-13 CI gate | Open, deferred. Today `.github/workflows` only checks PR labels, builds docs and runs the release and sheet-sync jobs. |
| **DM-14 (new)** | Fix the DatasetView row in `qc_model/qc_attribute_mapping.csv`: `Data Use Codes` becomes `Dataset Data Use Codes`. This is a patch PR. |
| **DM-15 (new, low priority)** | Consider making `GrantView Key` a `string_list` with an anchored per-item pattern, instead of an unanchored pattern on one comma-joined string. That would change the key type, so it belongs in a later minor or major release. |

---

## Section B: mc2-center-dcc code
### B0. `update_table` column-name alignment: **done** (`3b35c2b`)
- **What changed.** `Table.store_rows(df)` matches columns by name. The fix relabels the DataFrame's columns by position to the live column names, and stops before the snapshot or truncate if there are too many columns.
- **Verified:** mocked writes for publications, tools, education, projects, people and grants.
- **Still to check:** one real write to a scratch copy of a portal table. This needs go-ahead to create and delete the scratch table.

### B1. Header normalization, crosswalk and consortium lookup
- **Header normalization (decision 5).** One function, `normalize_headers(df, component)`, maps template display names to class labels. It uses the display name ↔ label pairs from `mc2.model.jsonld`, pinned to `v14.0.0`.
  - It replaces the ad hoc `manifest.columns.str.replace(" ", "")` in `sync_datasets.py:263`, `sync_education.py:115` and `sync_people.py:84`. That rule happens to work for most headers but gives `DatasetView_id`, where the label is `DatasetViewId`.
  - It moves to `cckp_metadata` when the CLI plan's Phase 1 lands.
- **Crosswalk.** `portal_tables/crosswalk.py` gives, per component, an ordered class-label → portal-column map, stamped `model_version="14.0.0"`.
  - It replaces the scattered rename maps and `annotations/attribute_dictionary.py`.
  - `check_crosswalk(table_id, component)` checks the live column order. B0's positional rename then becomes crosswalk-driven.
- **Consortium lookup.** `consortium_display_names(syn, ids)` goes in `portal_tables/utils.py`. It fails loudly on unknown IDs.

### B2. Sync scripts (input in class labels, after B1 normalization)
| Script | Changes |
|---|---|
| `sync_publications.py` | `PublicationAssay`, `PublicationTumorType` and `PublicationTissue` become `Assay`, `TumorType`, `Tissue`. Replace positional `row[4]` with `row["GrantViewKey"]`. Today this script reads display names directly; switch it to B1 normalization. |
| `sync_datasets.py` | `DatasetAssay`, `DatasetSpecies`, `DatasetTissue` and `DatasetTumorType` become `Assay`, `Species`, `Tissue`, `TumorType`. **`DataUseCodes` becomes `DatasetDataUseCodes`** (new on `main`). `DatasetView_id` becomes `DatasetViewId`. DUO handling: B3. |
| `sync_tools.py` / `sync_education.py` | `ToolLicense` / `ResourceLicense` become `License`, as a string list. |
| `sync_grants.py`, `create_grant_projects.py` | `Grant Investigator` becomes `Investigator`, as a list. `Grant Consortium Name` becomes `ConsortiumKey`, resolved via B1. Recheck the `updated_scope` handling on the OOP `EntityView` path. |
| `sync_projects.py` | `ProjectInvestigator` becomes `Investigator`. `ProjectGrantNumber` becomes `GrantViewKey`. Consortia come via B1. Add `ProjectShortName`, going to `projectShortName`. |
| `sync_people.py` | `personConsortiumName` becomes `ConsortiumKey`. `personGrantNumber` becomes `GrantViewKey`. Add `InstitutionKey`. |

### B3. Dataset access info: DUO IDs in, existing portal text out (O3, O10)
- **Translate IDs.** Build an ID → short-code map from `modules/shared/duo.csv`, pinned to `v14.0.0`, which records each short code in its `Properties` column (`DUO:0000042` → `GRU`). Then use the existing `DUO_DICT` text.
- **Accept both forms during the transition.** IDs and short codes are both accepted until S4.5 finishes; short codes pass straight through.
- **Fix the skip bug.** Replace the `except KeyError: continue` (`sync_datasets.py:131-134`) with a logged warning that keeps processing the row. Today an unknown code silently skips `sourceRepository`, `downloadType` and `downloadSynId`.
- **`DUOPlus` codes**, which the new pattern allows, need `DUO_DICT` entries or a defined fallback text.

### B4. Annotation and utility scripts
- **Scripts:** `processing-splits.py`, `add_cols.py` (possibly dead per the CLI plan; confirm first), `schema_update.py`, `edit_legacy_annotations.py`, `split_manifest_grants.py`, `merge_and_correct_manifests.py`, `table_to_annotations.py`, `check_publications_status.py`.
- **Change:** rename through B1.
- **Pin** the `all_valid_values.csv` URL to `v14.0.0`. Its format is unchanged on `main` (`category,valid_value,nonpreferred_values`).
- **`processing-splits.py`** only splits by component once the pipeline emits template headers ([C] P1).
- **Existing bugs to fix:**
  - `table_to_annotations.py:525` `keys_to_drop=None`.
  - `pubMedUrl` should be `pubMedLink`.
  - Check File-level writes against the new `File Assay Category` field.

### B5. `csv_to_ttl.py`
- Remove the `10x_` stripping (lines 258-259 and 476); DM-6 is done on `main`.
- Check that it still parses the revised `Properties` and `DependsOn`.
- Fix the `is_enum` inconsistency (lines 196 and 199).

### B6. Schema tooling
- **Registration:** a tracked `register-json VERSION=14.0.0` entry point around the OOP `utils/synapse_json_schema_bind.py`, pointing at `data-models/json_schemas` at the `v14.0.0` tag. It replaces the untracked `local_inputs/Makefile`, which hardcodes 13.0.0.
- **Validation:** moves to the CLI plan's `cckp_metadata` jsonschema validator, which can reuse data-models' `scripts/check_json_schemas.py` logic.
- **`union_qc.py`** keeps `qc_model/qc_attribute_mapping.csv` once DM-14 is fixed.
- Ignore `curator_tools/`.

### B7. Workflows
- **Cron deadline.** `update-theme-graphs.yml` and `publications-status-check.yml` run on `0 0 1 * *`. The **first cron after #262's merge is 2026-11-01**. B4's `check_publications_status.py` fix must merge before then.
- **Dry-run flags.** Add `--dryrun` to `tally_themes.py` and `check_publications_status.py`. Neither has been run live since the OOP migration.

### B8. Order relative to the CLI consolidation plan
- Model alignment lands in the current scripts first.
- `mc2dcc sync …` then moves the aligned code over.
- `normalize_headers` and the crosswalk move to `cckp_metadata` and `mc2dcc/sync/_shared.py` respectively.

### B10. The curation hand-off chain (from AI pipeline output to grant-level uploads)
- **The pipeline writes one file per type covering all grants:**
  - `publication_metadata.csv`: crawler-shaped 13.1.0 columns plus `keywords` and the `_…` bookkeeping columns.
  - `datasets_publication_metadata.csv` and `tools_<stem>.xlsx`: already in 13.1.0 template shape.
- **Then `annotations/` runs:**
  1. `split_manifest_grants.py`: one file per grant. It fetches `all_valid_values.csv` from `main` without a pinned version, and reads only CSV.
  2. `gen-mp-csv.py`: works out each file's target grant folder.
  3. `processing-splits.py`: reshapes to a **hardcoded 13.1.0** column order with `df[col_order]`, which silently drops non-template columns; also applies the 500 → 400-character cap.
  4. `schema_update.py`: changes grant-table column types.
  5. `create_entity_links.py`: creates link or Dataset entities and fills the ID columns.
  6. `upload-manifests.py`: `schematic submit -mrt table_and_file -tcn display_name` into the grant's data-type folder and its `*_synapse_storage_manifest_table`.
- **What 14.0.0 breaks.** `processing-splits.py` raises `KeyError` on 14.0.0-shaped input. So the pipeline's template-exact output (P1) and the template-driven `processing-splits.py` (D8) ship together.
- **`upload-workflow.sh` is already broken**, separate from 14.0.0: it passes a folder where a CSV is expected, and hardcodes past file names and a personal config path. Replacing it is D15.

### B11. Grant-level layer, RecordSets and `merge_tables.py`
- **Today.** There are 532 grant-level tables, all with identical 13.1.0 display-name schemas. That's what lets `merge_tables.py` build its `SELECT * UNION` MaterializedView per type. These tables have to move to 14.0.0 all at once for each type (infrastructure plan I5).
- **RecordSets are the target (agreed), with caveats:**
  - Synapse can't query them; no SQL or MaterializedView over a RecordSet. So unions are built in Python from the RecordSet CSVs (D12).
  - Nothing uploads into them today; that's D11.
  - Behavior when a new CSV version is stored (binding, curation task, Grid) is unverified; the infrastructure plan's I3 spike settles it, and gate G1 chooses between RecordSets and migrating the tables in place.
- **Existing bugs in `merge_tables.py`'s RecordSet path** (fixed in D12):
  - `SELECT 'grantId'` is a string literal, so it returns the text `grantId` for every row.
  - Missing folders or RecordSets crash it.
  - The per-grant mirror tables get a duplicate copy appended every run.
  - `INFER_FROM_DATA` infers different column types per grant, which breaks the UNION.

### B9. AI curation pipeline (companion plan [C]): **now urgent**
- **What's broken.** `ai-curation-pipeline` reads `data-models/main/mc2.model.jsonld` without a pinned version, asking for `Publication Assay`, `Publication Tissue` and `Publication Tumor Type`. Against the current `main` that returns **0** terms; `Assay`, `Tissue` and `Tumor Type` return 391, 114 and 186. **Every pipeline run since #262 merged produces empty or near-empty annotations, with no error.**
- **P0 is now an incident fix:** the field-name fallback, a hard failure on an empty vocabulary, and a pinned model ref.
- **Until P0 ships,** run the pipeline only with `--model-jsonld` pointing at a `v13.1.0` copy of `mc2.model.jsonld`.
- **P1** is revised by decision 5: output headers equal `templates/*.csv` exactly.

---

## Section C: Synapse tables and operations (Opus runs these with explicit approval; snapshot first)
### S0. Holds
- **No non-dryrun syncs** until B1–B3 merge. B0 makes the write safe, but the 14.0.0 manifests won't map correctly yet.
- **Don't run the AI curation pipeline** without the v13.1.0 `--model-jsonld` workaround (B9).

### S1. Schema registry (`MC2Center`), after the `v14.0.0` tag
- **S1.1 Delete:**
  - `PublicationView` 15.0.0
  - `Biospecimen` 15.0.0 and 15.0.1
  - `Model` 15.0.0 and 15.0.1
  - `SequencingRNALevel1` 15.0.0 and 15.0.1
  - **All of `StudyAR`**, every version (O9)
  - `AccessRequirementCA123` 17.0.0–18.0.0
  - **Keep** every `AccessRequirementCA261841` version. Deletion can't be undone, so it needs explicit go-ahead.
- **S1.2 Register** all 38 `json_schemas/*.json` from `v14.0.0` at **14.0.0**, via B6:
  - The Visium schemas use the existing `VisiumRNALevel*` and `VisiumAuxiliaryFiles` names.
  - New: PersonView, ProjectView, Consortium, Institution and Theme.
  - `ImagingChannel` gets its 14.0.0 version.
- **S1.3 Registry table.** Add 14.0.0 rows to syn69735275, and mark earlier data-model versions superseded (their enums are camelCase).

### S2. Table changes
**Portal tables** (names stay):
- `license` in Tools syn26127427 and Education syn51497305: STRING becomes STRING_LIST. Split the 4 comma-joined tool values.
- `investigator` in Grants syn21918972 and Projects syn21868602: STRING becomes STRING_LIST.
- Projects: add `projectShortName`.
- `consortium` and the Datasets columns: unchanged.

**Manifest tables:** move them to RecordSets bound to 14.0.0 (preferred), or rename their columns to the 14.0.0 **template headers** (decision 5).
- **Grant syn53259587:**
  - `Grant Investigator` becomes `Investigator`.
  - `Grant Consortium Name` becomes `Consortium Key`, with values mapped to `program.*`.
  - Add `Institution Key` and `PersonView Key`.
- **Project syn59074382:**
  - `Project Investigator` becomes `Investigator`.
  - `Project Consortium Name` becomes `Consortium Key`.
  - `Project Grant Number` becomes `GrantView Key`.
  - Add `PersonView Key` and `Project Short Name`.
- **PersonView syn38301033:** move to the PersonView template headers (`Consortium Key`, `GrantView Key`, `Institution Key`, …). Its columns are camelCase today.
- **Dataset, publication, tool and education manifest CSVs:** syn53478774 (now `DatasetView_tagged_Assay_20261001.csv`), syn53478776, syn53479671 and syn53651540. Regenerate them with the 14.0.0 template headers. For datasets, `Data Use Codes` becomes `Dataset Data Use Codes`.

### S2b. Grant-level manifest layer (see infrastructure plan I3, I5, I11)
- **Spike first.** The RecordSet spike (I3) decides gate G1.
- **G1 = RecordSets:** migrate each grant table to a bound `MC2Center_<Type>_RecordSet`, type by type, with the I8 value mappings applied (I5a). Then archive the old tables (I11).
- **G1 = tables:** rename all 532 grant tables' columns in place with `schema_update.py`, one fixed schema per type, and rebuild the MaterializedViews (I5b).
- **Either way,** the admin manifests (Grant, Project, PersonView) and the merged manifest files that the sync scripts read are regenerated with 14.0.0 headers (I6).

### S3. Bindings
- Bind 14.0.0 to the RecordSets and folders that feed each manifest.
- Portal tables stay unbound.

### S4. Live-data audit and curation
- **S4.1 Portal values that fail the new enums** (audited 2026-09-30):
  - Publications: 60 `assay`, 5 `tissue` and 1 `tumorType` value.
  - Datasets: 3 `assay` values.
  - Tools: 4 comma-joined `license` values.
- **S4.2 Assay-level annotations** (open; blocks the cutover):
  - Sex: 9 values become 3.
  - Acquisition Method: 20 become 10; map by the CSV labels, with a curator deciding where removed methods go.
  - Composition: 21 become 11.
  - Preservation Method: 20 become 14.
  - Preservation Medium: 24 become 17.
  - Tumor Grade: `G1` becomes `G1 Low Grade`.
  - Labels in the fields that now need UBERON or NCIt IDs.
- **S4.3 Legacy licenses:** remap using the Nonpreferred Terms now in `shared/license.csv`.
- **S4.4 Worklists:** a Haiku SME generates the correction CSVs, a curator reviews them, and they're applied with OOP batch edits.
- **S4.5 DUO short codes become IDs (new, O10).** The dataset manifest's 5 `RTN` and 5 `NPUNCU` values (the rest are `Pending Annotation`) map to IDs via `duo.csv` `Properties`.
  - `NPUNCU` is a **combination** of `NPU` and `NCU`, so it becomes two IDs.
  - Do the same for any DUO annotations on Dataset entities.

### S5. Communication
- **Curators:** the regenerated 14.0.0 templates (display-name headers), the Visium renames, the DCA removal, and DUO values as `DUO:` IDs.
- **People who run the syncs and the pipeline:** the S0 holds.
- **Data-models owners:** DM-13 and DM-14.

---

## Cross-repo order
1. **Now:**
   - [C] P0 incident fix.
   - DM-14 patch.
2. **Tag `v14.0.0`** (DM-11), with the owner's go-ahead.
3. **S1** registry: delete, then register 14.0.0.
4. **DCC:** B1–B5 and B7, plus [C] P1, on branches. Dry-run them against the `v14.0.0` manifests and the scratch-table write. Both merge **before 2026-11-01**.
5. **Synapse tables:** S2 and S3, then the first non-dryrun sync.
6. **Curation:** S4 (S4.2 must be triaged before step 5 counts as complete), then re-sync.
7. **Later:** CLI consolidation Phase 2 ports, DM-13, DM-15, and [C] P2 and P3.

## Delegation
- **[C] P0:** Sonnet, immediately, in the pipeline repo; Opus reviews it.
- **DCC:** B1 is one Sonnet task. B2–B5 and B7 run as parallel Sonnet tasks, one commit per script group.
- **DM-14:** a one-line Sonnet or Haiku patch in data-models.
- **S1–S3 Synapse writes, the tag, and PRs:** Opus, with explicit go-ahead each time. An independent code review runs before any `gh pr create`.

## Verification
- **B0:** a scratch-table write leaves rows and values identical to `--output_csv`.
- **B1:** `normalize_headers` turns every `templates/*.csv` header at `v14.0.0` into exactly that template's schema keys. Test this against all 33 templates.
- **B2 and B3:** dry runs differ from the pre-change snapshots only as intended. A row with an unknown DUO code still gets `sourceRepository` and `downloadType`. `check_crosswalk()` passes on all 7 tables.
- **Registry:** 14.0.0 is the highest version for all 38 schemas. Enums are in display form. There are no `10x` names.
- **Validation:** the regenerated manifests validate against 14.0.0. The only failures are the known S4 items.
- **Pipeline:** P0's empty-vocabulary test fails the run. With the `v14.0.0` JSON-LD it loads 391, 114 and 186 terms.
- **Workflows:** the cron scripts' `--dryrun` runs cleanly before 2026-11-01. The portal facets still render after S2.
