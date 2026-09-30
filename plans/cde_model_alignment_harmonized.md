# Harmonized plan: land the CDE model revisions (data-models PR #262) across data-models, mc2-center-dcc, and CCKP Synapse

This combines two plans:
- **[U]**: `mc2-center/data-models` → `plans/cde_model_revisions_integration.md`, the upstream integration plan written from the data-models side.
- **[D]**: `mc2-center-dcc` → `plans/model_alignment.md` (commit `d4659ec`), the downstream alignment plan written from the DCC side. It includes live reads of Synapse.

Each item below is tagged with where it came from ([U], [D] or [U+D]) and which repo or system it touches:
- **[DM]**: data-models
- **[DCC]**: this repo
- **[SYN]**: Synapse

It also carries forward what each source plan left open, and resolves the items the other plan's evidence settles.

## Context

PR #262 (`cde-model-revisions`, labelled `major`) makes three kinds of change [U]:
1. **Consolidation.** About 70 attributes with per-entity prefixes are collapsed into shared attributes: `Assay`, `Species`, `Tissue`, `Tumor Type`, `License`, `Investigator`, `Sex`, `Data Use Codes`, `grantNumber`, and others.
2. **From closed lists to references.** Some closed lists are replaced by NCIt/UBERON reference validation or a `^UBERON:\d+$` pattern: therapeutic agent, primary site and diagnosis, metastasis sites, anatomic sites, and event type.
3. **Curator migration.** Model generation moves from `schematicpy` to `synapseclient.extensions.curator`.

It also changes key structure:
- **Consortium becomes an ID.** `consortium_name.csv` is replaced by `consortium_id.csv`, whose values are `program.*` IDs. The `Consortium Key` foreign key is added to Grant, Person and Project.
- **New foreign keys:** `Institution Key`, `PersonView Key` and `Project Short Name` are added.

Target end state [D]:
- All schemas are registered in Synapse (org `MC2Center`) as the source of truth.
- The DCC sync pipeline reads the new attribute keys.
- The CCKP portal tables hold values that are valid against the schemas, with no change to the portal app config.

Evidence that this breaks consumers:
- Upstream: `kg-pipeline/crosswalk_scdm.py` broke when `consortium_name.csv` was deleted (fixed in `191c8f1`) [U].
- DCC: 9+ scripts and 3 manifest tables still use the old names [U+D].

### Decisions already made
1. **Portal columns.** Portal table columns keep their camelCase names as a presentation layer. One versioned crosswalk maps schema keys to portal columns [D].
2. **Consortium values.** Portal consortium columns keep display names (`CSBC`), resolved from `Consortium Key` through the Consortium table [D].
3. **Validation happens upstream.** Synapse JSON-schema binding validates entity annotations, not table rows. So validation happens upstream (RecordSets or curation tasks bound to schemas), and portal tables are derived from validated records [D].

### Open decisions (need an owner)
| # | Decision | From |
|---|---|---|
| O1 | "No grant" sentinel: `Affiliated/Non-Grant Associated` fails `^CA\d{6}$`, and `sync_projects.py`, `sync_people.py` and `gen-mp-csv.py` use it. Either allow it explicitly or model it as an empty `GrantView Key`. | D |
| O2 | `ImagingChannel.json`: add it to the Makefile `DATA` list (the class is still `IsTemplate: True`), or intentionally leave it out and deprecate the registered `MC2Center/ImagingChannel`. | U + D |
| O3 | Whether the portal shows the 15 new DatasetView DUO fields plus `License` (new portal columns). | D |
| O4 | Sign-off from the curation QA owner on relaxing closed lists to reference validation. | U |
| O5 | Whether to restore a CI gate for `make all` and `kg-pipeline` in data-models. | U |
| O6 | Legacy study-license tokens (`CC_BY`, `Apache_2`, `GPL_3` and 7 others) are dropped from the merged `License`. Map them to SPDX equivalents, or keep them as legacy values. | D (resolves U A.3) |

## Where the two plans disagreed, and how it's resolved

| Topic | [U] said | [D] found | Resolution |
|---|---|---|---|
| `grantNumber` "went from a closed list to empty" (U A.1) | Confirm it's intentional | The legacy `Dataset/Publication/... Grant Number` attributes had a closed list of 155 values but weren't in any portal template. The templates already linked through `GrantView Key`. `Grant View`'s `Grant Number` was already `^CA\d{6}$` on `main`. | Intentional, and it has no portal-sync impact. The only real consequence is the sentinel (O1). |
| `Assay` values dropped? (U A.2) | Diff the sets | Old union of 381 values vs new 391: **0 dropped** | Closed. The same holds for `Tumor Type` (186→186), `Tissue` (114→114) and `Species` (29→32). |
| `License` values dropped? (U A.3) | Confirm the superset | The old union had 336 values, the new list 326. **10 dropped**, all of them legacy Study short tokens. | Closed as a finding. Needs O6. |
| Biospecimen acquisition-method renames (U B.2) | Listed `Coreneedlebiopsy→BoneMarrowAspiration` and `ForcepsBiopsy→NeedleBiopsy` (enum-symbol forms) | CSV labels: `Blood draw→Blood Draw`, `Fine needle aspirate→Aspiration`. `Core needle biopsy`, `Forceps Biopsy`, `Punch biopsy` and `Shave biopsy` are removed; `Needle Biopsy`, `Bone Marrow Aspiration` and `Tumor Resection` are added (20→10 values). | Base the live-data audit (Phase 4) on the CSV labels, not the enum-symbol pairs. Treat the old-to-new mapping as a curator decision, not an assumed 1:1 rename. |
| DCC `schematic` usage (U C.2) | 5 files | [D] listed 2 | Use the U list: `upload-manifests.py`, `upload-workflow.sh`, `union_qc.py`, `utils/csv_to_ttl.py`, `curator_tools/create_file_based_metadata_task.py`. |
| DCC TTL tooling overlap with kg-pipeline (U follow-up) | Unread | `utils/csv_to_ttl.py` converts a **model** (schematic CSV or CRDC TSV) to RDF for several orgs (mc2, nf, htan, crdc, and others). `build_template_ttl.py` converts template headers to TTL. Neither builds instance data. | Different purpose from kg-pipeline's portal-instance KG, which overlaps only with its Stage 0 (model→OWL). Keep both, but check `csv_to_ttl.py` still parses the revised `Properties` CDE tags (Phase 2). |
| Portal-table live audit (U out-of-scope) | Not done | Done for the portal tables: 69 invalid values (Phase 4) | Portal tables are covered. The Biospecimen, Individual and Model annotation audit is **still open**. |
| JSON schema names (U C.4) | Visium rename is cosmetic | The registered `MC2Center` schemas still use the old `VisiumRNALevel*` names. Version ordering is broken (PublicationView `15.0.0` > `13.0.0`). | Not cosmetic in the registry. See Phase 1. |

---

## Phase 0: Pre-merge gates in data-models [DM]
- **0.1 Fix Species enforcement** [D]. `Species` is Required in the CSV but missing from `required` in DatasetView, FileView, Biospecimen, Model and Sequencing*.json. Find why the curator generation drops it.
- **0.2 Fix the key collision** [D]. `license` (the DUOPlus6 row) and `License` both become the JSON key `License` in DataCatalog.json and Study.json. Rename one of them.
- **0.3 Align the DataCatalog license list** [D]. `dataCatalogLicense` still uses the 11-token `studyLicense.csv`. Align it with SPDX, or document why it differs; tie this to O6.
- **0.4 Resolve O1 and O2.**
- **0.5 Generate the missing schemas** [D]. Generate JSON schemas for Person, Project, Consortium, Institution and Theme. None exist yet, so those tables have no source of truth.
- **0.6 Check the dca_config file** [U A.8]. Read `dca_config/dca-template-config.json` by hand against the renamed and removed schema names.
- **0.7 Confirm nothing uses the deleted consortium file** [U A.5]. Search the repo for any remaining reference to `consortium_name.csv`.
- **0.8 Release notes** [U C.1]:
  - `make all` now includes `convert`, so `mc2.model.jsonld` is regenerated.
  - `schematicpy` is dropped.
  - `qc_model` and `make qc` are removed (they were already broken).
  - The 18 `templates/*.csv` files have renamed headers.
- **0.9 Tag the release** (e.g. `v14.0.0`) so that DCC can pin `all_valid_values.csv` and the schemas to it [D].
- **0.10 Build checks** [U]. `make all` (root) and `make schema && make test` (kg-pipeline) pass from a clean clone. Decide O5.

## Phase 1: Clean up the schema registry [SYN] (Opus runs it, with explicit approval)
- **1.1 Register at 16.0.0** [D]. Register every schema from this release at **16.0.0**, above the stale PublicationView `15.0.0`, so "highest version" is unambiguous.
- **1.2 Register the renamed schemas** [U+D]:
  - Register `10xVisium*`.
  - Mark `VisiumRNALevel1-4` and `VisiumAuxiliaryFiles` deprecated in the registry table syn69735275; do not delete them.
  - Handle `ImagingChannel` according to O2.
- **1.3 Move registration into a tracked target** [D]. The untracked `local_inputs/Makefile` hardcodes `CURRENT_VERSION := 13.0.0`. Replace it with a tracked `make register-json VERSION=…` that wraps `utils/synapse_json_schema_bind.py --no_bind`.
- **1.4 Fill the registry ID** [D]. Set `SCHEMA_REGISTRY_TABLE_ID` in `curator_tools/query_schema_registry.py` to syn69735275, and make sure registry rows are added for the new versions.

## Phase 2: DCC code [DCC] (Sonnet SMEs, parallel after 2a/2b)
**Pattern:**
- Replace old attribute keys with new ones in the rename maps, the string-list column lists and `col_order`.
- Keep the camelCase portal targets and the positional order, because `utils.update_table` writes `df.values.tolist()`.
- Reuse the existing helpers in `portal_tables/utils.py`: `convert_to_stringlist`, `update_table`, `get_args` and `syn_login`.

- **2a. Crosswalk module** [D]: `portal_tables/crosswalk.py`.
  - For each component: an ordered mapping of schema key → portal column, plus the model version it targets.
  - It replaces the scattered rename maps and `annotations/attribute_dictionary.py`. This is the key U D-list file.
  - `check_crosswalk(table_id, component)` asserts the live Synapse column order before any write.
- **2b. Consortium lookup** [D]: `consortium_display_names(syn, ids)` in `portal_tables/utils.py`. It fails loudly on unknown IDs.
- **2c. Sync scripts** [U+D]:

| Script | Changes |
|---|---|
| `sync_publications.py` | `Publication Assay/Tumor Type/Tissue` become `Assay`, `Tumor Type`, `Tissue`. Replace positional `row[4]` with `row["GrantView Key"]`. |
| `sync_datasets.py` | `DatasetAssay/Species/Tissue/TumorType` become the shared keys. `DatasetView_id` becomes `DatasetViewId`. An unknown DUO code at `:117-123` must no longer skip the download fields. |
| `sync_tools.py` / `sync_education.py` | `ToolLicense` / `ResourceLicense` become `License`, as a string list. |
| `sync_grants.py`, `create_grant_projects.py` | `Grant Investigator` becomes `Investigator`, as a list. `Grant Consortium Name` becomes `Consortium Key`, resolved via 2b. Fix the shared `updated_scope` list at `sync_grants.py:81`. |
| `sync_projects.py` | `ProjectInvestigator` becomes `Investigator`. `ProjectGrantNumber` becomes `GrantViewKey`. Consortia come via 2b. Add `projectShortName`. |
| `sync_people.py` | `personConsortiumName` becomes `ConsortiumKey`, resolved via 2b. `personGrantNumber` becomes `GrantViewKey`. |

- **2d. Annotation and utility scripts** [U+D]:
  - Scripts: `annotations/processing-splits.py` (including `"Tool License"`), `add_cols.py`, `schema_update.py`, `edit_legacy_annotations.py`, `split_manifest_grants.py`, `utils/merge_and_correct_manifests.py`, `utils/table_to_annotations.py`, `utils/check_publications_status.py`.
  - Change: rename via `crosswalk.py`.
  - Pin the `all_valid_values.csv` URL to the release tag.
  - Also fix the existing bugs found along the way:
    - `table_to_annotations.py:525` `keys_to_drop=None` TypeError.
    - `pubMedUrl` should be `pubMedLink`.
- **2e. File-level annotations** [U B.4]. `File Assay Category` is now split out of `Assay`. Confirm that any DCC script touching File-level annotations writes each value to the correct field.
- **2f. Retire schematic** [U C.2 + D]. Move `upload-manifests.py`, `upload-workflow.sh` and `union_qc.py` from the `schematic` CLI to validation through `synapseclient.extensions.curator` against 16.0.0, matching `utils/create_curation_task.py`. Where a step has no curator equivalent, first verify that schematic still accepts the curator-generated `mc2.model.jsonld` (U C.2's check). Then flag the remaining schematic use for approval; don't leave it in place silently.
- **2g. `csv_to_ttl.py`** [U follow-up]. Check that it still parses the revised model's `Properties` (CDE tags) and `DependsOn`. Fix its `is_enum` inconsistency (lines 196 and 199).
- **2h. Workflows** [U]. `update-theme-graphs.yml` and `publications-status-check.yml` run on the `0 0 1 * *` cron. The scripts they call must ship in the same PR, **before the first of the month after #262 merges**.

## Phase 3: Synapse table changes [SYN] (Opus runs it with explicit approval; snapshot each table first)
**Portal tables** (keep names, fix types) [D]:
- `license` in Tools syn26127427 and Education syn51497305: STRING becomes STRING_LIST. Split the 4 comma-joined tool values.
- `investigator` in Grants syn21918972 and Projects syn21868602: STRING becomes STRING_LIST.
- Projects: add `projectShortName`.
- `consortium` columns: unchanged (decision 2).
- Datasets: per O3.

**Manifest tables** [D]: rename them to match the schema keys, or replace them with RecordSets bound to 16.0.0 (preferred).
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
- Manifest CSVs syn53478776, syn53478774, syn53479671 and syn53651540: regenerate them with the new keys.

**Bindings:** bind 16.0.0 to the RecordSets and folders that feed each manifest. Portal tables stay unbound.

## Phase 4: Live-data audit and curation [SYN, curation]
- **4.1 Portal tables** (done for audit, still needs fixing) [D]. Values that fail the new enums:
  - Publications `assay`: 60 values (typos, leading spaces, synonyms like `ChIP-seq`).
  - Publications `tissue`: 5 values (e.g. `Not Applicale`).
  - Publications `tumorType`: 1 value (`Pan-Cancer`).
  - Datasets `assay`: 3 values (`ELISpot`, `Micro-CT`, `snATAC-seq`).
  - Tools `license`: 4 comma-joined values.
  - For each, fix the source record or propose adding the term to the model.
- **4.2 Assay-level annotations** (open) [U B.1/B.2]:
  - Scope: Biospecimen, Individual and Model annotations in the MC2 assay file views.
  - What to check:
    - `Sex`: 9 values become 3.
    - Acquisition Method: 20 become 10.
    - Composition: 21 become 11.
    - Preservation Method: 20 become 14.
    - Preservation Medium: 24 become 17.
    - Tumor Grade: `G1` becomes `G1 Low Grade`.
    - Old labels in the fields that moved to UBERON or NCIt references.
  - Output: a mapping worklist.
  - Blocks merge unless every violation is triaged, either re-annotated or accepted as a legacy value.
- **4.3 Study-license tokens:** remap according to O6.
- **4.4 Worklist:** a Haiku SME can generate the mechanical correction CSV. A curator reviews it, and it's applied with batch edits in the style of `annotations/update_pending_annotations.py`.

## Phase 5: Communication
- **5.1 Curators** [U B.6]. Warn active curators about the 18 renamed template headers, so in-progress local templates don't mismatch.
- **5.2 Owners** [U C.5/C.7]. Tell the data-models owners that kg-pipeline has no CI coverage, as input to O5.

---

## Sequencing
1. Phase 0 is done and #262 is ready. **Don't merge it yet.**
2. The DCC Phase 2 PR is reviewed and passes its dry runs against a clone of #262.
3. Phase 4.2 is triaged.
4. Merge #262, tag the release, then run Phase 1 (registry).
5. Merge the DCC PR, **before the next `0 0 1 * *` cron**.
6. Phase 3 (Synapse tables), then the first sync with `sync-to-portal.yml`.
7. Phase 4.1 curation, then re-sync.

## Delegation
- **[DM] Phase 0:** a Sonnet SME opens a PR in data-models.
- **Phase 2:** 2a and 2b are one Sonnet task. Then 2c, 2d and 2e–2g run as parallel Sonnet tasks on `model-alignment`. Each script group is its own commit.
- **Phases 1 and 3:** Synapse writes. Opus does these directly, with explicit go-ahead.
- **Before any `gh pr create`:** an independent code review.

## Verification
- **Dry-run diffs.** Each `sync_*.py --dryrun` against the regenerated manifests produces `--output_csv`. Its diff against a snapshot of the pre-change portal table shows only the intended type and value changes [D].
- **Column order.** `check_crosswalk()` passes for all 7 portal tables [D].
- **Validation.** Manifests and RecordSets validate against 16.0.0 through `synapseclient.extensions.curator`. The only errors are the known Phase 4 items. Where schematic remains, it validates against the curator-generated `mc2.model.jsonld` [U C.2].
- **Registry.** `POST /schema/version/list` shows 16.0.0 as the highest version for every MC2Center schema, and syn69735275 has matching rows [D].
- **Builds.** `make all` and `make schema && make test` in kg-pipeline pass from a clean clone [U].
- **dca_config.** `dca_config/dca-template-config.json` has been read and confirmed or updated [U].
- **Assay-level audit.** The Phase 4.2 audit has zero untriaged violations [U].
- **Workflows.** `sync-to-portal.yml` is run first against a staging copy of one portal table. The Publications, Datasets, Tools and Grants facets on the portal still render [D].

## Evidence
- **[D] session reports:** the scratchpad files `model_diff.md`, `dcc_inventory.md` and `synapse_cols.json` (session-local; the key numbers are restated above).
- **[U] upstream plan:** `data-models/plans/cde_model_revisions_integration.md`.
- **Source plans:** both remain in place. This document supersedes them for execution order.
