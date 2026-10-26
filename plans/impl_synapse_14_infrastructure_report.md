# Implementation report: Synapse and infrastructure changes for data model 14.0.0

Running log for `plans/impl_synapse_14_infrastructure.md`.

## I0. Holds and communication
Done by the owner before 2026-10-23.

## I2. Register the 14.0.0 schemas
The owner registered 14.0.0 for every `MC2Center` schema on 2026-10-23 (about 22:30 UTC). Not yet compared against `json_schemas/` at the `v14.0.0` tag.

## I1. Registry cleanup (2026-10-23)
**Owner instructions, 2026-10-23:**
- Keep `SequencingRNALevel1` 15.0.1 until I7. Two data folders in CA293853 are bound to it: `syn75299050` (`CA293853_aml_mouse_mRNA_Level_1`, 539 files) and `syn75299771` (`CA293853_aml_mouse_miRNA_Level_1`, 142 files). [REVIEW: Retain 15.0.1 permanently, this is released data]
- Don't delete any 13.x version. That keeps `StudyAR` 13.0.0, 13.1.0, 13.2.0 and 13.3.0, which narrows O9 ("all of `StudyAR`"). [REVIEW: StudyAR can be removed, these are unused]

**Binding check before deleting:** checked 2,938 entities: 155 grant projects, their 2,783 direct children, and the 538 tables in syn52750482. The only hit on the delete list was the `SequencingRNALevel1` 15.0.1 binding above. Bindings deeper in the tree weren't covered, and Synapse's own check later found some (see the refusals below).

**Result: 30 of 36 versions deleted.**

| Schema | Deleted | Refused by Synapse |
|---|---|---|
| `PublicationView` | — | 15.0.0 (bound to an object) |
| `Biospecimen` | 15.0.0, 15.0.1 | — |
| `Model` | 15.0.0, 15.0.1 | — |
| `SequencingRNALevel1` | — | 15.0.0 (bound to an object) |
| `AccessRequirementCA123` | — | 17.0.0 (bound to an object); 18.0.0 (referenced by another schema) |
| `StudyAR` | 1.0.0–8.0.0, 10.0.0–12.0.0, 14.0.0, 16.0.0–24.0.0, 26.0.0–30.0.0 | 9.0.0, 15.0.0 (bound to an object) |

**Left in the registry after I1:**
- `PublicationView` 15.0.0
- `SequencingRNALevel1` 15.0.0 and 15.0.1
- `AccessRequirementCA123` 17.0.0 and 18.0.0
- `StudyAR` 9.0.0 and 15.0.0, plus the kept 13.x versions

None of these are 14.0.0 or 13.x versions targeted by I1. The refused versions go to I7: find the bound entities, rebind them, then delete the versions.

Scripts (scratchpad, not committed): `i1_dryrun.py` (registry listing and binding check), `i1_delete.py` (guarded delete). Guards: no 13.x, nothing registered on 2026-10-23, no `AccessRequirementCA261841`, not `SequencingRNALevel1` 15.0.1.

## B4. Registry cleanup, second pass (2026-10-26)
**Owner decision (R5, revised):** leave bound versions in place. For each schema that has a 14.x version, keep its newest version below 14 and delete the unbound older ones. Delete unbound `StudyAR` 13.x. Don't touch `AccessRequirementCA261841`, `AccessRequirementCA123`, `Landscape` or `Resource`, and don't delete anything ≥ 14.

**B2 verification first (read-only):** all 38 `json_schemas/*.json` at `v14.0.0` match their registered `MC2Center-<Name>-14.0.0`, ignoring `$id`, `$schema` and key order. The PublicationView enum has `RNA Sequencing` and not `RNASequencing`, and no key contains a space.

**Result: 29 of 45 candidates deleted.** Synapse refused the other 16, every one with "Cannot delete a schema version that is bound to an object". They stay.

| Schema | Deleted | Refused (bound) |
|---|---|---|
| `Biospecimen` | 13.0.0 | — |
| `DataCatalog` | 12.0.1–12.0.7 | 12.0.8 |
| `DataDSP` | — | 12.0.0, 13.0.0 |
| `DatasetView` | — | 13.0.0 |
| `FileView` | 13.0.0–13.0.5 | 13.0.6 |
| `Model` | 13.0.0 | — |
| `PublicationView` | 13.0.0 | 1.0.0, 1.1.1, 1.1.9 |
| `SequencingRNALevel1` | 13.0.0, 13.0.1 | 13.0.2, 13.0.8 |
| `Study` | 2.0.0–2.0.7 | 3.0.0, 4.0.0, 13.0.0, 13.1.0 |
| `StudyAR` | 13.0.0, 13.1.0, 13.3.0 | 9.0.0, 13.2.0 |

Kept as the previous release: the newest version below 14 of each schema (33; listed in the dry run). `StudyAR` 15.0.0 isn't deleted under the ≥ 14 rule; it was already known to be bound (I1).

Script (scratchpad, not committed): `b4/cleanup.py`. With no flags it's a dry run; `--execute` asserts the guards before every call. Per-version results are in `b4/results.csv`.

## C1. RecordSet spike setup (2026-10-26)
Test project "MC2 infrastructure refresh testing" `syn78387305` (owner-created; empty and unbound beforehand). Created with `create_record_based_metadata_task`, which binds each RecordSet:

| Entity | ID | Binding |
|---|---|---|
| Folder `publications` | `syn78387908` | — |
| Folder `datasets` | `syn78387909` | — |
| Folder `tools` | `syn78387910` | — |
| `MC2Center_PublicationView_RecordSet` | `syn78387911` | `MC2Center-PublicationView-14.0.0` (CurationTask 8036, Grid session `MTMxMjQ1Ng==`) |
| `MC2Center_DatasetView_RecordSet` | `syn78387914` | `MC2Center-DatasetView-14.0.0` (CurationTask 8037, Grid session `MTMxMjQ1Nw==`) |

The upsert keys are `PublicationViewId` and `DatasetViewId`. Rows are loaded in C2/C3.

## C3. RecordSet spike results (2026-10-26, `syn78387305`)
Sample: 20 PublicationView and 20 DatasetView rows from grant CA193461 (`syn52499989`, `syn52745189`), converted with `RENAMES_13_TO_14` and `normalize_headers`. Scripts: scratchpad `c3/p1_direct.py` … `p5_null.py`.

| # | Question | Result | Evidence |
|---|---|---|---|
| 1a | Does a new CSV version keep the **binding** and the **CurationTask**? | **Yes** | After the direct store (v2) and 5 Grid exports (v3–v6), `get_schema` still returns `MC2Center-<Type>-14.0.0`, and tasks 8036/8037 still exist. |
| 1b | Does an open **Grid session** survive a new version? | **The session survives, but exporting it overwrites newer data** | The session opened at v1 (empty) was exported after the direct v2 store of 20 rows. The RecordSet went from 20 rows to 1. Two concurrent fresh sessions A and B: B exported an edit, then A exported, and **B's edit was lost**. An export writes the session's whole snapshot plus its edits. |
| 2 | Does validation re-run on each version? | **Only on a Grid export, not on a direct store** | A direct store left `validation_summary` and `validation_file_handle_id` null after 60 s. Each Grid export produced them immediately. Grid validation is **asynchronous**: rows exported straight after import are `unknown`, and after a 45 s wait they were validated. |
| 3 | Is a primary-key upsert idempotent? | **Yes, through a Grid session** | Importing the same 20 rows into a fresh session twice gave 20 rows, 0 duplicate keys, and the edited row was replaced by its key (`PublicationViewId`). |
| 4 | Can SQL or a MaterializedView read a RecordSet? | **No** (as expected) | `tableQuery("select * from syn78387914")` → 400 |
| 5 | Time and size for ~1,200 DatasetView rows | **OK** | 1,200 rows (2.0 MB CSV): Grid import 6.8 s, export 29.9 s |
| 6 | Do list fields round-trip? | **Yes** | Both `RNA Sequencing, Whole Genome Sequencing` and `["RNA Sequencing", "Whole Genome Sequencing"]` are stored as the JSON array `["RNA Sequencing","Whole Genome Sequencing"]`, so RecordSet readers must parse JSON arrays. An invalid term (`Not An Assay`) is flagged. |

**Validation finding:** every sample DatasetView row failed 14.0.0 with `expected type: String, found: Null`. A one-field test pinned it on an empty `DatasetDoi`, which 14.0.0 still requires. Empty `StudyKey` (optional) and empty `DatasetDataUseCodes` pass. The v14.0.1 patch makes `Dataset Doi` optional (D-Q23), which removes this failure. PublicationView: 20 of 20 valid.

**Implications for D11 (`upload_recordsets.py`) if RecordSets are chosen:**
- write only through a fresh Grid session: create → `import_csv` → wait until no rows are `unknown` → `export_to_record_set`;
- never store a CSV directly;
- refuse to run while any other Grid session is active on the RecordSet (`Grid.list(source_id=…)`), since an export from an older session overwrites later changes.

Test data stays in `syn78387305` until it's cleared after I9.

## C6. Grant creation, live test (2026-10-26)
A fake grant (`ZZ MC2 Spike Test Grant CA000000`, team `CSBC SPIKE U01 CA000000`) was run through `create_grant_projects()`.
- **First run:** created project `syn78390168` with all 10 folders, the main wiki plus the "Project Investigators" sub-page, and team `3624631`. The ACL has admin principal 3450948, the creator, and the team.
- **Second run:** reused the same project and team ("Reusing existing project/team"), with no duplicate folders.
- **One inconsistency:** the team ID is a string from a new team and an integer from a reused one. It's harmless in the f-string links; normalize it to `str` in a follow-up.
- **Cleanup:** the project (skipping the trash can) and the team were deleted, and both return 404 afterwards.

## C7. D-2 rollback rehearsal on a copy of Tools (2026-10-26)
Copy: `syn78390211` ("C7 rollback rehearsal - Tools copy", in `syn78387305`), 349 rows taken from `syn26127427`.
- **`license` STRING → STRING_LIST works.** Values are kept, but a comma-joined value becomes a one-item list (`['GPL-3.0, MIT']`); the next sync splits it.
- **Synapse refuses to change a list column back** ("Cannot perform schema change from _LIST type to non-_LIST type").
- **Rollback that works** (`c7/c7c.py`):
  1. `delete_column("license")` → store
  2. `add_column(Column("license", STRING, maximum_size=50))` → `reorder_column("license", original_index)` → store
  3. `delete_rows` → `store_rows(pre-change CSV export)`
- **Result:** column order identical to the live Tools table, `license` back to `STRING`, all 349 rows equal to the export (0 differing cells). The copy stays in `syn78387305` until it's cleared after I9.

## D11 live test (2026-10-26, `syn78387305`)
- **Write log:** test table `syn78390296` ("MC2Center_RecordSet_WriteLog (test)"). RecordSet: `syn78387911`.
- **Upload:** 3 publications from CA193461 (p1 `21446047`, p2 `26349637`, p3 `26379039`). Script: scratchpad `d11/live.py`.

| Step | Action | Result |
|---|---|---|
| 1 | Dry run | `dryrun`, first write, nothing written |
| 2–3 | Execute twice | Written (v8, v9) and logged. Each was reported as a "verification mismatch" because a Grid import **drops columns that aren't schema properties** (`Component`). Fixed in `925e52c`: such columns are left out of uploads. |
| 4 | A contributor edits p1's title and adds row p9 (v10); the pipeline uploads a different p1 title and a new p2 title | v11: p1 kept the contributor title (`kept_edit`, conflict reported), p2 updated, p9 kept (`kept_contributor_row`) |
| 5 | Contributor A sets p2's year (v12). Then a stale session, opened before step 4, exports a p3 journal edit (v13), wiping p9, A's year, the pipeline's p2 title and p1's contributor title. Then the pipeline runs. | v14, verified. **Recovered:** A's year (`kept_edit` at v12, `restored_revert` at v13), the p2 title, p9 (`restored_missing`). **Kept:** B's journal edit. **Conflicts reported:** both contributor values that won over the upload. **Defect:** p1's contributor title (from step 4) was overwritten by the upload, because contributor-wins was only applied in the run that detected the edit. Being fixed: ownership is now decided by the origin of the current value. |
| 6 | After the ownership fix (`ec122f4`): a contributor sets p1's title again (v15); the pipeline uploads the same file | v16: p1 kept (`kept_edit`); 3 conflicts reported (p1 title, A's p2 year, B's p3 journal) |
| 7 | The pipeline runs again with the same upload | v17: still kept, with the same 3 conflicts reported. **Contributor values now survive later runs.** v17 was an unnecessary write, so unchanged merges are now skipped (`217e10b`, status `unchanged`). |

## D11 v2 live test (2026-10-26, `syn78387305`)
After the second review, `1a2fd19` replaced ownership inference with a replay that tracks ownership plus write log v2. `39b1af3` then classified stale cells by the session's snapshot. The write log v2 test table is `syn78392519`, created by `create_write_log_table`. Script: scratchpad `d11v2/live.py`.

| Scenario | Result |
|---|---|
| First write (17 unlogged versions, a pipeline-owned baseline) | v18 written; the upload applied |
| Contributor edits p1's title; two pipeline runs | Kept both times, conflict reported; both runs `unchanged` (nothing written) |
| Contributor A sets p2's year (v20); a stale session exports a p3 journal edit (v21), wiping A's edit | **First rule** (any past value = stale): A's 1999 had been in the cell before, so it counted as stale and was lost. That's the same mechanism that undoes a curator's deliberate revert, so the rule changed. **Snapshot rule:** v20 `kept_edit`, v21 `restored_revert` (untouched cell) plus `kept_edit` (journal). Final: A's year 1999, B's journal, p1's contributor title. |
| A crash right after an export (`record` raises) | v23 recorded as `recovered` (pending row + principal + time). The next run applied the pipeline's new p2 title with no false conflict. |
| Another run's pending row | `locked`, grant skipped. Once that row was resolved (`aborted`), the run went through. |

Write log after the test: pending/written pairs for v18, v22 and v24; v23 `recovered`; the other run's row `aborted`.
| Third-review fixes (`2ad3401`, `0e7ccc6`, `3484769`), complete-reversion snapshot rule | **Stale session with an edit:** opened at v26; the pipeline wrote p2 "newer" (v27); the stale session saved a p3 abstract edit (v28), reverting p2's title. Next run (v29): p2 `restored_revert`, the p3 abstract `kept_edit`, conflict reported. **Partial undo:** not exercised live (the test row's title was already contributor-owned); covered by unit tests, including a randomized comparison with a brute-force version of the rule. |
