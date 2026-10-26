# Consolidated plan: data model 14.0.0 cutover (pipeline, DCC, Synapse, orchestration)

**As of 2026-10-26, revision 2.** This revision folds in your answers on revision 1. This document replaces the "what's next" parts of:
- `plans/impl_ai_curation_pipeline_14.md` (P-tasks) and its report
- `plans/impl_dcc_14_alignment.md` (D-tasks) and its report
- `plans/impl_synapse_14_infrastructure.md` (I-tasks) and its report
- `orchestration/README.md` (orchestrator design)

Those files stay as the record of what was specified and done. Their task IDs (P0, D8, I3, …) are kept here so you can trace each item back. The decision log in `plans/cde_model_alignment_harmonized.md` still applies; section 3 lists every decision made since.

**Section 2 lists every change to the CCKP database project (syn21498902) and the tables that feed it.** Read it first if you only need to know what changes about how the CCKP gets its data. **Section 4 lists what's still waiting on you.**

---

## 1. Where things stand

| Stream | State | Where |
|---|---|---|
| data-models | `v14.0.0` tagged at `c234c467` (2026-10-23). DM-13, DM-14 and DM-15 are open. DM-16 is new (section 3). | `mc2-center/data-models` |
| ai-curation-pipeline | P0–P3 done and reviewed on three stacked local branches. **None are pushed or in a PR.** P4 is deferred. | `model-14-alignment` → `model-14-output-contract` → `model-14-validation` |
| mc2-center-dcc code | D1–D10, D13, D14, D16 and D17b done, plus DUO text from governanceDUO (D-Q17); 208 tests pass. D11, D12 (RecordSet mode) and D15 are blocked on the RecordSet spike. D17a deletions are not done. | `model-alignment` (pushed), PR #167, stacked on #166 (OOP migration). #167's description is out of date. |
| Synapse registry | I0 holds are in place. I2: 14.0.0 is registered for all 38 data-model schemas, but not yet compared file by file with the tag. I1: 30 of 36 versions deleted; 6 refused (bound or referenced), and they stay (D-Q8). | `MC2Center` org: 43 schemas, 129 versions |
| Portal tables (syn21498902) | **Unchanged.** No type changes and no non-dryrun syncs since the cutover began. | Section 2 |
| Grant-level manifests | 532 tables in 13.1.0 shape. No RecordSets exist. | grant projects |
| Test project | "MC2 infrastructure refresh testing" `syn78387305`: empty, unbound, you have full rights. | Phase C |
| Orchestrator | Design only (`orchestration/README.md`). | — |

**The 2026-11-01 deadline is soft.** Both monthly cron jobs (`update-theme-graphs.yml`, `publications-status-check.yml`) run `main`'s code against the portal tables. Those haven't changed, and both jobs succeeded on 2026-10-01, after data-models #262. The rule is: **nothing in section 2.1 changes before #166/#167 merge.**

---

## 2. Changes to the CCKP database and the tables that feed it

The portal app reads **only the tables in "Cancer Complexity Knowledge Portal - Database" (syn21498902)** (confirmed, D-Q9). The sync scripts write them from manifests in **"CCKP - MC2 Admin" (syn28742843)**. Each item below is marked:

- **SCHEMA**: a column's type changes, or a column is added. Whatever reads that column has to change.
- **VALUE**: same column and type, but the content or form of some values changes.
- **NONE**: checked, no change.

### 2.1 Portal tables in syn21498902: what the CCKP reads

Decision 1 still holds: **no portal column is renamed, removed or reordered.**

| Table | Column | Kind | From → to | Why | When |
|---|---|---|---|---|---|
| Tools `syn26127427` | `license` | **SCHEMA** | `STRING` → `STRING_LIST` | 14.0.0 `License` is multi-valued; 4 rows hold comma-joined values today | D-2 (I4) |
| Education `syn51497305` | `license` | **SCHEMA** | `STRING` → `STRING_LIST` | Same shared `License` attribute | D-2 (I4) |
| Grants `syn21918972` | `investigator` | **SCHEMA** | `STRING` → `STRING_LIST` | 14.0.0 `Investigator` is multi-valued | D-2 (I4) |
| Projects `syn21868602` | `investigator` | **SCHEMA** | `STRING` → `STRING_LIST` | Same | D-2 (I4) |
| Projects `syn21868602` | `projectShortName` | **SCHEMA** | new trailing `STRING` column | New 14.0.0 `Project Short Name` | D-2 (I4) |
| Datasets `syn21897968` | `accessInformation` | **SCHEMA** | `STRING_LIST` item `maximumSize` 100 → **300** (list length stays 10) | The governanceDUO texts (D-Q17) are up to 232 characters; at 100, the first dataset sync would be rejected (D-Q27) | D-2 (I4), before E4 |
| Tools, Education | `license` | **VALUE** | Free-text names (`MIT License`) → SPDX IDs (`MIT`, `Apache-2.0`, `CC-BY-NC-4.0`); values not in the 14.0.0 set are left blank | O6 / I8.3 | E1 + first sync |
| Datasets `syn21897968` | `accessInformation` | **VALUE: every row's text changes** | The text now comes from governanceDUO (D-Q17): the DUO label plus its definition, e.g. `Return to Database or Resource - This data use modifier indicates that the requestor must return derived/enriched data to the database/resource.` That replaces the old paraphrase on all 1,141 rows. **About 1,131 `Pending Annotation` rows** change from `Access information was not provided for this dataset` to `The data use modifier for this record has not yet been annotated.` The 5 `NPUNCU` rows map to the single ID `DUO:0000018` (D-Q21), so they still show one entry. Every text is in Appendix A. | O10 / I8.4 / D-Q17 | first sync after I8.4 |
| Publications, Datasets | `assay`, `tissue`, `tumorType` | **VALUE** | 60 + 5 + 1 publication values and 3 dataset values corrected or kept, per the I8.1 worklist. No live `Not-Applicable` or `immunoassay` values (checked 2026-10-26), so DM-16 needs no remap. | I8.1 | first sync |
| Tools, Datasets | `doi` | **VALUE** | 7 outliers normalized to `https://doi.org/…`: 5 tools and 1 dataset use `http://doi.org/`, and 1 tool uses `https://dx.doi.org/`. The other 5,955 live DOIs already use that form, so they don't change. Also, 3 `No DOI Listed` values (2 publications, 1 dataset) become `DOI Not Available` (D-Q22). | D-Q1 / D-Q22 | first sync after I8.1 |
| Tools, Datasets, Education | list cells | **VALUE (cosmetic)** | List items keep the manifest's order, without duplicates. Before, the order was random (`set()`). | D5 | first sync |
| Datasets | `downloadSynId` | **VALUE** | A row whose manifest lists two synIDs keeps the first one, instead of a random one | D4 | first sync |
| Education | `publicationId` | **VALUE** | Empty value: `"['']"` → `""` | D5 | first sync |
| Publications, People | all | **NONE** | People fails to sync until D-1 (I4b) | — | — |
| Datasets | other columns | **NONE** | The 15 DUO fields are not added (O3) | — | — |
| all | `consortium` | **NONE** | Still display names (decision 2), now resolved from `Consortium Key` | — | — |
| All Data Files v2 `syn27210848` (EntityView) | scope | **NONE for the portal** | It sits in syn21498902, but the portal doesn't read it (D-Q14). `gen-mp-csv.py` reads it and `sync_grants.py` adds new grants to its scope; both are unchanged. Archiving centrally (D-Q15) adds nothing to it. | — | — |

**What a reader of these tables has to handle:**
0. **Datasets `accessInformation` accepts items up to 300 characters** (was 100). Anything that copies this column into a fixed-size field elsewhere needs the same limit.
1. **Four columns become `STRING_LIST`.** Queries and facets on `license` or `investigator` need list semantics: in SQL, `HAS ('MIT')` instead of `= 'MIT'`; in a portal facet, a multi-value facet. Other readers (kg-pipeline, the Croissant export) need the same check. `tally_themes.py` reads neither column.
2. **Projects gains one trailing column.** Readers that go by position are affected; readers that go by name are not.
3. **License values become SPDX IDs.** The portal shows `Apache-2.0`, not `Apache License 2.0`.
4. **Every dataset's `accessInformation` text changes** to governanceDUO's wording (Appendix A). The text is longer and starts with the DUO label. The number of entries per dataset doesn't change.

**How D-2 runs (per your note: verify each change immediately, then roll back or fix the portal).** Live portal with a rehearsed rollback (D-Q16). One table at a time:
1. **Rehearse once first**, before touching any live table. In `syn78387305`, copy Tools, change `license` to `STRING_LIST`, then roll it back: restore `STRING` and re-store the rows from the CSV export. Confirm the copy matches the export.
2. Snapshot the live table (`Table.snapshot(label="pre-14.0.0-I4")`) and export it to `output/pre14/<table>.csv`.
3. Change the column type, or add `projectShortName`.
4. **Verify right away, before the next table:**
   - `check_crosswalk` passes on that table.
   - A sync `--dryrun` for that table writes lists, not strings, to that column.
   - On the live CCKP, that table's explore page loads, the facet for that column filters, a detail page shows the values, and search returns rows.
5. **If anything fails:** decide on the spot between rolling back (restore the type and re-store the rows from step 2, as rehearsed) and fixing the portal config. Record which in the infrastructure report.
6. Move to the next table only after step 4 passes. Order: Tools → Education → Grants → Projects → Datasets (`accessInformation` size). For Datasets, step 4's dry run must produce the governanceDUO texts with one entry per code, and the new pre-write size guard must pass.

### 2.2 CCKP - MC2 Admin (syn28742843): what feeds the CCKP

The portal doesn't read any of these (D-Q9).

| Entity | Kind | Change | When |
|---|---|---|---|
| Consortium `syn36345930` | **SCHEMA** | Add `consortiumId` (`STRING`), filled for the 8 existing rows: `program.csbc`, `program.ps-on`, `program.tec`, `program.icbp`, `program.nci-clinical-and-translational-exploratory-developmental-studies`, `program.ccbir`, `program.metnet`, `program.pdmc` | D-1 (I4b) |
| Consortium `syn36345930` | **VALUE** | Add 3 rows (`program.htan`, `program.nci`, `program.sage-bionetworks`). The values are drafted in Appendix B and need your approval. | D-1 (I4b) |
| Grant manifest `syn53259587` (table) | **SCHEMA** | `Grant Investigator` → `Investigator`; `Grant Consortium Name` → `Consortium Key` (values become `program.*`); add `Institution Key`, `PersonView Key`. If G1 = RecordSets, it's replaced by a RecordSet. | E1 |
| Project manifest `syn59074382` (table) | **SCHEMA** | `Project Investigator` → `Investigator`; `Project Consortium Name` → `Consortium Key`; `Project Grant Number` → `GrantView Key`; add `PersonView Key`, `Project Short Name` | E1 |
| PersonView `syn38301033` (table) | **SCHEMA** | camelCase → 14.0.0 template headers | E1 |
| Merged manifests (CSV files): pubs `syn53478776`, datasets `syn53478774`, tools `syn53479671`, education `syn53651540` | **SCHEMA (file headers)** | New file versions with 14.0.0 headers (`RENAMES_13_TO_14`). For datasets, `Data Use Codes` → `Dataset Data Use Codes` with DUO IDs as values. DOI outliers are fixed here, so the sync carries the fix through. | E2 |
| `release_validation` `syn53461903` | **SCHEMA (file headers)** | Same as the merged manifests | E2 |
| New folder `archive_13.1.0/` | **new entities** | Holds the 532 legacy grant tables, one subfolder per grant (D-Q15) | E6 |
| `MC2_All_Project_Tables` view `syn52750482` | **deleted** | It empties by itself once the tables move out of the grant projects. Delete it and remove it from `sync_grants.py`. See E6. | E6 |
| UNION MaterializedViews (per type) | **deleted / rebuilt** | Deleted if G1 = RecordSets (D12 builds unions in Python); rebuilt if G1 = tables | E1 / E6 |

### 2.3 Outside the CCKP projects (no effect on how the CCKP reads data)
- **The `MC2Center` schema registry** and its table `syn69735275` (project "Curator"). Deletions are listed in Appendix C.
- **The 532 grant-level tables** while they're still in the grant projects, and the bindings on them.
- **The test project `syn78387305`**.

---

## 3. Decision log since the master plan

All of these come from your review notes and answers on 2026-10-26.

| # | Decision | What it changes |
|---|---|---|
| R1 / D-Q1 | **DOIs:** use the current recommended form, `https://doi.org/{doi}`, and normalize everything else | P5 (A4) normalizes pipeline output. I8.1 fixes the 7 live outliers. DM-16 corrects the PublicationView description, which says `www.doi.org`. |
| R2 | **`Not Applicable` and `Immunoassay` are the correct terms** | DM-16: remove `Not-Applicable` from Tumor Type, fix the `immunoassay` casing in `mc2.model.jsonld`. Then the pipeline's "the spaced form wins" workaround can go (B3). |
| R3 | **P4 ignored for now** (pipeline branch rebases) | Moved to Later |
| R4 | **`SequencingRNALevel1` 15.0.1 is kept permanently** (released data) | Never deleted or rebound |
| R5 (revised) | **Registry cleanup:** leave bound versions in place. Delete the unbound out-of-date ones, keeping **the newest version below 14 for each schema** as the previous release. **`StudyAR` 13.x is deleted too.** Schemas with no 14.0.0 version (`Landscape`, `Resource`, `AccessRequirementCA123`) and `AccessRequirementCA261841` are untouched. | B4. Appendix C has the exact list: 45 candidates, about 44 deletions. Synapse refuses bound versions, and `StudyAR` 9.0.0 is known to be bound. Only 5 schemas ever had a 13.1.0 registered, so "previous release" means 13.0.x for most of them. |
| D-Q2 | **Fix DM-14 and DM-16, release `v14.0.1`, bump `MODEL_REF`** | B1–B3, before E1 |
| D-Q3 | **DCC:** merge #166, then #167, as stacked, after refreshing #167's description and running an independent review | A1, A2, A6 |
| D-Q4 | **Pipeline:** three stacked PRs opened together. P0 can merge right away. | A3 |
| D-Q5 | **Exercise grant creation once** with a fake grant, then clean up | C6 |
| D-Q6 | **D17a deletions** go in a separate PR after #167 | A7 |
| D-Q7 | **DUO texts reviewed before the first dataset sync.** They now come from governanceDUO (D-Q17). | Appendix A, waiting on you (W1) |
| D-Q8 | **Leave the remaining I1 refusals:** `PublicationView` 15.0.0, `SequencingRNALevel1` 15.0.0, `AccessRequirementCA123` 17.0.0/18.0.0, `StudyAR` 15.0.0 | Removed from I7 |
| D-Q9 | **Only syn21498902 tables populate the portal** | The Consortium table is admin-only (section 2.2) |
| D-Q10 | **Display names `HTAN`, `NCI`, `Sage Bionetworks` confirmed.** I draft the other columns and you approve them. | Appendix B, waiting on you |
| D-Q11 | **Suppress `required` errors for DCC-assigned IDs** in the pipeline report | A5 |
| D-Q12 | **Log the live-data oddities and leave them:** education `dataType` holds synIDs; one dataset title has a corrupted thin space; tools `portalDisplay` is written as `"true"` | Known issues (section 6) |
| D-Q13 | **Gate G1 = RecordSets** if the binding, the CurationTask and Grid survive a new CSV version, and upserts are idempotent (I3 questions 1–3) | C4 |
| D-Q14 | **The portal doesn't use `syn27210848`** (All Data Files v2) | Section 2.1 last row |
| D-Q15 | **Archive the legacy tables in one admin folder**, not per grant | E6 |
| D-Q16 | **D-2 is verified on the live portal, with a rollback rehearsed first** | Section 2.1, "How D-2 runs" |
| D-Q17 | **governanceDUO is the source for DUO portal text.** The text is the DUO label plus definition from `linkml/mixins.yaml` `DataUseModifierEnum`, with the curator guidance ("If providing this term…") removed. | `portal_tables/duo_terms.csv` is generated by `utils/sync_duo_terms.py` from a governanceDUO checkout, and `DUO_DICT` is loaded from it. It replaces the hand-written paraphrase. Note that governanceDUO's model is on branch `synapse-curator-infra`, not `main`. |
| D-Q18 | **Use the official DUO shorthands** (`US`, `RS`, `TS`, `IS`, `GS`, `DS`, as in governanceDUO and DUO), and **fix data-models `duo.csv`**, which uses `USE`, `RST`, `TSM`, `IST`, `GSR`, `DSR` | The dataset sync accepts both spellings during the transition. The `duo.csv` fix goes into DM-16 (B1). |
| D-Q19 | **NPOA's ID:** official DUO uses `DUO:00000044` (8 digits, a quirk in DUO); everything here uses `DUO:0000044`. **Accept both**, and **widen the data-models pattern to `DUO:\d{7,8}`**. | The dataset sync aliases the 8-digit form. The pattern change goes into DM-16 (B1). governanceDUO's `meaning: DUO:0000044` doesn't resolve in DUO; that's logged in Later for governanceDUO. |
| D-Q20 | **Keep in sync with a vendored CSV and a script**, not a hand-copied dict | Re-run `utils/sync_duo_terms.py` when governanceDUO's DUO enum changes. Done on `model-alignment`: `26840f4` (script + CSV, from governanceDUO `868f184`), `d01c959` (loader, both shorthand spellings, NPOA alias); 208 tests pass. |
| D-Q22 | **Missing DOIs:** source manifests stay blank. The portal shows `DOI Not Available` (as `sync_datasets.py` already writes), and `No DOI Listed` (2 publications, 1 dataset) is standardized to it. | **VALUE** change on 3 portal rows. `sync_publications.py` writes the same placeholder for a blank DOI. The 11 existing dataset placeholders stay. |
| D-Q23 | **`Dataset Doi` becomes optional** in the v14.0.1 patch. The migration doesn't change source content; the portal sync keeps filling the DOI from the linked publication. | DM-16 (B1). No I5 fill step. |
| D-Q24 | **The owner reviews the I8.1 enum worklist** as a CSV (149 values; 53 auto-fixable, 96 need a curator) | `output/worklists/i8_1_enums.csv` (gitignored) |
| D-Q25 | **Defect fixed:** `processing-splits.py`'s 500→400 "(Read more on Pubmed)" cap cut multi-value enum cells mid-term (4 live publication `assay` values, e.g. `Immunohistochemi(Read more on Pubmed)`). The cap applies to free-text fields only. | DCC fix. The 4 values are in the I8.1 worklist; the terms lost after each cut have to come back from the source. |
| D-Q26 | **Defect fixed upstream:** trailing spaces in 14.0.0 valid values (`Gene Silencing `, `Diagnosis Date `, `Database Hits (Sequence) Format `) | DM-16 (B1). Portal `Gene Silencing` (11 dataset rows) becomes valid without a change. |
| D-Q27 | **Widen Datasets `accessInformation` to 300 characters per item**, rather than shortening the approved text. The live limit was 100; the longest text is 232. | New SCHEMA row in section 2.1 (D-2) |
| D-Q28 | **A2 pre-PR review** of `model-alignment` (2026-10-26): 1 must-fix, 5 should-fix, 2 nits. Being fixed in one batch:<br>• access texts split on commas (must-fix)<br>• Projects sync crashes once `projectShortName` exists<br>• `tally_themes` writes by position<br>• `fillna("")` on nullable dtypes<br>• no cell-size check before the delete<br>• `check_grant_ids` runs too late<br>• `strip_newlines` changes dtypes<br>• the enum truncation (D-Q25)<br>• the DOI placeholder (D-Q22)<br>Live checks: the count tables' order matches the code, and no `None`/empty `grantId` rows exist. | Fixes go to `model-alignment` before A1/A6. Nit 8 (14.0.0-header splits against 13.1.0 grant tables before E1) is covered by the I0 curation hold. |
| D-Q29 | **Gate G1 = RecordSets: shared RecordSets with cell-level reconciliation** (owner, 2026-10-26; spike evidence in the infrastructure report, C3). The pipeline and Curator UI contributors write the same `MC2Center_<Type>_RecordSet` per grant. The pipeline can't see or end other users' Grid sessions, so it reconciles instead. For each version it didn't write, it compares each cell with its own last version and the earlier versions:<br>• a value never seen before is a contributor edit and is kept<br>• a value that matches an older version is a stale revert, and the pipeline's value is restored<br>• a missing pipeline key is restored and reported<br>Writes go only through fresh Grid sessions (import → wait for validation → export), never through a direct store, and curation tasks are created with `create_grid=False`. | I5a; D11 (with reconciliation), D12 RecordSet mode and D15 are unblocked. Separate contribution RecordSets were rejected: twice the entities and ACLs, a write to every contribution RecordSet on every sync, and the same stale-session problem. The Synapse conflict-check request is deferred. |
| D-Q21 | **`NPUNCU` → the single ID `DUO:0000018`**, not NPU + NCU. `DUO:0000018` is a current DUO term, and governanceDUO and data-models both list it. | Reverses master plan S4.5's split. I8.4 maps `RTN` → `DUO:0000029` and `NPUNCU` → `DUO:0000018`, one to one. |

---

## 4. Waiting on you

W1 (DUO access texts, Appendix A) and W2 (Consortium rows, Appendix B) were **approved by the owner on 2026-10-26**. What's left is one item:

| # | What | Blocks | Where |
|---|---|---|---|
| W3 | **Go-ahead for each 🔒 step** when it comes up. Deletions (B4, E6) are confirmed against a printed list at execution time. | Each step | Section 5 |

---|---|---|---|
| W1 | **Confirm the governanceDUO-derived access texts** in Appendix A, as the portal will show them. Wording changes belong in governanceDUO, followed by a re-run of `utils/sync_duo_terms.py`. Two rows to look at: `DUO:0000017` has no label in governanceDUO, and `DUO:0000016` has no final period. | E4, the first dataset sync | Appendix A |
| W2 | **Approve the 3 new Consortium rows**, or edit them in place | D-1 (I4b) | Appendix B |
| W3 | **Go-ahead for each 🔒 step** when it comes up. Deletions (B4, E6) are confirmed against a printed list at execution time. | Each step | Section 5 |

---

## 5. Next steps

Phases run in order; within a phase, items run in parallel unless one names another. **🔒 = a Synapse write or other outward-facing action.** Opus runs each of these with your go-ahead for that step, after a snapshot and CSV export, and logs it in `plans/impl_synapse_14_infrastructure_report.md`.

### Phase A: Land the code (no Synapse writes)
| Step | Task | Owner/tier | Check |
|---|---|---|---|
| A1 | Refresh #167's description: code + plans, tag done, links to the reports and this plan. 🔒 | Opus | — |
| A2 | Independent pre-PR review of `model-alignment` against `main`, through #166 | fresh reviewer | Findings fixed or explained |
| A3 | Push the three pipeline branches and open stacked PRs (D-Q4). 🔒 | Opus | CI green |
| A4 | **P5 DOI form** (pipeline). Write `Publication Doi` and `Tool Doi` as `https://doi.org/{doi}`. Rewrite `http://doi.org/`, `dx.doi.org` and `www.doi.org` forms to that, and pass other URLs through unchanged. Add the same normalization to DCC `processing-splits.py`, so manual manifests are covered too. | Sonnet | `pytest` in both repos. The 10-paper run's validation report has no DOI rows. |
| A5 | **P2 report noise** (pipeline). Leave `required` failures for `DatasetView_id` and `ToolView_id` out of `validation_report.csv` and log their count instead. | Sonnet | The 10-paper run's report lists only real gaps |
| A6 | Merge #166 → #167, then the pipeline PRs. 🔒 | Opus, your go-ahead | `pytest` on `main` |
| A7 | D17a deletions in their own PR. 🔒 | Opus | `git grep` finds no references |

### Phase B: Model patch and registry (parallel with A)
| Step | Task | Check |
|---|---|---|
| B1 | data-models **DM-14** (`qc_attribute_mapping.csv`: `Data Use Codes` → `Dataset Data Use Codes`) and **DM-16**, all in one PR (Sonnet), then the **`v14.0.1` release** 🔒. DM-16 covers:<br>• drop `Not-Applicable` from Tumor Type<br>• `immunoassay` → `Immunoassay` in the JSON-LD<br>• change the `Publication Doi` description (and any other DOI description that names a prefix) to `https://doi.org/{doi}`<br>• **DUO (D-Q18, D-Q19):** change `modules/shared/duo.csv` `Properties` to the official shorthands (`USE`→`US`, `RST`→`RS`, `TSM`→`TS`, `IST`→`IS`, `GSR`→`GS`, `DSR`→`DS`); widen the `Dataset Data Use Codes` pattern to `^(DUO:\d{7,8}\|DUOPlus\d+\|Pending Annotation)$` (and any other DUO pattern in the model); optionally align `duo.csv` descriptions with governanceDUO's "Label - definition" form. | `scripts/check_json_schemas.py` finds 0 failures; the regenerated `json_schemas/` show only these diffs; `DUO:00000044` validates |
| B2 | **I2 verification and patch registration.** Read-only first: compare each registered 14.0.0 schema with `json_schemas/` at `v14.0.0`, ignoring `$id` and key order. Then register 14.0.1 with `make register-json VERSION=14.0.1 …` 🔒, and repeat the comparison against `v14.0.1`. | All registered schemas match. 14.0.1 is the highest version for every schema with a 14.x. |
| B3 | Bump `MODEL_REF` to `v14.0.1`: `portal_tables/model_contract.py` and the pipeline's `manifest_fields.py` (one commit each). Remove the pipeline's `Not-Applicable` workaround. | Tests pass. Vocab counts are logged: Tumor Type 186 → 185. |
| B4 | **Registry cleanup (R5).** A dry-run script prints Appendix C, refreshed live. You confirm the printed list 🔒, then it deletes one version at a time, recording deletions and refusals. Guards: never a version ≥ 14, nothing in the keep column, nothing from `AccessRequirementCA261841`, `AccessRequirementCA123`, `Landscape` or `Resource`. | `POST /schema/version/list` shows none of Appendix C's "Delete" versions, apart from logged refusals |
| B5 | Registry table `syn69735275`: add rows for 14.0.0 and 14.0.1, mark the older data-model versions `superseded`, and mark the deleted ones `deleted`. 🔒 | Rows present |

### Phase C: RecordSet spike in `syn78387305` (I3), which decides gate G1
Lay `syn78387305` out like one grant project, so D11's "find the RecordSet in the grant's data-type folder" logic runs unchanged. Bind on the RecordSets, not the project.

| Step | Task | Check / evidence for the report |
|---|---|---|
| C1 | 🔒 Create `syn78387305/{publications,datasets,tools}`. Create `MC2Center_PublicationView_RecordSet` and `MC2Center_DatasetView_RecordSet` with `create_record_based_metadata_task`, bound to 14.0.1 (14.0.0 if B1 isn't done yet). | Entity IDs logged |
| C2 | Load 20 real rows into each: one grant's `publicationview_…` / `datasetview_…` table, converted with `RENAMES_13_TO_14` and `normalize_headers(to="keys")`, with the I8.4 DUO mapping applied to the dataset rows. A Sonnet subagent prepares them as a dry run; Opus stores them. | Row counts |
| C3 | Answer I3's questions 1–6, with evidence. Question 1 includes: `get_schema` on each RecordSet returns the bound `$id`. | One paragraph per question |
| C4 | **Gate G1** (D-Q13): RecordSets if questions 1–3 pass, otherwise tables. Recorded in the report. | — |
| C5 | If G1 = RecordSets: unblock **D11**, **D12** (RecordSet mode) and **D15**; Sonnet implements, Opus reviews. `--execute` tests run only against `syn78387305`. | Checks in `impl_dcc_14_alignment.md` |
| C6 | D-Q5: run `create_grant_projects.py` once for a fake grant number. It creates a top-level project and team, so log their IDs and delete both afterwards. 🔒 | The project, team and wiki are created; a re-run reuses them |
| C7 | Rehearse the D-2 rollback here (section 2.1, step 1). 🔒 | The copy matches its export after rollback |
| C8 | Keep `syn78387305` as the scratch target through E4. Clear it after I9, with your go-ahead. 🔒 | — |

### Phase D: Portal and admin prep (after A6)
| Step | Task | Section 2 ref |
|---|---|---|
| D-1 | **I4b** Consortium table: add `consortiumId` and the 3 rows (Appendix B, once approved). 🔒 | 2.2 |
| D-2 | **I4** portal type changes, one table at a time, **verified right after each change**, with rollback ready (section 2.1, "How D-2 runs"). 🔒 | 2.1 SCHEMA rows |
| D-3 | **I8 worklists** (Haiku generates, a curator reviews): I8.1 enums plus the 7 DOI outliers; I8.2 assay-level values; I8.3 licenses; I8.4 DUO; I8.5 consortium. These are worklists only; they're applied in E1/E2. | 2.1 VALUE rows |

### Phase E: Migration and first sync
| Step | Task |
|---|---|
| E1 | **I5a** (G1 = RecordSets) or **I5b** (G1 = tables), one type at a time, with the I8 corrections. After each type, the union's row count and keys must equal the legacy union's. 🔒 |
| E2 | **I6**: regenerate the merged manifests and the `release_validation` files with 14.0.x headers. Remove `union_qc.py`'s `schematic` call. 🔒 |
| E3 | **I7** bindings: bind 14.0.1 to each RecordSet (I5a) or grant data-type folder (I5b), and to the admin manifests. The I1 refusals stay (D-Q8). 🔒 |
| E4 | **I9** first sync. First the B0 scratch-table write test (a copy of a portal table in `syn78387305`). Then each sync, one at a time, compared with `--output_csv` and checked on the portal. The dataset sync waits for W1. 🔒 |
| E5 | **I10** communication: curators and the people who run syncs. Lift the I0 holds. |
| E6 | **I11 archive** (only if G1 = RecordSets), after one full sync cycle with no regressions. Details below. 🔒 |

**E6 in detail (answer to your question about the `syn52750482` scope).** `MC2_All_Project_Tables` (`syn52750482`) is a table-only EntityView whose scope is 166 grant projects; it lists 538 tables today. Two things use it:
- `merge_tables.py` **legacy mode** reads it to find each grant's `<type>_synapse_storage_manifest_table` and builds the UNION MaterializedViews.
- `sync_grants.py` adds each new grant project to its scope (and to `syn27210848`'s).

Once the grant tables aren't used, **nothing needs to change in its scope, because the view itself is no longer needed.** RecordSet-mode `merge_tables.py` finds grants through the Grants table (`syn21918972`), not this view. So, in order:
1. 🔒 Create `CCKP - MC2 Admin/archive_13.1.0/<grantNumber>/` and **move** the 532 tables there (a parent change; IDs and version history are kept). Moving them out of the grant projects takes them out of the view's scope, so the view empties by itself, and nothing new appears in the grant projects or in All Data Files v2.
2. 🔒 Delete the per-type UNION MaterializedViews; they union tables that are no longer maintained.
3. Code: remove `syn52750482` from `sync_grants.py`'s `table_list`, keeping `syn27210848`. Remove `merge_tables.py`'s legacy mode and its `-s` view argument. Sonnet, one commit.
4. 🔒 After step 3 merges, delete `syn52750482`, with your go-ahead.
5. **No change to `syn27210848`'s scope.** `gen-mp-csv.py` still needs it to find grant data-type folders.

If G1 = tables, E6 doesn't happen: the tables stay live, and `syn52750482` keeps feeding the legacy-mode unions with no scope change.

### Phase F: Orchestrator (`orchestration/README.md`), after C5
The README's design holds, with these updates:
- **Phase 1 outputs:** the pipeline now writes template-exact manifests, `<stem>.pipeline.csv` sidecars and a `validation_report.csv`. Checkpoint 1 reviews the report and CSVs; `review_app.py` is on the deferred P4 branch.
- **Phase 2 is `annotations/upload_workflow.py` (D15)**, called once per component, with `--target` carrying G1. The README's known gaps become checks inside D15:
  - drop empty-link rows and empty split files before `create_entity_links`
  - check GEO accessions against the existing `datasets` folder before creating Dataset entities
  - set `SYNAPSE_AUTH_TOKEN` in the subprocess environment while the `schematic-table` target exists
- **Phase 3:**
  - Step 1 uses D12's RecordSet mode.
  - Step 2's validation comes from I6.
  - Step 4 asserts that a sample of known corrections persisted (#168).
- **The "publish" checkpoint (before uploading to `release_validation`)** shows that component's section 2.1 VALUE changes before they reach the CCKP.
- **Shape:** a stage driver with one `output/<date>/<stage>/` folder per stage, resuming from the last completed stage. Sonnet implements; Opus reviews.

### Later (not scheduled)
- P4 pipeline branch rebases (R3).
- DM-13 (CI gate), DM-15 (`GrantView Key` as a list).
- Missing upstream templates for ProjectView, Consortium, Institution and Theme.
- CLI consolidation (`plans/cckp_metadata_and_cli_consolidation.md`).
- **governanceDUO follow-ups** (that repo, not this plan):
  - NPOA's `meaning: DUO:0000044` points at an IRI DUO doesn't define; the real term is `DUO:00000044`.
  - Its LinkML model is only on `synapse-curator-infra`. Once that merges to `main`, re-run `utils/sync_duo_terms.py` against `main`.

---

## 6. Risks and known issues
- **A section 2.1 change before the code merges** would break the crons and the syncs on `main`. Gate: Phase D waits for A6.
- **Bindings below the top two levels:** I1's own check missed some. B4 relies on Synapse refusing to delete bound versions (it did in I1), and records every refusal.
- **Grant creation has never run live** until C6.
- **Dataset access text changes on every row** (D-Q17). Anything that matches `accessInformation` strings (saved portal searches, downstream filters) has to be checked against Appendix A.
- **Known live-data issues, logged and left (D-Q12):** education `dataType` holds synIDs; one dataset title has a corrupted thin space; tools `portalDisplay` is written as `"true"` into a `BOOLEAN` column.

---

## Appendix A: DUO access texts the portal will show (W1, approved 2026-10-26)

Generated from governanceDUO `868f184` (`linkml/mixins.yaml`, `DataUseModifierEnum`) into `portal_tables/duo_terms.csv` by `utils/sync_duo_terms.py`. The rule: governanceDUO's description, with the curator guidance ("If providing this term…") removed. **Wording changes go to governanceDUO**, then re-run the script; don't edit them here. Rows in use on the live portal today are marked. "Old text" is the hand-written paraphrase this replaces.

| Code | Short | Live use | Portal text (new) | Old text |
|---|---|---|---|---|
| DUO:0000042 | GRU |  | General Research Use - This data use permission indicates that use is allowed for general research use for any research purpose. | Data access is allowed for any research purpose |
| DUO:0000027 | PS |  | Project Specific Restriction - This data use modifier indicates that use is limited to use within an approved project. | Data access is limited to use within an approved project |
| DUO:0000016 | GSO |  | Genetic Studies Only - This data use modifier indicates that use is limited to genetic studies only (i.e., studies that include genotype research alone or both genotype and phenotype research, but not phenotype research exclusively) | Data access is limited to genetic studies only |
| DUO:0000029 | RTN | 5 datasets (RTN) | Return to Database or Resource - This data use modifier indicates that the requestor must return derived/enriched data to the database/resource. | Derived/enriched data must be returned to the database/resource repository |
| DUO:0000043 | CC |  | Clinical Care Use - This data use modifier indicates that use is allowed for clinical use and care. | Data access is allowed for clinical use and care |
| DUO:0000015 | NMDS |  | No General Methods Research - This data use modifier indicates that use does not allow methods development research (e.g., development of software or algorithms). | Data may not be used for methods development research |
| DUO:0000004 | NRES |  | No Restriction - This data use permission indicates there is no restriction on use. | There are no restrictions on the use of this data |
| DUO:0000045 | NPU |  | Not-for-Profit Organisation Use Only - This data use modifier indicates that use of the data is limited to not-for-profit organizations. | Data access is limited to not-for-profit organizations |
| DUO:0000017 | DUM |  | Data use modifiers indicate additional conditions for use. | Additional conditions apply to data use |
| DUO:0000011 | POA |  | Population Origins or Ancestry Research Only - This data use permission indicates that use of the data is limited to the study of population origins or ancestry. | Data access is limited to the study of population origins or ancestry |
| DUO:0000046 | NCU |  | Non-Commercial Use Only - This data use modifier indicates that use of the data is limited to not-for-profit use. | Data access is limited to not-for-profit use |
| DUO:0000018 | NPUNCU | 5 datasets (NPUNCU) | Not-for-Profit, Non-Commercial Use Only - This data use modifier indicates that use of the data is limited to not-for-profit organizations and not-for-profit use, non-commercial use. | Data access is limited to not-for-profit organizations and not-for-profit/non-commercial use |
| DUO:0000044 | NPOA |  | Population Origins or Ancestry Research Prohibited - This data use modifier indicates use for purposes of population, origin, or ancestry research is prohibited. | Data may not be used for population, origin, or ancestry research |
| DUO:0000021 | IRB |  | Ethics Approval Required - This data use modifier indicates that the requestor must provide documentation of local IRB/ERB approval. | Requestor must provide documentation of local IRB/ERB approval to access data |
| DUO:0000006 | HMB |  | Health or Medical or Biomedical Research - This data use permission indicates that use is allowed for health/medical/biomedical purposes; does not include the study of population origins or ancestry. | Data access is allowed for health/medical/biomedical research, not including the study of population origins or ancestry |
| DUO:0000019 | PUB |  | Publication Required - This data use modifier indicates that requestor agrees to make results of studies using the data available to the larger scientific community. | Requestor must make results of studies using the data available to the scientific community |
| DUO:0000026 | US |  | User Specific Restriction - This data use modifier indicates that use is limited to use by approved users. | Data access is limited to approved users |
| DUO:0000020 | COL |  | Collaboration Required - This data use modifier indicates that the requestor must agree to collaboration with the primary study investigator(s). | Requestor must agree to collaborate with the primary study investigator(s) |
| DUO:0000012 | RS |  | Research Specific Restrictions - This data use modifier indicates that use is limited to studies of a certain research type. | Data access is limited to use in specific types of research |
| DUO:0000025 | TS |  | Time Limit on Use - This data use modifier indicates that use is approved for a specific number of months. | Data access is limited to a specific period of time |
| DUO:0000024 | MOR |  | Publication Moratorium - This data use modifier indicates that requestor agrees not to publish results of studies until a specific date. | Requestor must not publish results of studies using the data before a specified date |
| DUO:0000028 | IS |  | Institution Specific Restriction - This data use modifier indicates that use is limited to use within an approved institution. | Data access is limited to use within approved institutions |
| DUO:0000022 | GS |  | Geographical Restriction - This data use modifier indicates that use is limited to within a specific geographic region. | Data access is limited to use within specific geographic regions |
| DUO:0000007 | DS |  | Disease Specific Research - This data use permission indicates that use is allowed provided it is related to the specified disease. | Data access is limited to research related to specific diseases |
| DUOPlus1 | — |  | Source geography is relevant to governance decisions. | Source geography is relevant to governance decisions |
| DUOPlus2 | — |  | Study population is relevant to governance decisions. | Study population is relevant to governance decisions |
| DUOPlus3 | — |  | Data deidentification is relevant to governance decisions. | Data deidentification is relevant to governance decisions |
| DUOPlus4 | — |  | A data permission designation is associated with the Study. | A data permission designation is associated with the Study |
| DUOPlus5 | — |  | A data tier designation is associated with the Study. | A data tier designation is associated with the Study |
| DUOPlus6 | — |  | A license is associated with the Study. | A license is associated with the Study |
| DUOPlus7 | — |  | Attribution conditions are associated with this Study. | Attribution conditions are associated with this Study |
| Pending Annotation | — | ~1,131 datasets | The data use modifier for this record has not yet been annotated. | Access information was not provided for this dataset |

The dataset sync also accepts data-models' shorthands (`USE`, `RST`, `TSM`, `IST`, `GSR`, `DSR`) and NPOA's official ID `DUO:00000044`, which resolve to the same rows.

## Appendix B: new Consortium rows (W2, approved 2026-10-26)

These go into Consortium `syn36345930`, alongside the new `consortiumId` column. `consortiumName` follows the existing lowercase convention (`csbc`, `nci_cts`). Edit any cell in place, or add `[REVIEW: …]`.

| consortiumId | component | consortiumName | consortiumDisplayName | consortiumFullName | consortiumDescription | consortiumFundingAgency | website |
|---|---|---|---|---|---|---|---|
| `program.htan` | Consortium | `htan` | HTAN | Human Tumor Atlas Network | The Human Tumor Atlas Network (HTAN) constructs 3-dimensional atlases of the dynamic cellular, morphological, and molecular features of human cancers as they evolve from precancerous lesions to advanced disease. | NIH | https://humantumoratlas.org/ |
| `program.nci` | Consortium | `nci` | NCI | National Cancer Institute | Grants and resources funded directly by the National Cancer Institute (NCI) outside a named MC2 Center consortium. | NIH | https://www.cancer.gov/ |
| `program.sage-bionetworks` | Consortium | `sage_bionetworks` | Sage Bionetworks | Sage Bionetworks | Sage Bionetworks is a nonprofit biomedical research and technology organization that coordinates the MC2 Center and builds the Synapse platform. | (blank) | https://sagebionetworks.org/ |

## Appendix C: registry cleanup list (B4, R5)

Generated read-only on 2026-10-26 from `MC2Center`: 43 schemas, 129 versions. B4 regenerates it live before deleting. **Rule:** for each schema with a 14.x version, keep the newest version below 14 and delete the older unbound ones. Delete every unbound `StudyAR` version. Synapse refuses bound versions, and those stay.

| Schema | Delete | Keep (previous release) |
|---|---|---|
| `Biospecimen` | 13.0.0 | 13.1.0 |
| `DataCatalog` | 12.0.1–12.0.8 (8) | 13.0.0 |
| `DataDSP` | 12.0.0, 13.0.0 | 13.1.0 |
| `DatasetView` | 13.0.0 | 13.0.1 |
| `FileView` | 13.0.0–13.0.6 (7) | 13.0.7 |
| `Model` | 13.0.0 | 13.1.0 |
| `PublicationView` | 1.0.0, 1.1.1, 1.1.9, 13.0.0 | 13.0.1 (15.0.0 stays, D-Q8) |
| `SequencingRNALevel1` | 13.0.0, 13.0.1, 13.0.2, 13.0.8 | 13.0.9 (15.0.0 and 15.0.1 stay) |
| `Study` | 2.0.0–2.0.7 (8), 3.0.0, 4.0.0, 13.0.0, 13.1.0 | 13.1.1 |
| `StudyAR` | 13.0.0, 13.1.0, 13.2.0, 13.3.0 (and 9.0.0, which Synapse will refuse: it's bound) | 15.0.0 stays (bound) |

**Total:** 45 candidates; about 44 deletions expected. **Untouched:** every other schema has only its 13.0.x plus 14.x and loses nothing. `AccessRequirementCA261841` (O9), `AccessRequirementCA123` (D-Q8), `Landscape` 3.x and `Resource` 1.x (no 14.0.0 version) are not touched.
