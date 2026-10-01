# Implementation plan: mc2-center-dcc alignment with data model 14.0.0

**Repo:** `mc2-center/mc2-center-dcc`, branch `model-alignment`. B0 has landed and is hardened: a column check before truncating, and a restore if the upload fails. The OOP-migration review fixes are merged in from `synapse-client-oop-refresh`.
**Master plan:** `plans/cde_model_alignment_harmonized.md` (decisions 1–5, O1–O10). Synapse writes and migrations live in `plans/impl_synapse_14_infrastructure.md`. The curation pipeline is in `plans/impl_ai_curation_pipeline_14.md`.
**Deadline:** merge before **2026-11-01**, the first monthly cron after #262.

## Ground rules for every task
- **Model pin.** Every reference to data-models goes through `MODEL_REF` (task D1). Until the tag exists, pin to `c234c467`, then bump to `v14.0.0` in one commit. Never point at `main`.
- **Header contract (decision 5).** Manifests and templates use display names (`GrantView Key`). Schemas and RecordSets use class labels (`GrantViewKey`). Convert between them only through `model_contract.normalize_headers()`. Never strip spaces by hand, and never keep a hand-written column list where a template can be read instead.
- **Portal tables keep their camelCase names (decision 1).** The crosswalk (D2) is the only place that maps to them.
- **Test before committing.** `python -m py_compile` on every touched file. Run the task's own check, and `pytest tests/` once D1 adds the suite.
- **Commits.** One commit per task. The message ends with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- **What a task may not do:** subagents **don't** push, open PRs, or write to Synapse outside `--dryrun`. Any live write is a separate, approved step in the infrastructure plan.
- **Tiers:** Sonnet unless a task says otherwise. Opus reviews each diff against its spec.

## Dependency graph
```
D1 ─┬─ D2 ─┬─ D3, D4, D5, D6, D7   (sync scripts, parallel)
    │      └─ D12 (merge_tables union)
    ├─ D8 (hand-off: split / processing-splits)  ── pairs with pipeline P1
    ├─ D9 (other annotation/utils renames)
    └─ D11 (RecordSet upload path)  ← blocked on infra I3 (RecordSet spike)
    └─ D16 (manifest readers for tables/RecordSets) ← before infra I5a
D10 (cron --dryrun), D13 (csv_to_ttl), D14 (register entry point): independent
D15 (upload-workflow driver): after D8 (and D11, if RecordSets go ahead)
```

---

### D1. `portal_tables/model_contract.py`: the single model-contract module
- **Files.** New `portal_tables/model_contract.py`. New `tests/test_model_contract.py`, with small vendored fixtures under `tests/fixtures/`. New `requirements-dev.txt` (`pytest`).
- **Spec:**
  - `MODEL_REF = "c234c467"`, with a comment saying to bump it to `v14.0.0` once tagged. `RAW = f"https://raw.githubusercontent.com/mc2-center/data-models/{MODEL_REF}"`.
  - `fetch(path) -> Path`: downloads `RAW/path` once into a local cache (`~/.cache/mc2-center-dcc/<MODEL_REF>/`) and returns the cached path. `MC2_MODEL_DIR` overrides the cache with a local data-models checkout.
  - `label_map() -> dict[str, str]`: display name → class label, from `mc2.model.jsonld` (`sms:displayName` → `rdfs:label`). It raises if one display name maps to more than one label. **Verified on `c234c467`:** every header in all 33 `templates/*.csv` maps to exactly one schema key (`DatasetView_id` → `DatasetViewId`, `Dataset Data Use Codes` → `DatasetDataUseCodes`). `Component` has no label; pass it through unchanged.
  - `normalize_headers(df, to="keys" | "display") -> DataFrame`: renames in either direction. **Unknown headers raise `ValueError`.** The only exceptions are a passthrough set: `Component`, `Id`, `entityId`, and any portal-only enrichment column the caller lists.
  - `template_headers(component) -> list[str]`: the ordered header row of `templates/<component>.csv`.
  - `duo_id_to_code() -> dict[str, str]`: from `modules/shared/duo.csv`, mapping `Attribute` (`DUO:0000042`) to `Properties` (`GRU`).
  - `ALL_VALID_VALUES_URL = f"{RAW}/all_valid_values.csv"`.
  - `RENAMES_13_TO_14`: a per-component dict of old → new display names, taken from the 13.1.0 vs 14.0.0 template diff:

    | Component | Old header | New header |
    |---|---|---|
    | PublicationView | `Publication Assay` | `Assay` |
    | PublicationView | `Publication Tumor Type` | `Tumor Type` |
    | PublicationView | `Publication Tissue` | `Tissue` |
    | DatasetView | `Dataset Assay` | `Assay` |
    | DatasetView | `Dataset Species` | `Species` |
    | DatasetView | `Dataset Tumor Type` | `Tumor Type` |
    | DatasetView | `Dataset Tissue` | `Tissue` |
    | DatasetView | `Data Use Codes` | `Dataset Data Use Codes` |
    | ToolView | `Tool License` | `License` |
    | EducationalResource | `Resource License` | `License` |
    | GrantView | `Grant Investigator` | `Investigator` |
    | GrantView | `Grant Consortium Name` | `Consortium Key` (values change: `program.*` IDs) |
    | PersonView | `Person Consortium Name` | `Consortium Key` |

    New in 14.0.0, with no 13.1.0 equivalent: GrantView `Institution Key` and `PersonView Key`; PersonView `Institution Key`. These are used by the migration (infrastructure plan I5) and by the legacy-input fallback in D8.
  - `LEGACY_ADMIN_HEADERS`: today's admin manifests don't use 13.1.0 template headers, so `normalize_headers` would reject them. Map them explicitly:
    - **PersonView syn38301033 (camelCase):** `personGrantNumber` → `GrantView Key`, `personConsortiumName` → `Consortium Key`, `name` → `Name`, `alternativeNames` → `Alternative Names`, and so on. Build the full map from the live columns, read-only.
    - **Project syn59074382:** `Project Investigator` → `Investigator`, `Project Consortium Name` → `Consortium Key`, `Project Grant Number` → `GrantView Key`.
  - **There's no `templates/ProjectView.csv`** at `c234c467`; there are 33 templates, and ProjectView, Consortium, Institution and Theme have none. For those components, `template_headers()` falls back to the schema's property order mapped back to display names. The missing templates are reported upstream (data-models follow-up).
- **Importing it from `annotations/` and `utils/`.** Those scripts add `portal_tables` to `sys.path` with a three-line shim, commented `# removed when the mc2dcc package (CLI consolidation plan) lands`. `model_contract` is a unique module name, so this doesn't collide with the `utils/` package or `portal_tables/utils.py`.
- **Checks.** `pytest tests/test_model_contract.py`:
  - a round trip of display → keys → display over all 33 vendored template headers
  - unknown headers raise
  - `DatasetView_id` → `DatasetViewId`
  - DUO map: `DUO:0000042` → `GRU`
- **Commit:** `Add model_contract module pinned to data-models 14.0.0`.

### D2. `portal_tables/crosswalk.py`: schema keys to portal columns, with consortium lookup
- **Depends on** D1.
- **Spec:**
  - `CROSSWALK[component]`: an ordered list of `(source, portal_column, kind)`.
    - `source` is a class-label key, or `derived:<name>` for enrichment columns the sync script computes (`theme`, `consortium`, `grantName`, `link`, `iconTags`, `version`, `sourceRepository`, `downloadType`, `downloadSynId`, `pub`, and so on).
    - `kind` is one of `string`, `list` or `int`.
    - Build it from each sync script's current `col_order` and the live column lists (`synapse_cols.json` in the session scratchpad, re-fetched read-only). It covers the 7 portal tables.
  - `check_crosswalk(syn, table_id, component)` asserts that the live column names and order equal the crosswalk's `portal_column` list. It tolerates trailing live columns listed in `utils.OPTIONAL_TRAILING_COLUMNS`, and crosswalk entries marked `optional` (e.g. `projectShortName`) that don't exist yet. It returns `{portal_column: live_column_type}`.
  - `to_portal(df, component, live_types)` renames to the portal columns, orders them, and converts `list`-kind columns **only when the live type is `STRING_LIST`**. It converts only string cells; cells that are already lists (from datasets, people or RecordSet input) pass through. `utils.convert_to_stringlist` uses `.str` and would turn them into NaN. That keeps the code correct both before and after the infrastructure plan's I4 type changes (license, investigator).
  - `portal_tables/utils.py`:
    - `update_table` gets an optional `component` argument. If given, it calls `check_crosswalk` instead of B0's positional rename. B0 stays as the fallback when `component` is None.
    - Add `consortium_display_names(syn, ids) -> list[str]`, reading the Consortium table. Find its ID in the portal project, read-only, and record it in `CONFIG`. It raises on an unknown `program.*` ID.
  - **Replaces** `annotations/attribute_dictionary.py`. Delete it once D9 has moved its importers to the crosswalk.
- **Checks:**
  - `check_crosswalk` passes against all 7 live tables (read-only).
  - A unit test: a `list` column against a live `STRING` type stays a string; against `STRING_LIST` it becomes a list.
- **Commit:** `Add portal crosswalk and consortium lookup`.

### D3–D7. Sync scripts (parallel after D2; same pattern)
**Pattern for each script:**
1. Read the manifest.
2. `normalize_headers(df, to="keys")`, replacing `manifest.columns.str.replace(" ", "")`.
3. Use class-label keys internally.
4. Convert the output with `crosswalk.to_portal(...)`.
5. Write with `utils.update_table(syn, table_id, df, component=...)`.
6. Delete the script's hardcoded `col_order`.

**Check for each:** `--dryrun --noprint -o <scratch>` succeeds. The diff of its output CSV against the B0-era dry-run output in the session scratchpad (`b0/final_<type>.csv`) contains **only** the changes the task spec lists.

| Task | Script | Specific changes |
|---|---|---|
| D3 | `sync_publications.py` | `PublicationAssay`, `PublicationTumorType` and `PublicationTissue` become `Assay`, `TumorType`, `Tissue`. Replace positional `row[4]` (`:36`) with `row["GrantViewKey"]`. Drop the `"GrantView Key" → "Publication Grant Number"` rename; the crosswalk maps `GrantViewKey` to `grantNumber`. |
| D4 | `sync_datasets.py` | `DatasetAssay`, `DatasetSpecies`, `DatasetTissue` and `DatasetTumorType` become `Assay`, `Species`, `Tissue`, `TumorType`. `DataUseCodes` becomes `DatasetDataUseCodes`. `DatasetView_id` becomes `DatasetViewId`. **DUO (O10):** translate each code with `duo_id_to_code()` first; short codes pass through during the transition. Then look up `DUO_DICT`. Replace `except KeyError: continue` (`:131-134`) with a logged warning plus the fallback text `"Access information not mapped: <code>"`, and keep processing the row so the repository and download fields are still set. Add `DUO_DICT` entries, or the fallback, for `DUOPlus*`. Extra check: a fixture row with an unknown code still gets `sourceRepository` and `downloadType`. |
| D5 | `sync_tools.py`, `sync_education.py` | `ToolLicense` / `ResourceLicense` become `License` (`list` kind). Any other class-label input keys come from normalization. |
| D6 | `sync_grants.py`, `create_grant_projects.py` | `Grant Investigator` becomes `Investigator` (`list` kind). `Grant Consortium Name` becomes `ConsortiumKey`, resolved with `consortium_display_names()` into the `consortium` column (display names, decision 2). The grant manifest is a table (syn53259587) with display-name columns today; normalize it the same way. Recheck the `updated_scope` handling on the OOP `EntityView` path. |
| D7 | `sync_projects.py`, `sync_people.py` | Both manifests are tables with non-template headers today, so normalize them through `LEGACY_ADMIN_HEADERS` (D1) until infrastructure step I5 regenerates them. Projects: `ProjectInvestigator` becomes `Investigator`. `ProjectGrantNumber` becomes `GrantViewKey`. Consortia come from `ConsortiumKey` via the lookup. `ProjectShortName` goes to `projectShortName`, mapped only once that column exists (infrastructure plan I4); until then the crosswalk marks it `optional`. People: `personConsortiumName` becomes `ConsortiumKey`, through the lookup. `personGrantNumber` becomes `GrantViewKey`. Leave `InstitutionKey` unmapped for now; there's no portal column for it. |

### D16. Manifest readers that work for tables and RecordSets
- **Depends on** D1. **Needed before** infrastructure step I5a points `CONFIG` at RecordSets.
- **Why.** `sync_people.py`, `sync_projects.py`, `sync_grants.py` and `create_grant_projects.py` read their manifests with `Table(id).query`, but a RecordSet can't be queried with SQL.
- **Spec:** add `model_contract.read_manifest(syn, entity_id) -> DataFrame`.
  - **Table:** `Table.query(include_row_id_and_row_version=False)`.
  - **RecordSet:** download its CSV.
  - **File:** read the CSV, as the publications, datasets, tools and education syncs do now.
  - All sync scripts call it.
- **Check:** unit tests with a mocked entity of each type.

### D8. Hand-off from the AI curation pipeline (`annotations/split_manifest_grants.py`, `annotations/processing-splits.py`)
- **Depends on** D1. **Pairs with pipeline task P1;** test both against the same fixture.
- **`split_manifest_grants.py`:**
  - The CV URL becomes `model_contract.ALL_VALID_VALUES_URL`.
  - Split publications on `GrantView Key`. Fall back to the legacy `Publication Grant Number`, with a deprecation warning.
  - Accept `.xlsx` input by reading the `manifest` or `Sheet1` sheet, so the tools output no longer needs converting by hand.
  - Keep the comma split and the grant-name sanitizing as they are.
- **`processing-splits.py`:**
  - Take the column order from `model_contract.template_headers(component)` instead of the 4 hardcoded lists.
  - Before ordering: apply `RENAMES_13_TO_14[component]` to any legacy headers, with a warning. Rename `<Resource> Grant Number` to `GrantView Key`. Fill `PublicationView_id` from `Pubmed Id`. Add any missing template column as empty.
  - **Log every column it drops**, which covers the `_download_status`, `_download_source`, `_pdf_path`, `_parse_error`, `_tool_availability_text` and `keywords` columns the pipeline writes. Don't drop silently as `df[col_order]` does now.
  - Keep the 500 → 400-character "(Read more on Pubmed)" cap, but **don't** apply it to ID or key columns.
  - Add argparse (`input_csv` from gen-mp-csv) instead of `sys.argv[1]`.
- **Checks:**
  - Using a fixture of today's pipeline output (old headers plus `_…` columns) **and** a fixture of the P1 output (14.0.0 headers): both produce files whose header row equals `templates/<component>.csv` exactly.
  - The drop log lists the bookkeeping columns.
- **Commit:** one per script.

### D9. Other annotation and utility scripts
- **Depends on** D1 and D2.
- **Files:**
  - `annotations/edit_legacy_annotations.py`: pin the CV URL (`:41`) via `model_contract`.
  - `annotations/schema_update.py`: column names via `RENAMES_13_TO_14`. The migration (infrastructure plan I5) also uses it.
  - `utils/merge_and_correct_manifests.py`, `utils/table_to_annotations.py` (also fix `:525` `keys_to_drop=None` → `[]`).
  - `utils/check_publications_status.py`:
    - `Publication Grant Number` becomes `GrantView Key`.
    - Replace `PUBLICATION_DICT` with the crosswalk.
  - Delete `annotations/attribute_dictionary.py`.
- **`annotations/add_cols.py`:** the CLI plan flags it as possibly dead. **Don't edit it**; list it in the report for the owner to decide.
- **Check:** `py_compile`. `grep -rnE "Publication Assay|Dataset Assay|Tool License|Resource License|Grant Investigator|Grant Consortium Name|Person Consortium Name"` over the tracked `.py` files returns only `RENAMES_13_TO_14` and the D8 legacy fallback.

### D10. Cron scripts get `--dryrun`
- **Files:** `utils/tally_themes.py`, `utils/check_publications_status.py`.
- **Spec:** `--dryrun` computes everything, prints a summary and writes local CSVs. It skips `update_table`, the File upload and `sendMessage`. The workflows keep calling them without the flag.
- **Check:** both run cleanly with `--dryrun` against live Synapse (read-only).

### D11. RecordSet upload path: `annotations/upload_recordsets.py`
- **Blocked on** infrastructure plan **I3** (the RecordSet spike) confirming that versioned CSV upserts keep the binding, the curation task and the Grid intact, and that validation re-runs.
- **Depends on** D1.
- **Tier:** Sonnet, with Opus reviewing the spike-dependent behavior.
- **Spec,** for each `(grant file, component)` from the gen-mp-csv CSV:
  1. `normalize_headers(to="keys")`.
  2. Find `MC2Center_<Component>_RecordSet` in the grant's data-type folder. If it's missing, **stop and report**; creation is infrastructure step I5.
  3. Download the current CSV and upsert by the component's primary key (`<Component>Id`; for publications `PublicationViewId` = PMID).
  4. Store a new version.
  5. Poll `validation_summary` and `get_detailed_validation_results`, and write a per-grant validation report.
  6. **Default is `--dryrun`**: compute the merged CSV and the diff without storing. A live run needs `--execute`.
- **Replaces** `upload-manifests.py`'s `schematic` submit for the components that have moved to RecordSets. Leave `upload-manifests.py` in place until the migration is complete, with a deprecation notice.
- **Check:** against the spike's scratch project only (infrastructure plan I3): a dry run, then an `--execute` run on scratch. The validation report matches a hand-checked fixture.

### D12. `portal_tables/merge_tables.py`: union from RecordSets, and bug fixes
- **Depends on** D1 and D2. **The RecordSet mode waits on** I3.
- **Fix in the existing RecordSet path:**
  - `SELECT 'grantId'`, a string literal, becomes `SELECT grantId`.
  - Skip grants whose folder or RecordSet is missing, with a log line, instead of calling `RecordSet(id=None)`.
  - Remove the per-grant mirror tables. Stop calling `Table(name=…).store()` and `store_rows(INFER_FROM_DATA)`, which appends a duplicate copy every run and infers different types per grant.
- **New RecordSet mode:**
  - Read each grant's RecordSet CSV and concatenate them in pandas.
  - `normalize_headers(to="display")`, so that `union_qc.py` and `qc_attribute_mapping.csv` keep working unchanged.
  - Write `output/<Component>_UNION.csv`.
  - Optionally store it as a single table with a fixed schema from the crosswalk, behind `--publish`. That's a live write, so it's infrastructure step I6.
- **Legacy table mode** (grant tables → MaterializedView) stays until the migration is complete.
- **Check:** RecordSet mode against the scratch project gives the expected row count and template headers. Legacy mode is unchanged (dry-run the query string only).

### D13. `utils/csv_to_ttl.py`
- Remove the `10x_` stripping (lines 258-259 and 476).
- Make the `is_enum` check consistent (lines 196 and 199).
- Smoke-test it on `mc2.model.csv` at `MODEL_REF`, and diff the triple count against 13.1.0. The differences should be explained by the release notes.

### D14. Registration entry point
- **Spec:** a tracked `Makefile` target, `register-json VERSION=14.0.0 SCHEMAS=<path to data-models json_schemas at v14.0.0>`. It loops over `utils/synapse_json_schema_bind.py -p <file> -n MC2Center -v <VERSION> --no_bind`. It replaces the untracked `local_inputs/Makefile`.
- **Not run here.** Running it is infrastructure step I2.
- **Check:** `make -n register-json …` prints the 38 commands.

### D15. Replace `annotations/upload-workflow.sh`
- **Depends on** D8, and D11 if RecordSets are adopted.
- **Spec:** a parameterized driver, `annotations/upload_workflow.py`, with `--manifest`, `--type`, `--out-dir` and `--target {recordset,schematic-table}`. It runs split → gen-mp-csv → processing-splits → `create_entity_links` (for datasets, tools and education) → upload. No hardcoded file names or personal config paths. It replaces the broken `.sh`, which passes a folder where `processing-splits.py` expects a CSV.
- **Later home:** this becomes `mc2dcc annotations upload-workflow` in the CLI plan.
- **Check:** a dry run end to end on the D8 fixtures.

## Done when
- D1–D10 and D13–D14 are merged on `model-alignment`.
- D11, D12 (RecordSet mode) and D15 are merged, or explicitly deferred, depending on the I3 result.
- All dry runs and tests pass.
- An independent code review has run before `gh pr create`; opening the PR needs the owner's go-ahead.
- An implementation report is saved at `plans/impl_dcc_14_alignment_report.md`.
