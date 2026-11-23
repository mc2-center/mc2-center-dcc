# CCKP DCC pipeline runbook

One cycle: the AI curation pipeline produces manifests -> `upload_workflow.py` loads them into per-grant RecordSets
(`MC2Center_<Component>_RecordSet`) -> curators edit them in Synapse Curator -> `union_qc.py --regenerate` rebuilds the
merged manifests -> the sync scripts write the staging portal tables -> after validation, `promote_staging.py` copies
the staging tables to the production portal tables. Every script that writes to Synapse is a dry run unless you pass
`--execute` (`--upload` for `union_qc.py`). Run everything from the repo root.

## Before you start

- Python 3.11 (what CI uses) in the conda env `mc2-center-dcc`:
  `conda create -n mc2-center-dcc python=3.11 && conda activate mc2-center-dcc && pip install -r requirements.txt`
  (`requirements.txt` includes `requirements-ci.txt`; `requirements-dev.txt` adds pytest). `geo-synapse`, a private
  repo, is only needed by `annotations/create_entity_links.py` (the `links` stage).
- Synapse login: `export SYNAPSE_AUTH_TOKEN=<personal access token>` (or a `.synapseConfig`). Without either,
  `utils.syn_login()` prompts for a token. Give the token the view, download and modify scopes. The account must be
  a member of the MC2 Admin team, which gives write access to the grant projects, the CCKP - MC2 Admin project (it
  holds the staging portal tables), the log tables and the production portal tables (step 7 only).

## The cycle

**1. AI curation pipeline (separate repo, `ai-curation-pipeline`).** Run `run-publication-pipeline`, then QC the output
with its `review_app.py`. The result is one manifest CSV per component (`PublicationView`, `DatasetView`, `ToolView`,
`EducationalResource`). Checkpoint: QC sign-off before step 2.

**2. Load into the grant RecordSets** (`annotations/upload_workflow.py`, once per component and manifest):

```bash
python annotations/upload_workflow.py --manifest publication.csv --type PublicationView \
    --out-dir runs/2026-11-publications                                         # dry run
python annotations/upload_workflow.py --manifest publication.csv --type PublicationView \
    --out-dir runs/2026-11-publications --execute                                # write
```

Stages `split` -> `paths` -> `reshape` -> `links` (DatasetView/ToolView only; creates Synapse entities, so only with
`--execute`) -> `upload`; each writes `<out-dir>/<NN>_<stage>/` and `<out-dir>/run.json`. `--write-log-table` and
`--removals-table` default to the production tables `syn78404341` and `syn78404342`; a dry run reads them and
writes nothing to them. Resume a failed run with `--from-stage <stage>`; a different manifest or type in a used `--out-dir`
needs `--force-new` (wipes the stage folders). Per-grant results are in
`<out-dir>/05_upload/<run-id>/<grant>/` (`summary.txt`, `decisions.csv`, `conflicts.csv`, `removal_skipped.csv`,
`merged.csv`, and `validation.csv` with `--execute`); `summary.csv` lists every grant.
Check: no grant in `summary.csv` is `failed`, `missing_recordset`, `locked`, `retry` or `verification_mismatch`;
read `conflicts.csv` (cells a curator owns are never overwritten by the upload) and `validation.csv`; in a dry run
read `decisions.csv` and `merged.csv`. Run the dry run first and proceed to `--execute` only once it is clean.
Also check `04_links/dropped_rows.csv` and `04_links/reused_entities.csv`.
The upload can also be run alone: `annotations/upload_recordsets.py` (`--paths-csv`, `--component`, `--report-dir`,
`--write-log-table`, `--removals-table`, `--execute`).

**3. Curation in Curator.** Grant metadata is curated in each grant project's `MC2Center_<Component>_RecordSet`
(in its data-type folder), through the curation tasks / Grid. To fix records, see [Fixing records](#fixing-records). Admin metadata (grants, projects, people) is curated
in its own RecordSets (`MC2Center_GrantView_RecordSet`, `MC2Center_ProjectView_RecordSet`,
`MC2Center_PersonView_RecordSet`, folder `admin_recordsets` of CCKP - MC2 Admin, `syn28742843`), never through the
per-run upload.

**4. Row removals.** A Grid import cannot delete rows, so removals go through the removals table `syn78404342`:

```bash
python annotations/remove_recordset_rows.py request --removals-table syn78404342 \
    --recordset syn... --key <value per upsert key column> [--key ...] --reason "..." [--execute]
python annotations/remove_recordset_rows.py --removals-table syn78404342 --write-log-table syn78404341 \
    --report-dir runs/removals [--recordset syn...]                                  # dry run
python annotations/remove_recordset_rows.py ... --execute                             # write
```

`request` appends `requested` rows (dry run unless `--execute`); give one value per upsert key column, in key order,
and repeat `--key` per row. The run (default `--run-id` is `dcc-removal-<timestamp>`) stores a new RecordSet version
without those rows, re-validates through Grid, and logs `removed` in `syn78404341` and the removals table. Later
uploads skip removed keys; set a key's status to `restored` in the removals table to lift a removal. Check: the dry
run lists the expected keys and no `not_found`; the summary shows no `failed`, `locked`, `blocked` or `retry`
RecordSet (`blocked` = a Grid session is open on the RecordSet; nothing was written).

**5. Regenerate the merged manifests** (`portal_tables/union_qc.py`):

```bash
python portal_tables/union_qc.py --regenerate --output-dir output/i6                # dry run
python portal_tables/union_qc.py --regenerate --output-dir output/i6 --upload       # LIVE WRITE
```

Builds each component's union from the grants' RecordSets (grants from the staging Grants table), merges rows by alias key
(publications and datasets only; tools and education keep one row per grant unless `--merge-components` names them),
fills empty cells, computes `iconTags` (publications and datasets), validates against the 14.x JSON Schemas, and writes
`<output-dir>/<Component>/<Component>_merged_manifest.csv`, `validation.csv`, `validation_counts.csv`,
`key_corrections.csv`, `id_conflicts.csv`, `key_conflicts.csv`, `report.txt`, plus `<output-dir>/summary.txt`. `--components` limits it.
`--upload` stores each manifest as a new version of the current manifest file (publications `syn53478776`, datasets
`syn53478774`, tools `syn53479671`, education `syn53651540`); it is refused with `--input-dir` or `--grant-projects`,
and while any alias has different ids in different grants (`id_conflicts.csv`) or any unmerged id is twice in one
grant or on different names (`key_conflicts.csv`): fix in the grant RecordSets and regenerate. Invalid rows are kept and reported, never dropped. Checkpoint: review the validation report
(`validation_counts.csv`, `report.txt`, `id_conflicts.csv`) before `--upload`; `--upload` needs sign-off.
Write each option in full: `union_qc.py` does not accept abbreviations. The old flags `-c`, `-t` and `-bl` are
removed; a command with them stops with a usage error.

**6. Sync to the staging portal tables.** `run_sync.sh` runs the publication, dataset, tool and education syncs,
reading the merged manifests above (the same script runs in the `Sync manifests to portal tables` GitHub workflow,
which is `workflow_dispatch` only). Sync publications before datasets: the dataset sync reads the publications portal
table for each linked publication's title, year and DOI. `run_sync.sh` keeps this order.

```bash
bash run_sync.sh --dryrun     # extra arguments go to every script
bash run_sync.sh              # write the staging portal tables
```

The syncs read and write only the staging tables (the `STAGING ...` tables in CCKP - MC2 Admin, `syn28742843`).
They refuse a production table id. Each script also takes `--output_csv` (default `./final_<resource>_table.csv`;
`run_sync.sh` passes `--noprint`, so none is written), `-v`, `-m/--manifest_id` and `-t/--portal_table_id`; to compare
a dry run to the staging table, run one directly:
`python portal_tables/sync_publications.py --dryrun -o final_publication_table.csv`. The grant, project and people
syncs are not in `run_sync.sh`; run them directly when the admin RecordSets change (they read the admin RecordSets;
`--manifest-source table` reads the legacy manifest tables instead):
`python portal_tables/sync_grants.py|sync_projects.py|sync_people.py [--dryrun]`. Sync grants first: the other syncs
read the staging Grants table. `utils/tally_themes.py` reads the staging tables and writes the staging theme count
tables. `sync_grants.py` creates a Synapse project for each new grant (not a dry run).

**7. Promote staging to production** (`portal_tables/promote_staging.py`). This is the only script that writes the
production portal tables. A person starts it after the staging tables are validated; it is not in `run_sync.sh` or
the GitHub workflow.

```bash
python portal_tables/promote_staging.py --run-id 2026-11-promote --report-dir runs/promote            # dry run
python portal_tables/promote_staging.py --run-id 2026-11-promote --report-dir runs/promote --execute  # LIVE WRITE
```

It makes each production table an exact copy of its staging table (full replace; nothing is derived). `--tables`
limits it to some tables. First it compares the columns of every selected table; if any table differs it writes
`schema_diff.csv` and stops before it writes. Then it writes `<resource>_diff.csv` (rows added, removed and changed)
and `summary.csv` to `<report-dir>/<run-id>/`. With `--execute`, for each table it snapshots the staging table,
replaces the production rows (production is snapshotted first and restored if the store fails), reads production
back and compares. It stops at the first failure; tables promoted before the failure stay promoted. If the process
stops between the delete and the store, restore the production table from its snapshot by hand.
Check: `summary.csv` shows `promoted` for each table.

## Fixing records

Fix a record in the grant RecordSets. Never fix it in the portal tables or in the merged manifests: the next
cycle rebuilds them from the RecordSets and the fix is lost. Do the dry run first. Run `--execute` only after a
person reads the dry-run report. Each write takes a lock: see [Write log and locks](#write-log-and-locks).

The source of truth is each grant's `MC2Center_<Component>_RecordSet`, in the component folder of the grant
project. A row whose `GrantView Key` lists several grants is in the RecordSet of each of those grants. Fix it in
each one. After any fix, do [After a fix](#after-a-fix).

### Change cell values

Use this when a cell of an existing row has a wrong value. The script is `annotations/fix_recordset_cells.py`.

1. Make a changes CSV with the columns `component`, `grant`, `key`, `column` and `new_value`. Put one cell on
   each line. One run takes one component.
   - `component` is `PublicationView`, `DatasetView`, `ToolView` or `EducationalResource`.
   - `grant` is the grant number, for example `CA209891`.
   - `key` is the key value of the row: a PMID, or a Synapse ID for datasets and tools.
   - `column` is the display name or the key name of the column.
   - `new_value` is the full new cell value. Write a list cell as comma-separated values, or as a JSON array
     (`["Smith, John", "Lee, Ann"]`) when an item has a comma.
2. Do a dry run:

```bash
python annotations/fix_recordset_cells.py --changes changes.csv --report-dir runs/fix
```

3. Read `refused.csv` (if there is one) and `cells.csv` in `<report-dir>/<run-id>/fix_cells/`. `cells.csv` shows
   the current and the new value of each cell.
4. Add `--execute` and run the command again.

The script finds the RecordSet of the grant. If that RecordSet has no row with the key, the script searches the
RecordSets of all grants. When one RecordSet has the key, the script uses it (`cells.csv` shows `holding_grant`
and `found_by_search`). When several have it, the line is refused: add one line for each grant.

The script refuses a line when the key is not a row, the column is unknown or is the key column, two lines set one
cell to different values, the value fails the schema, or a list item has a quote or a backslash. If the script
refuses a line, it writes nothing for any grant. `refused.csv` lists the refused lines.

The script can change a cell that a curator owns (a cell edited in Curator), but only the cells in the changes
CSV. The old values are in `<report-dir>/<run-id>/<grant project ID>/overwritten.csv`. A standard upload never changes a cell that a curator owns.

Check: each grant in `summary.csv` is `written`. The script reads the RecordSet back and compares it.

A fix is stored as a pipeline version. A later standard upload that has the same key with the old value changes
the cell back, and it does not report a conflict. Thus, also fix the source of the wrong value (for example the
AI pipeline output file), and do not upload old rows again.

To add a new row, use `annotations/upload_recordsets.py` with an upload CSV and a paths CSV (columns
`File Paths`, `folderId<Component>` and `grantId`). A key that is not in the RecordSet adds a row:

```bash
python annotations/upload_recordsets.py --paths-csv paths.csv --component DatasetView \
    --write-log-table syn78404341 --removals-table syn78404342 --report-dir runs/add
```

Read `decisions.csv`, `merged.csv` and `conflicts.csv` of each grant, then add `--execute`.

On failure:
- `written_with_invalid`: the version is stored. Some rows fail validation, also rows you did not change. Read
  `validation.csv` in the report directory and fix the cells.
- `failed` with `SynapseTimeoutError`: Synapse did not answer. Run the command again for that grant only. If a
  version was stored, the next run logs it as `recovered`. Read the RecordSet back to check.
- `locked` or `retry`: see [Write log and locks](#write-log-and-locks).

### Change a record id

Use this when a `DatasetView_id` or a `ToolView_id` must point at another entity. An upload cannot do this: it
adds the new id as a second row and keeps the old row. The script is `annotations/rekey_recordset_rows.py`.

1. Make a changes CSV with the columns `recordset`, `old_key`, `new_key` and, if you want, `reason`.
2. Do a dry run:

```bash
python annotations/rekey_recordset_rows.py --changes changes.csv --component DatasetView \
    --write-log-table syn78404341 --report-dir runs/rekey
```

3. Read `summary.csv` in `<report-dir>/<run-id>/`. Read `changes.csv` and `refused.csv` in
   `<report-dir>/<run-id>/<recordset-id>/`. The script refuses an `old_key` that is not a row or that is in two
   changes. It refuses a `new_key` that is empty, equal to the old key, in two changes, or already a key of the
   RecordSet (unless that key is also an `old_key` in the same file). It refuses a `new_key` that the removals table lists as `removed`. It writes nothing to a
   RecordSet that has a refused change.
4. Add `--execute` and run the command again.

The script stores a new version and logs it as `removed`. It adds a `removed` row for each old key to the
removals table, so later uploads drop the old key. Cells that a curator owned under the old key do not move to
the new key.

Check: `summary.csv` shows `rekeyed` for each RecordSet. The script reads the version back and compares it.
On failure: fix the `refused.csv` rows and run again. For `locked`, `retry` or `failed`, see
[Write log and locks](#write-log-and-locks). For `readback_mismatch`, do not run again: read the RecordSet.

### Remove a record

Use this for a duplicate, and for a record that must not be in the portal (for example, a summary article of
another publication). A Grid import cannot delete rows.

1. File the request. The request also needs `--execute`. Without it, the request is not written, and step 2
   removes nothing:

```bash
python annotations/remove_recordset_rows.py request --removals-table syn78404342 \
    --recordset syn... --key <key value> --reason "..." --execute
```

2. Do a dry run of the removal. Make sure that it lists the key. Then add `--execute` and run it again:

```bash
python annotations/remove_recordset_rows.py --removals-table syn78404342 --write-log-table syn78404341 \
    --recordset syn... --report-dir runs/removals
```

Check: the dry run lists the keys you expect after "would remove", and nothing after "not found". After `--execute`, no RecordSet is `failed`,
`locked`, `blocked` or `retry`. `blocked` means a Grid session is open on the RecordSet: ask the curator to close
it, then run again. See also step 4 of [The cycle](#the-cycle).

### Dataset and tool entities

Use these rules to decide if a dataset or tool row is right.

- Data stored in Synapse has only a representation in the grant project (a Folder or a Dataset). The row uses
  the Synapse ID of that entity. When `Dataset Url` names a Synapse entity, the links stage uses its Synapse ID
  and makes no new entity.
- Data stored outside Synapse has an entity in the central project CCKP Indexed Datasets (`syn76986881`, folder
  `datasets`). It has a `{accession}_links` folder with indexed file links and a Dataset that shows them. If it
  cannot be indexed yet, it has an empty `{alias}_links` folder and no Dataset.
- The repository URL stays in `Dataset Url` of the row.
- One accession is one dataset row. Split a row that lists several accessions into one row for each accession.
  For a non-GEO row, the Dataset Name is the title in the repository.
- Each Tool Name has one shared entity in `syn76986881/tools`: a link File to the homepage, or an empty Folder
  when there is no homepage.

### Mismatches and conflicts

| Report | Meaning | Fix |
|---|---|---|
| `id_conflicts.csv` (`union_qc.py`) | One alias has different ids in different grants | Change a record id or remove a record in the grant RecordSets. Regenerate. |
| `key_conflicts.csv` (`union_qc.py`) | An id twice in one grant, or on different names (unmerged components) | The same. |
| `validation.csv`, `validation_counts.csv` | The row fails the schema. The row is kept. | Change the cell values. Or record the item for curation. |
| `04_links/tool_conflicts.csv`, `dataset_conflicts.csv` | A name is already used by a different kind of entity, a `_links` folder of that name is not empty, an alias has different `Dataset Url` values, or a Synapse URL has no Synapse ID. The row is skipped and dropped. | Change the name, the URL or the existing entity. Run again with `--from-stage links`. |

`union_qc.py --upload` is refused while `id_conflicts.csv` or `key_conflicts.csv` has rows.

No report shows this mismatch. Check it by hand: an alias and a URL that name different accessions. Read the
publication, then fix the wrong cell.

### After a fix

1. Run `portal_tables/union_qc.py --regenerate` (dry run). Read `report.txt`, `validation_counts.csv` and the
   conflict reports (step 5 of [The cycle](#the-cycle)).
2. Run it with `--upload` after sign-off. This stores the merged manifests.
3. Run the staging syncs (step 6).
4. Promote to production (step 7) only with the explicit okay of the owner.

## Manual checkpoints

1. After step 1: QC the AI pipeline output in `review_app.py`.
2. After the step 2 dry run: read the reports, then `--execute`.
3. After step 5 dry run: review the validation report and `id_conflicts.csv`; sign off before `--upload`.
4. After step 6 dry run: compare with `--output_csv` against the staging table before the real sync.
5. After step 6: validate the staging tables (by query in Synapse), then read the
   step 7 dry run (`summary.csv`, `<resource>_diff.csv`). `--execute` needs explicit sign-off.

## Write log and locks

`syn78404341` records every pipeline write per RecordSet: `recordSetId`, `version`, `baseVersion`, `runId`,
`principalId`, `status`, `writtenOn`, `rowsWritten`. Statuses: `pending` (a write is starting; this is the lock),
`written` (a version made by a Grid export), `removed` (a DCC removal's direct store), `recovered` (a version
found and logged after a crashed run), `created` (the RecordSet's creation version, migration), `verified` (a
version read back and matched, migration), `done` (all exports of a write logged; closes `pending`), `aborted`
(the run created nothing; closes `pending`). Versions not logged as `written`, `removed` or `recovered` count as
curator edits and are replayed cell by cell; a cell a curator owns is never overwritten by an upload.

Lock: a `pending` row with no `done`/`recovered`/`aborted` closer and younger than 2 hours blocks other writes to
that RecordSet; the run skips the grant with status `locked` (exit 1). If two runs start together, the earlier
`pending` row wins. A `locked` result means another run is writing: wait for it. If none is running (a crash), the
lock expires after 2 hours; the next run of `upload_recordsets.py` or `remove_recordset_rows.py` then recovers the
orphaned version (`recovered`) or closes the row (`aborted`). `retry` means the RecordSet changed during the run;
nothing was written, rerun.

To clear a stuck lock by hand (no run is writing to the RecordSet):
1. Find the `pending` row in `syn78404341` for the RecordSet: note its `runId` and `baseVersion`.
2. Compare the RecordSet's latest version with `baseVersion`.
3. If they are the same (the run created no version), add a row to `syn78404341` with the same `recordSetId`,
   `runId` and `baseVersion`, `status` = `aborted`, `writtenOn` = now and `version` empty. The lock is then clear.
4. If the RecordSet has a newer version, do not clear the lock by hand. Wait until the lock expires (2 hours), then
   run `upload_recordsets.py` or `remove_recordset_rows.py` (a dry run is enough): it logs the orphaned version as
   `recovered`. A version that is not logged counts as a curator edit.

## When the data model pin moves

Each grant RecordSet is bound to one JSON Schema version (`MC2Center-<Component>-<version>`; now 14.1.2). After a new
data-models release, re-bind the RecordSets to it, or Grid keeps validating against the old version. For the grant
RecordSets, run `migration/migrate_grant_tables_14.py --component <Component> --schema-version <version>` (dry run,
then `--execute`, which needs `--write-log-table syn78404341`): for each migrated RecordSet it binds the new schema and re-validates it with one logged Grid
export; the data does not change. The admin RecordSets (grants, projects, people) have no re-bind step in
`migration/migrate_admin_manifests_14.py` yet. Also move the AI curation pipeline's `MODEL_REF` pin and
`portal_tables/model_contract.MODEL_REF` (the default schema version of `union_qc.py`) to the same version.

## Grid and list-cell caveats

- In list columns (JSON arrays) Grid's export stores U+0080-009F, U+2000-20FF and `/` as literal `\uXXXX` / `\/`.
  The readers undo this (`recordsets.grid_unescape`).
- A `"` or `\` inside a list item makes Grid unable to open the RecordSet (500 errors). `upload_recordsets.py`
  keeps such rows out of an upload (`unsafe_list_items.csv`). To repair a RecordSet that already has one, run
  `annotations/repair_recordset.py`: it lists the invalid list cells (`broken_cells.csv`), applies a fixes CSV
  (`key`, `column`, `value` = the corrected cell as a JSON array), stores the result directly as a new version,
  logs it in the write log and re-validates through one Grid export (dry run unless `--execute`).
  `summary.csv` shows `repaired` when the read-back matches. `locked` (another run holds the lock) and `retry`
  (the RecordSet moved before the write) mean nothing was written: see [Write log and locks](#write-log-and-locks).
  `refused`, `failed` and `mismatch` also give exit status 1:

  ```bash
  python annotations/repair_recordset.py --recordset syn... --component PublicationView --fixes fixes.csv \
      --write-log-table syn78404341 --report-dir runs/repair [--execute]
  ```
- Control characters (tab, newline) inside list items lose their backslash on export and cannot be recovered.

## Exit codes

| Script | 0 | 1 | 2 |
|---|---|---|---|
| `upload_workflow.py` | all stages done, nothing dropped | a stage failed (including any grant needing attention in the upload) | done, but rows dropped or files excluded (see `run.json`) |
| `upload_recordsets.py` | all grants ok | a grant `failed`, `missing_recordset`, `locked`, `retry` or `verification_mismatch` | - |
| `remove_recordset_rows.py` | ok | a RecordSet `failed`, `locked`, `blocked` or `retry` | - |
| `rekey_recordset_rows.py` | every RecordSet `dryrun` or `rekeyed` | a RecordSet `refused`, `locked`, `retry`, `failed` or `readback_mismatch` | - |
| `repair_recordset.py` | `dryrun` or `repaired` | `locked`, `retry`, `refused`, `failed` or `mismatch` | - |
| `union_qc.py --regenerate` | no validation failures | - | validation failures, or a component skipped (empty union) |

`missing_recordset`: the grant has no `MC2Center_<Component>_RecordSet` in its data-type folder; the upload does not
create it.
`migration/migrate_grant_tables_14.py` and `migration/migrate_admin_manifests_14.py` (one-time cutover tools, not part of the cycle) use
0 ok, 1 a grant failed or the comparison differs, 2 unmapped values or validation failures.

## Synapse IDs

| ID | What |
|---|---|
| `syn78404341` | write log table |
| `syn78404342` | `MC2Center_RecordSet_Removals` table |
| `syn28742843` | CCKP - MC2 Admin project (admin RecordSets in `admin_recordsets`, `STAGING ...` portal tables) |
| `syn53478776` / `syn53478774` / `syn53479671` / `syn53651540` | merged manifests: publications / datasets / tools / education |
| `syn53259587` / `syn59074382` / `syn38301033` | legacy admin manifests: grants / projects / people |
| `syn21868591` / `syn21897968` / `syn26127427` / `syn51497305` | production portal tables: publications / datasets / tools / education |
| `syn21918972` / `syn21868602` / `syn28073190` | production portal tables: grants / projects / people |
| `syn21641485` / `syn21649281` / `syn21639584` | production theme count tables: consortium / consortium-theme / theme |
| `syn78748028` / `syn78748031` / `syn78748036` / `syn78748037` | staging portal tables: publications / datasets / tools / education |
| `syn78748038` / `syn78748039` / `syn78748040` | staging portal tables: grants / projects / people |
| `syn78754152` / `syn78754154` / `syn78754168` | staging theme count tables: consortium / consortium-theme / theme |
| `syn76986881` / `syn77025587` | central project / DatasetCollection used by the dataset `links` stage |

## Not yet run for real

Remove this section after the first full cycle.

- A real grant, project or people sync from the admin RecordSets.
- A real sync into the staging tables, and `promote_staging.py --execute`.
- `union_qc.py --regenerate --upload` from the RecordSets.
- The first curator cycle through Curator.
