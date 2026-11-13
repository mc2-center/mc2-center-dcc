# CCKP DCC pipeline runbook

One cycle: the AI curation pipeline produces manifests -> `upload_workflow.py` loads them into per-grant RecordSets
(`MC2Center_<Component>_RecordSet`) -> curators edit them in Synapse Curator -> `union_qc.py --regenerate` rebuilds the
merged manifests -> the sync scripts write the live portal tables. Every script that writes to Synapse is a dry run
unless you pass `--execute` (`--upload` for `union_qc.py`). Run everything from the repo root.

## Before you start

- Python 3.11 (what CI uses) in an environment of your choice, e.g. a conda env: `pip install -r requirements.txt`
  (`requirements.txt` includes `requirements-ci.txt`; `requirements-dev.txt` adds pytest). `geo-synapse`, a private
  repo, is only needed by `annotations/create_entity_links.py` (the `links` stage).
- Synapse login: `export SYNAPSE_AUTH_TOKEN=<personal access token>` (or a `.synapseConfig`). Without either,
  `utils.syn_login()` prompts for a token. The token needs write access to the grant projects, the CCKP - MC2 Admin
  project, the log tables, and the portal tables for the steps that write.

## The cycle

**1. AI curation pipeline (separate repo, `ai-curation-pipeline`).** Run `run-publication-pipeline`, then QC the output
with its `review_app.py`. The result is one manifest CSV per component (`PublicationView`, `DatasetView`, `ToolView`,
`EducationalResource`). Checkpoint: QC sign-off before step 2.

**2. Load into the grant RecordSets** (`annotations/upload_workflow.py`, once per component and manifest):

```bash
python annotations/upload_workflow.py --manifest publication.csv --type PublicationView \
    --out-dir runs/2026-11-publications --write-log-table syn78404341           # dry run
python annotations/upload_workflow.py --manifest publication.csv --type PublicationView \
    --out-dir runs/2026-11-publications --write-log-table syn78404341 --execute  # write
```

Stages `split` -> `paths` -> `reshape` -> `links` (DatasetView/ToolView only; creates Synapse entities, so only with
`--execute`) -> `upload`; each writes `<out-dir>/<NN>_<stage>/` and `<out-dir>/run.json`. `--removals-table` defaults
to `syn78404342`. Resume a failed run with `--from-stage <stage>`; a different manifest or type in a used `--out-dir`
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
(in its data-type folder), through the curation tasks / Grid. Admin metadata (grants, projects, people) is curated
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

Builds each component's union from the grants' RecordSets (grants from the Grants table), merges rows by alias key,
fills empty cells, computes `iconTags`, validates against the 14.x JSON Schemas, and writes
`<output-dir>/<Component>/<Component>_merged_manifest.csv`, `validation.csv`, `validation_counts.csv`,
`key_corrections.csv`, `id_conflicts.csv`, `report.txt`, plus `<output-dir>/summary.txt`. `--components` limits it.
`--upload` stores each manifest as a new version of the current manifest file (publications `syn53478776`, datasets
`syn53478774`, tools `syn53479671`, education `syn53651540`); it is refused with `--input-dir` or `--grant-projects`,
and while any alias has different ids in different grants (`id_conflicts.csv`: fix in the grant RecordSets and
regenerate). Invalid rows are kept and reported, never dropped. Checkpoint: review the validation report
(`validation_counts.csv`, `report.txt`, `id_conflicts.csv`) before `--upload`; `--upload` needs sign-off.

**6. Sync to the live portal tables.** `run_sync.sh` runs the publication, dataset, tool and education syncs, reading
the merged manifests above (the same script runs in the `Sync manifests to portal tables` GitHub workflow, which is
`workflow_dispatch` only):

```bash
bash run_sync.sh --dryrun     # extra arguments go to every script
bash run_sync.sh              # write the portal tables
```

Each script also takes `--output_csv` (default `./final_<resource>_table.csv`; `run_sync.sh` passes `--noprint`, so
none is written), `-v`, `-m/--manifest_id` and `-t/--portal_table_id`; to compare a dry run to the live table, run
one directly: `python portal_tables/sync_publications.py --dryrun -o final_publication_table.csv`. The grant, project
and people syncs are not in `run_sync.sh`; run them directly when the admin RecordSets change:
`python portal_tables/sync_grants.py|sync_projects.py|sync_people.py [--dryrun] [--manifest-source recordset]`.
Portal tables: publications `syn21868591`, datasets `syn21897968`, tools `syn26127427`, education `syn51497305`,
grants `syn21918972`, projects `syn21868602`, people `syn28073190`.

## Manual checkpoints

1. After step 1: QC the AI pipeline output in `review_app.py`.
2. After the step 2 dry run: read the reports, then `--execute`.
3. After step 5 dry run: review the validation report and `id_conflicts.csv`; sign off before `--upload`.
4. After step 6 dry run: compare with `--output_csv` against the live table before the real sync.

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

## Grid and list-cell caveats

- In list columns (JSON arrays) Grid's export stores U+0080-009F, U+2000-20FF and `/` as literal `\uXXXX` / `\/`.
  The readers undo this (`recordsets.grid_unescape`).
- A `"` or `\` inside a list item makes Grid unable to open the RecordSet (500 errors). Fix it by storing a
  corrected CSV directly as a new version, without a Grid session: `recordsets.store_new_version(syn, rs_id, csv_path)`
  (the function `remove_recordset_rows.py` uses; there is no script for it).
- Control characters (tab, newline) inside list items lose their backslash on export and cannot be recovered.

## Exit codes

| Script | 0 | 1 | 2 |
|---|---|---|---|
| `upload_workflow.py` | all stages done, nothing dropped | a stage failed (including any grant needing attention in the upload) | done, but rows dropped or files excluded (see `run.json`) |
| `upload_recordsets.py` | all grants ok | a grant `failed`, `missing_recordset`, `locked`, `retry` or `verification_mismatch` | - |
| `remove_recordset_rows.py` | ok | a RecordSet `failed`, `locked`, `blocked` or `retry` | - |
| `union_qc.py --regenerate` | no validation failures | - | validation failures, or a component skipped (empty union) |

`missing_recordset`: the grant has no `MC2Center_<Component>_RecordSet` in its data-type folder; the upload does not
create it.
`migrate_grant_tables_14.py` and `migrate_admin_manifests_14.py` (one-time cutover tools, not part of the cycle) use
0 ok, 1 a grant failed or the comparison differs, 2 unmapped values or validation failures.

## Synapse IDs

| ID | What |
|---|---|
| `syn78404341` | write log table |
| `syn78404342` | `MC2Center_RecordSet_Removals` table |
| `syn28742843` | CCKP - MC2 Admin project (admin RecordSets in `admin_recordsets`) |
| `syn53478776` / `syn53478774` / `syn53479671` / `syn53651540` | merged manifests: publications / datasets / tools / education |
| `syn53259587` / `syn59074382` / `syn38301033` | legacy admin manifests: grants / projects / people |
| `syn21868591` / `syn21897968` / `syn26127427` / `syn51497305` | portal tables: publications / datasets / tools / education |
| `syn21918972` / `syn21868602` / `syn28073190` | portal tables: grants / projects / people |
| `syn76986881` / `syn77025587` | central project / DatasetCollection used by the dataset `links` stage |

## Not yet run for real

Remove this section after the first full cycle.

- The admin RecordSet conversion (`migrate_admin_manifests_14.py --execute`) and the switch of
  `utils.MANIFEST_SOURCE` from `"table"` to `"recordset"` (until then the grant, project and people syncs read the
  legacy manifest tables unless you pass `--manifest-source recordset`).
- `union_qc.py --regenerate --upload` from the RecordSets.
- The first curator cycle through Curator.
