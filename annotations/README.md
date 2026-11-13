# Manifest Processing and Upload Workflow

## Recommended: `upload_workflow.py`

`upload_workflow.py` replaces `upload-workflow.sh` (broken; the step-by-step
description below is kept for reference). It chains, for one component:
`split` -> `paths` -> `reshape` -> `links` (DatasetView/ToolView only) -> `upload`.

```bash
python annotations/upload_workflow.py --manifest publication.csv --type PublicationView \
    --out-dir runs/2026-10 --write-log-table syn78404341            # dry run (default)
python annotations/upload_workflow.py ... --execute              # write to Synapse
python annotations/upload_workflow.py ... --from-stage reshape   # resume
```

- `--target recordset` (default) calls `upload_recordsets.py` (`--write-log-table`, or `--write-log-csv` for dry runs; `--execute` needs the table;
  `--removals-table` defaults to `syn78404342`, the production `MC2Center_RecordSet_Removals`, and is always passed on);
  `--target schematic-table` runs `upload-manifests.py` as a subprocess (needs `--schematic-config`
  with `--execute`; it has no dry-run mode, so a dry run only prints the command).
- Each stage writes `<out-dir>/<NN>_<stage>/`; `<out-dir>/run.json` records stages, outputs and timestamps.
- `links` creates Synapse entities, so it only runs with `--execute`. Before it, rows with an empty
  link column are dropped (`04_links/dropped_rows.csv`) and GEO/SRA accessions that already have a
  Dataset entity in the central project are reused instead of recreated (`04_links/reused_entities.csv`).
- `SYNAPSE_AUTH_TOKEN`, when set, is passed to subprocesses (never logged).

The full cycle (AI pipeline output, RecordSet upload, curation, removals, merged manifests, portal sync) is in
[`orchestration/README.md`](../orchestration/README.md). `upload-workflow.sh` is broken and legacy; use `upload_workflow.py`.
