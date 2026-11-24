# Manifest Processing and Upload Workflow

## `upload_workflow.py`

`upload_workflow.py` chains, for one component:
`split` -> `paths` -> `reshape` -> `links` (DatasetView/ToolView only) -> `upload`.

```bash
python annotations/upload_workflow.py --manifest publication.csv --type PublicationView \
    --out-dir runs/2026-10                                          # dry run (default)
python annotations/upload_workflow.py ... --execute              # write to Synapse
python annotations/upload_workflow.py ... --from-stage reshape   # resume
```

- `--target recordset` (default) calls `upload_recordsets.py`. `--write-log-table` and `--removals-table` default to
  the production tables (`syn78404341`, and `syn78404342` = `MC2Center_RecordSet_Removals`) and are always passed on.
  A dry run reads the two tables and writes nothing;
  `--target schematic-table` runs `upload-manifests.py` as a subprocess (needs `--schematic-config`
  with `--execute`; it has no dry-run mode, so a dry run only prints the command).
- Each stage writes `<out-dir>/<NN>_<stage>/`; `<out-dir>/run.json` records stages, outputs and timestamps.
- `links` creates Synapse entities, so it only runs with `--execute`. Before it, rows with an empty
  link column are dropped (`04_links/dropped_rows.csv`) and GEO/SRA accessions that already have a
  Dataset entity in the central project are reused instead of recreated (`04_links/reused_entities.csv`).
- `SYNAPSE_AUTH_TOKEN`, when set, is passed to subprocesses (never logged).

The full cycle (AI pipeline output, RecordSet upload, curation, removals, merged manifests, portal sync) is in
[`orchestration/README.md`](../orchestration/README.md).
