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

- `--target recordset` (default) calls `upload_recordsets.py` (a dry run reads the write-log and removals tables and writes nothing to them;
  `--write-log-table` defaults to `syn78404341` and `--removals-table` to `syn78404342`, the production
  `MC2Center_RecordSet_WriteLog` and `MC2Center_RecordSet_Removals`, and both are always passed on);
  `--target schematic-table` runs `upload-manifests.py` as a subprocess (needs `--schematic-config`
  with `--execute`; it has no dry-run mode, so a dry run only prints the command).
- Each stage writes `<out-dir>/<NN>_<stage>/`; `<out-dir>/run.json` records stages, outputs and timestamps.
- `links` creates Synapse entities, so it only runs with `--execute`. Before it, rows with an empty
  link column are dropped (`04_links/dropped_rows.csv`) and GEO/SRA accessions that already have a
  Dataset entity in the central project are reused instead of recreated (`04_links/reused_entities.csv`).
  Other datasets get an empty placeholder Folder named `{alias}_links` in the central project's `datasets` folder. A Dataset Url that names a Synapse entity gives that Synapse ID, with no new entity. A row is skipped, listed in `04_links/dataset_conflicts.csv` and dropped if its folder name is taken by a non-Folder entity or by a Folder that is not empty (an empty Folder of that name is reused), if its alias has different Dataset Url values across files, or if its Synapse URL has no Synapse ID.
- `SYNAPSE_AUTH_TOKEN`, when set, is passed to subprocesses (never logged).

## Fix single cells: `fix_recordset_cells.py`

Use `fix_recordset_cells.py` to change cell values in grant RecordSets without code. Give it a CSV with the columns
`component, grant, key, column, new_value`: one line for each cell, one grant number in each line (to fix a row that lists
several grants, add one line for each grant). When the named grant does not hold the row, the script finds the RecordSet that holds it;
if several RecordSets hold it, the script refuses the line and lists the grants. `key` is the row key (a PMID for PublicationView, a Synapse ID for DatasetView and ToolView).
`column` is the display name or the key name. `new_value` is the whole new value; write a list as a comma separated list.
An item that holds a comma must be given as a JSON array, for example `["Smith, John", "Lee, Ann"]`; the script writes every list cell
as a JSON array, so an unchanged item with a comma stays one item. A line is refused when a list item (new, or unchanged in a row that the script
writes) holds a quote or a backslash, because `upload_recordsets.py` drops such a row.

```bash
python annotations/fix_recordset_cells.py --changes fixes.csv --report-dir reports            # dry run (default)
python annotations/fix_recordset_cells.py --changes fixes.csv --report-dir reports --execute  # write to Synapse
```

The script refuses all lines, and writes nothing, if one line has an unknown grant, key or column, names the key column
(use `rekey_recordset_rows.py`), or has a value that the JSON Schema does not allow. It then runs `upload_recordsets.py`.
A cell that a curator edited in Curator is normally kept by an upload. Here, each cell in the changes file replaces such a value,
because it is a deliberate fix; every other curator-owned cell is kept. The replaced values are in `overwritten.csv` of the upload report.
Only this script does this (`upload_recordsets.py --overwrite-csv`); the standard upload never does.

This repository contains a workflow to process and upload manifests into synapse using a shell script named `upload-workflow.sh`. The workflow involves several steps, each with specific input files, processes, and outputs. Below is a detailed description of each step:

## Step 0: Activate Conda Environment

Before running the workflow, activate the Conda environment named "schematic."

```bash
conda activate schematic
```

## Step 1: Split Manifests

### Input Files:
- `manifest_month.csv`

### Process:
Runs the Python script `split_manifest_grants.py` to split manifests.

### Output:
Creates split manifest files in the `./output/output_month` directory in CSV format.

## Step 2: Generate File Paths

### Input Files:
- Split manifest files in `./output/output_month`

### Process:
Runs the Python script `gen-mp-csv.py <folder> <output_csv> <data_type>` to generate file paths based on split manifests. It queries `syn21918972` and `syn27210848` (read-only); `CCKP_backpopulation_id_validation.csv` is not used by the code.

### Output:
Creates `month_filepaths.csv` in present directory

## Step 3: Process Split Files

### Input Files:
- File path information in `./output/output_month`

### Process:
Runs the Python script `processing-splits.py` to process split files from the specified output folder. Adds missing columns required to match the schema, truncates free-text cells over 500 characters to 400 (enum, list, pattern and format columns are never cut), and appends "(Read more on Pubmed)".

### Output:
Performs processing on split files.

## Step 4: Run Schema Updates

### Input Files:
- `month_filepaths.csv`

### Process:
Runs the Python script `schema_update.py` to update the schema based on file paths and specified parameters. It isn't needed with `--target recordset`.

### Output:
Updates the schema on synpase.

## Step 5: Upload Manifests

### Input Files:
- `month_filepaths.csv`

### Process:
Runs the Python script `upload-manifests.py` to upload manifests to a specified target with additional configuration.

### Output:
Uploads manifests to the target specified.

## Success or Error Handling

Checks the success of each step within the `upload-workflow.sh` script and displays appropriate messages. If any step fails, it reports an error and terminates the script. If all steps are successful, it prints "All scripts executed successfully."
