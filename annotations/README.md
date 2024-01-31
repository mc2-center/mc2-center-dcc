# Manifest Processing and Upload Workflow

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
- `CCKP_backpopulation_id_validation.csv`

### Process:
Runs the Python script `gen-mp-csv.py` to generate file paths based on split manifests.

### Output:
Creates `month_filepaths.csv` in present directory

## Step 3: Process Split Files

### Input Files:
- File path information in `./output/output_month`

### Process:
Runs the Python script `processing-splits.py` to process split files from the specified output folder. Adds missing columns required to match the schema, truncates any columns with 400+ words, and adds "Read more on Pubmed."

### Output:
Performs processing on split files.

## Step 4: Run Schema Updates

### Input Files:
- `month_filepaths.csv`

### Process:
Runs the Python script `schema_update.py` to update the schema based on file paths and specified parameters.

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
