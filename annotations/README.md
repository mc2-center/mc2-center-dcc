Make sure manifest is in the folder and change filenames accordingly. Saving split manifests in a folder output_output_month

Activate Conda Environment:

Activates the Conda environment named "schematic".

Step 1: Split Manifests
Input Files:
manifest_sep.csv
Process:
Runs python3 split_manifest_grants.py to split manifests based on certain criteria.
Output:
Creates split manifest files in the ./output/output_month directory in CSV format.

Step 2: Generate File Paths
Input Files:
Split manifest files in ./output/output_month
month_filepaths.csv
CCKP_backpopulation_id_validation.csv
Process:
Runs python3 script_name.py to generate file paths based on split manifests.
Output:
Creates file path information in the ./output/output_month directory.

Step 3: Process Split Files
Input Files:
File path information in ./output/output_month
Process:
Runs python3 combined_script.py to process split files from the specified output folder. Adds missing columns required to match the schema and truncates any columns with 400+ words and adds "Read more on Pubmed"
Output:
Performs processing on split files.

Step 4: Run Schema Updates
Input Files:
month_filepaths.csv
Process:
Runs python3 schema_update.py to update the schema based on file paths and specified parameters.
Output:
Updates the schema.

Step 5: Upload Manifests
Input Files: month_filepaths.csv
Process:
Runs python3 upload-manifests.py to upload manifests to a specified target with additional configuration.
Output:
Uploads manifests to the target specified.

Success or Error Handling:
Checks the success of each step and displays appropriate messages.
If any step fails, it reports an error and terminates the script.
If all steps are successful, it prints "All scripts executed successfully."
This workflow provides a high-level overview of the processes, inputs, and outputs for each step in the shell script.