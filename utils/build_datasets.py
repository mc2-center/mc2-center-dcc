"""build_datasets.py

This script will provision Datasets as needed and add files to created/existing Datasets, based on
information provided in a Data Sharing Plan CSV or a Data Sharing Plan table Synapse Id.

Usage:
python build_datasets.py -d [DataDSP filepath] -n [Name for DSP CSV output]

author: orion.banks
"""

import argparse
import os
import pandas as pd
import re
import synapseclient
from synapseclient import Dataset
import synapseutils


def get_args():
    """Set up command-line interface and get arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d",
        type=str,
        help="Path or Table Synapse Id associated with a Data Sharing Plan.",
        required=True,
        default=None,
    )
    parser.add_argument(
        "-n",
        type=str,
        help="Name to apply to an updated Data Sharing Plan CSV",
        required=False,
        default="updated_dsp",
    )
    return parser.parse_args()


def get_table(syn, source_id: str) -> pd.DataFrame:
    """Collect a Synapse table entity and return as a Dataframe."""

    query = f"SELECT * FROM {source_id}"
    table = syn.tableQuery(query).asDataFrame().fillna("")

    return table


def filter_files_in_folder(syn, scope: str, formats: list[str]) -> list:
    """Capture all files in provided scope and select files that match a list of formats,
    return list of dataset items"""

    dataset_items = []
    walk_path = synapseutils.walk(syn, scope, ["file"])
    for *_, filename in walk_path:
        for f, entity_id in filename:
            if any(f.endswith(fmt) for fmt in formats):  # only select files of desired format
                dataset_items.append({
                    "entityId": entity_id,
                    "versionNumber": syn.get(f, downloadFile=False).versionLabel
                })

    return dataset_items


def create_dataset_entity(syn, name: str, grant: str) -> Dataset:
    """Create an empty Synapse Dataset using the
    Project associated with the applicable grant number as parent.
    Return the Dataset object."""

    query = f"SELECT grantId FROM syn21918972 WHERE grantViewId='{grant}'"
    project_id = syn.tableQuery(query).asDataFrame().iat[0, 0]
    dataset = Dataset(name=name, parent=project_id)

    return dataset


def main():

    syn = synapseclient.login()

    args = get_args()

    dsp, new_name = args.d, args.n
    update_dsp_sheet = None
    
    if os.path.exists(dsp):
        dsp_df = pd.read_csv(dsp, keep_default_na=False)
        print("\nData Sharing Plan read successfully!")
    elif "syn" in dsp:
        dsp_df = get_table(syn, dsp)
        print(f"Data Sharing Plan acquired from Synapse table {dsp}!")
    else:
        print(
            f"❗❗❗ {dsp} is not a valid Data Sharing Plan identifier. Please check your inputs and try again."
        )
        exit()

    if dsp_df.iat[1, 0] == "DataDSP":
        count = 0
        for _, row in dsp_df.iterrows():
            grant_id = row["GrantView Key"]
            dataset_id = row["DatasetView Key"]
            scope_id = row["DSP Dataset Alias"]
            dataset_name = row["DSP Dataset Name"]
            formats = re.split(", |,", row["DSP Dataset File Formats"])
            level = row["DSP Dataset Level"]
            if level in ["Metadata", "Auxiliary", "Not Applicable"]:
                continue  # move to next table entry if not data files
            
            print(f"\nProcessing Dataset {dataset_name}")
            if len(dataset_id) > 0:  # check if a Dataset entity was previously recorded 
                print(f"--> Accessing Dataset {dataset_id}")
                dataset = syn.get(dataset_id)
                print(f"--> {dataset_id} accessed!")
            else:
                print(
                    f"--> A new Dataset will be created for files from {scope_id}"
                )
                dataset = create_dataset_entity(syn, dataset_name, grant_id)
                update_dsp_sheet = True  # record the new DatasetView_id in DSP
                print(f"--> New Dataset created!")

            if len(formats) > 0:  # only filter files if formats were specified
                print(f"--> Filtering files from {scope_id}")
                scope_files = filter_files_in_folder(syn, scope_id, formats)
                folder_or_files = "files"  # use add_items function
                print(
                    f"--> {scope_id} files filtered!\n    {len(scope_files)} files will be added to the Dataset."
                )
            else:
                folder_or_files = "folder"  # whole folder should be added, use add_folder function

            if folder_or_files == "folder":
                print(f"--> Adding Folder {scope_id} to Dataset {dataset_id}")
                dataset.add_folder(scope_id, force=True)
                print(f"--> Folder added to Dataset!")
            elif folder_or_files == "files":
                print(f"--> Adding Files from {scope_id} to Dataset {dataset_id}")
                dataset.add_items(dataset_items=scope_files, force=True)
                print(f"--> Files added to Dataset!")

            dataset = syn.store(dataset)
            print(f"Dataset {dataset_id} successfully stored in {dataset.parentId}")

            if update_dsp_sheet is not None:
                dataset_id = dataset.id
                dsp_df.at[_, "DatasetView Key"] = dataset_id

            count += 1
    else:
        print(
            f"❗❗❗ The table provided does not appear to be a Dataset Sharing Plan.❗❗❗\nPlease check its contents and try again."
        )
        exit()
    
    print(f"\n\nDONE ✅\n{count} Datasets processed")

    if update_dsp_sheet is not None:
        dsp_path = f"{os.getcwd()}/{new_name}.csv"
        dsp_df.to_csv(path_or_buf=dsp_path, index=False)
        print(f"\nDSP sheet has been updated\nPath: {dsp_path}")


if __name__ == "__main__":
    main()
