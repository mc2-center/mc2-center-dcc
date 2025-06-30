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
import random
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


def filter_files_in_folder(syn, scope: str, formats: list[str], folder_or_files: str) -> list:
    """Capture all files in provided scope and select files that match a list of formats,
    return list of dataset items"""

    dataset_items = []
    walk_path = synapseutils.walk(syn, scope, ["file"])
    for *_, filename in walk_path:
        if folder_or_files == "files":
            file_to_add = [entity_id for f, entity_id in filename if any(f.endswith(fmt) for fmt in formats)]  # only select files of desired format
        elif folder_or_files == "folder":
            file_to_add = [entity_id for f, entity_id in filename]  # select all files in folder
        for entity_id in file_to_add:
            dataset_items.append({
                    "entityId": entity_id,
                    "versionNumber": syn.get(entity_id, downloadFile=False).versionLabel
                })
        dataset_len = len(dataset_items)
        print(f"--> {dataset_len} files found...")
    return dataset_items


def create_dataset_entity(syn, name: str, grant: str, multi_dataset: bool) -> Dataset:
    """Create an empty Synapse Dataset using the
    Project associated with the applicable grant number as parent.
    Return the Dataset object."""

    query = f"SELECT grantId FROM syn21918972 WHERE grantViewId='{grant}'"
    project_id = syn.tableQuery(query).asDataFrame().iat[0, 0]
    if multi_dataset:
        name = f"{name}-{random.randint(1000, 9999)}"  # append random number to name for multi-dataset
    dataset = Dataset(name=name, parent=project_id)

    return dataset

def chunk_files_for_dataset(scope_files: list[str], file_max: int, dataset_total: int) -> list[list[str]]:
    """Chunk files into lists of size file_max for Dataset creation."""
    file_groups = []
    for i in range(dataset_total):
        file_group = scope_files[i * file_max:(i + 1) * file_max]
        file_groups.append(file_group)
    return file_groups

def main():

    syn = synapseclient.login()

    args = get_args()

    dsp, new_name = args.d, args.n
    update_dsp_sheet = None

    file_max = 10000  # maximum number of files per Dataset
    
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

    if dsp_df.iat[0, 0] == "DataDSP":
        count = 0
        for _, row in dsp_df.iterrows():
            grant_id = row["GrantView Key"]
            dataset_id = row["DatasetView Key"]
            scope_id = row["DSP Dataset Alias"]
            dataset_name = row["DSP Dataset Name"]
            formats = re.split(", |,", row["DSP Dataset File Formats"])
            level = row["DSP Dataset Level"]
            if level in ["Metadata", "Auxiliary", "Not Applicable"]:
                print(f"Skipping Dataset {dataset_name} of type {level}")
                continue  # move to next table entry if not data files
            
            dataset_id_list = []
            file_scope_list = []
            dataset_name_list = []

            if formats:  # only filter files if formats were specified
                print(f"--> Filtering files from {scope_id}")
                folder_or_files = "files"  # filter files by extension/format
            else:
                folder_or_files = "folder"  # whole folder should be added, don't filter files
            
            scope_files = filter_files_in_folder(syn, scope_id, formats, folder_or_files)
            print(f"--> {scope_id} files acquired!\n    {len(scope_files)} files will be added to the Dataset.")

            if dataset_id:  # check if a Dataset entity was previously recorded 
                print(f"--> Files will be added to Dataset {dataset_id}")
                dataset = syn.get(dataset_id, downloadFile=False)
            else:
                dataset = create_dataset_entity(syn, dataset_name, grant_id, multi_dataset=False)
                update_dsp_sheet = True  # record the new DatasetView_id in DSP
                print(f"--> New Dataset created for files from {scope_id}")
            
            dataset_id_list.append(dataset.id)
            dataset_name_list.append(dataset.name)

            if len(scope_files) > file_max:
                dataset_total = (len(scope_files) // file_max) + 1
                multi_dataset = True
                update_dsp_sheet = True
                print(
                    f"--> File count exceeds file max.\n--> Creating {dataset_total} new Datasets for files from {scope_id}"
                )
                for i in range(dataset_total):
                    dataset = create_dataset_entity(syn, dataset_name, grant_id, multi_dataset)
                    print(f"--> New Dataset created!")
                    dataset_id_list.append(dataset.id)
                    dataset_name_list.append(dataset.name)
                file_scope_list = chunk_files_for_dataset(scope_files, file_max, dataset_total)
            else:
                multi_dataset = False
                file_scope_list = [scope_files]  # single dataset, no chunking needed

            dataset_tuples = zip(dataset_id_list, file_scope_list, dataset_name_list)

            for dataset_id, scope_files, name in dataset_tuples:
                dataset = syn.get(dataset_id, downloadFile=False)
                dataset.add_items(dataset_items=scope_files, force=True)
                print(f"--> Files added to Dataset!")
                dataset = syn.store(dataset)
                print(f"Dataset {dataset.id} successfully stored in {dataset.parentId}")
                if update_dsp_sheet is not None:
                    temp_df = pd.DataFrame()
                    if multi_dataset:
                        temp_df.loc[_] = dsp_df.loc[_]
                        temp_df[_, "DatasetView Key"] = dataset.id
                        temp_df[_, "DSP Dataset Name"] = name
                        dsp_df = pd.concat([dsp_df, temp_df], ignore_index=True)
                    else:
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
