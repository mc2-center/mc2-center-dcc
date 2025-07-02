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


def create_dataset_entity(syn, name: str, grant: str, multi_dataset: bool, scope: list) -> tuple[Dataset, str]:
    """Create an empty Synapse Dataset using the
    Project associated with the applicable grant number as parent.
    Return the Dataset object."""

    query = f"SELECT grantId FROM syn21918972 WHERE grantViewId='{grant}'"
    project_id = syn.tableQuery(query).asDataFrame().iat[0, 0]
    if multi_dataset:
        name = f"{name}-{random.randint(1000, 9999)}"  # append random number to name for multi-dataset
    dataset = Dataset(name=name, parent=project_id, dataset_items=scope)
    dataset = syn.store(dataset)

    return dataset

def chunk_files_for_dataset(scope_files: list[str], file_max: int, dataset_total: int) -> list[list[str]]:
    """Chunk files into lists of size file_max for Dataset creation."""
    file_groups = []
    i = 0
    while i < dataset_total:
        file_groups.append(scope_files[i * file_max:(i + 1) * file_max])
        i += 1
    if i == dataset_total:
        if len(scope_files) % file_max != 0:
            file_groups.append(scope_files[i * file_max:])
        else:
            file_groups.append(scope_files[(i - 1) * file_max:i * file_max])
    return file_groups

def main():

    syn = synapseclient.login()

    args = get_args()

    dsp, new_name = args.d, args.n
    
    update_dsp_sheet = None
    create_dataset = False
    multi_dataset = False
    file_max = 5000  # maximum number of files per Dataset; set to 5000 to avoid web page latency issues

    if os.path.exists(dsp):
        dsp_df = pd.read_csv(dsp, keep_default_na=False, header=0)
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
        updated_df = pd.DataFrame(columns=dsp_df.columns)
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
            
            dataset_total = 1
            dataset_id_list = []
            file_scope_list = []
            dataset_name_list = []

            if dataset_id:  # check if a Dataset entity was previously recorded 
                print(f"--> Files will be added to Dataset {dataset_id}")
            else:
                create_dataset = True
                update_dsp_sheet = True
            
            if formats:  # only filter files if formats were specified
                print(f"--> Filtering files from {scope_id}")
                folder_or_files = "files"  # filter files by extension/format
            else:
                folder_or_files = "folder"  # whole folder should be added, don't filter files
            
            scope_files = filter_files_in_folder(syn, scope_id, formats, folder_or_files)
            print(f"--> {scope_id} files acquired!\n    {len(scope_files)} files will be added to the Dataset.")
            
            if len(scope_files) > file_max:
                new_dataset_count = (len(scope_files) // file_max)
                dataset_total = dataset_total + new_dataset_count
                multi_dataset = True
                update_dsp_sheet = True
                create_dataset = True
                print(
                    f"--> File count exceeds file max.\n--> Creating {dataset_total} groups for files from {scope_id}"
                )
                file_scope_list = chunk_files_for_dataset(scope_files, file_max, new_dataset_count)
                
            else:
                multi_dataset = False
                file_scope_list = [scope_files]  # single dataset, no chunking needed

            if dataset_id:
                dataset = syn.get(dataset_id, downloadFile=False)
                dataset_id_list.append(dataset.id)
                dataset_name_list.append(dataset.name)
                dataset.add_items(dataset_items=file_scope_list[0], force=True)
                syn.store(dataset)
                print(f"--> Files added to existing Dataset {dataset.id}")
                file_scope_list = file_scope_list[1:]  # remove first item, already added

            if create_dataset:
                for scope in file_scope_list:
                    dataset = create_dataset_entity(syn, dataset_name, grant_id, multi_dataset, scope)
                    print(f"--> New Dataset created and populated with files!")
                    dataset_id_list.append(dataset.id)
                    dataset_name_list.append(dataset.name)
            
            count += 1

            dataset_tuples = zip(dataset_id_list, dataset_name_list)

            for dataset_id, name in dataset_tuples:
                if update_dsp_sheet is not None:
                    temp_df = dsp_df.copy()
                    temp_df.iloc[[_]] = row
                    temp_df.at[_, "DatasetView Key"] = dataset_id
                    temp_df.at[_, "DSP Dataset Name"] = name
                updated_df = pd.concat([updated_df, temp_df], ignore_index=True)
            updated_df.drop_duplicates(subset=["DatasetView Key"], keep="last", inplace=True)

    else:
        print(
            f"❗❗❗ The table provided does not appear to be a Dataset Sharing Plan.❗❗❗\nPlease check its contents and try again."
        )
        exit()
    
    print(f"\n\nDONE ✅\n{count} DSP entries processed")

    if update_dsp_sheet is not None:
        dsp_path = f"{os.getcwd()}/{new_name}.csv"
        updated_df.to_csv(path_or_buf=dsp_path, index=False)
        print(f"\nDSP sheet has been updated\nPath: {dsp_path}")


if __name__ == "__main__":
    main()
