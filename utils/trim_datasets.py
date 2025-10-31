"""trim_datasets.py

This script will remove files from datasets based on Synapse Ids.
Takes a CSV with two columns as input: "datasets", "files"
Iterates through the datasets, checks for Synapse Ids in files, and removes them if present

Usage:
python trim_datasets.py -d [trim config filepath]

author: orion.banks
"""

import argparse
import os
import pandas as pd
import synapseclient
from synapseclient import Dataset


def get_args():
    """Set up command-line interface and get arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d",
        type=str,
        help="Path or Table Synapse Id associated with a dataset trim config.",
        required=True,
        default=None,
    )
    return parser.parse_args()


def get_table(syn, source_id: str) -> pd.DataFrame:
    """Collect a Synapse table entity and return as a Dataframe."""

    query = f"SELECT * FROM {source_id}"
    table = syn.tableQuery(query).asDataFrame().fillna("")

    return table


def remove_files_from_dataset(syn, dataset: str, files: list[str]) -> tuple[str, list]:
    """Get files in dataset and remove if in input list of file Synapse IDs"""

    dataset_entity = syn.get(dataset, downloadFile=False)
    dataset_files = pd.DataFrame(dataset_entity.properties.datasetItems)
    files_to_remove = [file for file in files if file in dataset_files["entityId"].to_list()]
    for file in files_to_remove:
        dataset_entity.remove_item(file)
    syn.store(dataset_entity)

    return dataset_entity.id, files_to_remove

def main():

    syn = synapseclient.login()

    args = get_args()

    config = args.d

    if os.path.exists(config):
        config_df = pd.read_csv(config, keep_default_na=False, header=0)
        print("\nDataset trimming config read successfully!")
    elif "syn" in config:
        config_df = get_table(syn, config)
        print(f"Data trimming config acquired from Synapse table {config}!")
    else:
        print(
            f"❗❗❗ {config} is not a valid trimming config identifier. Please check your inputs and try again."
        )
        exit()

    if "datasets" and "files" in config_df.columns:
        dataset_list = [dataset for dataset in config_df["datasets"].to_list() if dataset]
        file_list = config_df["files"].to_list()
        for dataset in dataset_list:
            if dataset:
                dataset_id, removed_files = remove_files_from_dataset(syn, dataset, file_list)
                print(f"{len(removed_files)} removed from Dataset {dataset_id}")

if __name__ == "__main__":
    main()
