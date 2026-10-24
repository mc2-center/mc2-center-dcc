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
import sys
import pandas as pd
from synapseclient import operations

import importlib.util
# removed when the mc2dcc package (CLI consolidation plan) lands
# portal_tables/utils.py, loaded by path: "utils" would collide with this package
_spec = importlib.util.spec_from_file_location(
    "portal_tables_utils",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "portal_tables", "utils.py"),
)
_portal_utils = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_portal_utils)
syn_login = _portal_utils.syn_login

# removed when the mc2dcc package (CLI consolidation plan) lands
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "portal_tables"))
import model_contract


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


def remove_files_from_dataset(syn, dataset: str, files: list[str]) -> tuple[str, list]:
    """Get files in dataset and remove if in input list of file Synapse IDs"""

    dataset_entity = operations.get(
        dataset,
        file_options=operations.FileOptions(download_file=False),
        synapse_client=syn,
    )
    dataset_files = pd.DataFrame([{"entityId": item.id} for item in dataset_entity.items])
    files_to_remove = [file for file in files if file in dataset_files["entityId"].to_list()]
    # remove_item() needs a full EntityRef (id + version); match refs by id only
    # like the legacy remove_item(id).
    for ref in [r for r in dataset_entity.items if r.id in files_to_remove]:
        dataset_entity.remove_item(ref, synapse_client=syn)
    dataset_entity.store(synapse_client=syn)

    return dataset_entity.id, files_to_remove

def main():

    syn = syn_login()

    args = get_args()

    config = args.d

    if os.path.exists(config):
        config_df = pd.read_csv(config, keep_default_na=False, header=0)
        print("\nDataset trimming config read successfully!")
    elif "syn" in config:
        config_df = model_contract.read_manifest(syn, config).fillna("")
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
