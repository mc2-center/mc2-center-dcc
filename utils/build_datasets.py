"""build_datasets.py

This script will update the scope of a Synapse Dataset

Usage:
python build_datasets.py -d <Dataset Synapse Id> -s <Folder Synapse Id containing files for Dataset>

author: orion.banks
"""

import synapseclient
import argparse


def get_args():
    """Set up command-line interface and get arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d",
        type=str,
        help="Synapse Id of a Dataset to update with files from input folder",
    )
    parser.add_argument(
        "-s",
        type=str,
        help="Synapse Id of a folder that should have all files added to the Dataset",
    )
    return parser.parse_args()


def main():

    syn = (
        synapseclient.login()
    )  # you can pass your username and password directly to this function

    args = get_args()

    datasetId, scopeId = args.d, args.s  # assign path to manifest file from command line input

    dataset = syn.get(datasetId)

    dataset.add_folder(scopeId, force=True)

    dataset = syn.store(dataset)


if __name__ == "__main__":
    main()
