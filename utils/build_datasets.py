"""build_datasets.py

This script will update the scope of a Synapse Dataset

Usage:
python build_datasets.py -d <Dataset Synapse Id> -s <Folder Synapse Id containing files for Dataset> -c <CSV with dataset and folder Synapse Ids>

author: orion.banks
"""

import synapseclient
import argparse
import pandas as pd


def get_args():
    """Set up command-line interface and get arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d",
        type=str,
        help="Synapse Id of a Dataset to update with files from input folder",
        required=False
    )
    parser.add_argument(
        "-s",
        type=str,
        help="Synapse Id of a folder that should have all files added to the Dataset",
        required=False
    )
    parser.add_argument(
        "-c",
        type=str,
        help="Path to a CSV file with Dataset and Folder Synapse Ids.",
        required=False
    )
    return parser.parse_args()


def main():

    syn = (
        synapseclient.login()
    )  # you can pass your username and password directly to this function

    args = get_args()

    datasetId, scopeId, idSheet= args.d, args.s, args.c  # assign path to manifest file from command line input

    if idSheet:
        idSet = pd.read_csv(idSheet, header=None)
        if idSet.iat[0,0] == "DatasetView_id" and idSet.iat[0,1] == "Folder_id":
            print(f"\nInput sheet read successfully...\n\nCreating Datasets now:")
            idSet = idSet.iloc[1:,:]
            count = 0
            for row in idSet.itertuples(index=False):
                datasetId = row[0]
                scopeId = row[1]
                dataset = syn.get(datasetId)
                dataset.add_folder(scopeId, force=True)
                dataset = syn.store(dataset)
                print(f"\nDataset {datasetId} successfully updated with files from {scopeId}")
                count += 1
            print(f"\n\nDONE ✅\n{count} Datasets processed")
        else:
            print(f"\n❗❗❗ The table provided does not appear to be formatted for this operation.❗❗❗\nPlease check its contents and try again.")
    
    else:
        if datasetId and scopeId:
            dataset = syn.get(datasetId)

            dataset.add_folder(scopeId, force=True)

            dataset = syn.store(dataset)
        else:
            print(f"\n❗❗❗ No dataset information provided.❗❗❗\nPlease check your command line inputs and try again.")


if __name__ == "__main__":
    main()
