"""build_datasets.py

This script will update the version of a Synapse Dataset

Usage:
python version_datasets.py -d <Dataset Synapse Id> -c <Comment to include in version info> -l <Label to apply to version> -s <CSV with dataset Synapse Ids, label, and comment info>

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
        help="Synapse Id of a Dataset to version",
        required=False
    )
    parser.add_argument(
        "-c",
        type=str,
        help="Comment that will be applied to the Dataset version",
        required=False
    )
    parser.add_argument(
        "-l",
        type=str,
        help="Label that will be applied to the Dataset version",
        required=False
    )
    parser.add_argument(
        "-s",
        type=str,
        help="CSV file with arguments contained in each column",
        required=False
    )
    return parser.parse_args()


def main():

    syn = (
        synapseclient.login()
    )  # you can pass your username and password directly to this function

    args = get_args()

    datasetId, comment, label, sheet = args.d, args.c, args.l, args.s  # assign path to manifest file from command line input

    if sheet:
        infoSet = pd.read_csv(sheet, header=None)
        if infoSet.iat[0,0] == "DatasetView_id" and infoSet.iat[0,1] == "Comment" and infoSet.iat[0,2] == "Label":
            print(f"\nInput sheet read successfully...\n\nCreating Dataset versions now:")
            infoSet = infoSet.iloc[1:,:]
            count = 0
            for row in infoSet.itertuples(index=False):
                datasetId = row[0]
                comment = row[1]
                label = row[2]
                newVersion = syn.create_snapshot_version(datasetId, comment=comment, label=label)
                print(f"\nDataset {datasetId} successfully versioned to {newVersion}")
                count += 1
            print(f"\n\nDONE ✅\n{count} Datasets versioned")
        else:
            print(f"\n❗❗❗ The table provided does not appear to be formatted for this operation.❗❗❗\nPlease check its contents and try again.")
    
    else:
        if datasetId and comment and label:
            newVersion = syn.create_snapshot_version(datasetId, comment=comment, label=label)
            print(f"\nDataset {datasetId} successfully versioned to {newVersion}")
        else:
            print(f"\n❗❗❗ No dataset information provided.❗❗❗\nPlease check your command line inputs and try again.")


if __name__ == "__main__":
    main()
