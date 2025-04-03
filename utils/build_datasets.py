"""build_datasets.py

This script will update the scope of a Synapse Dataset

Usage:
python build_datasets.py -d [Dataset Synapse Id] -s [Folder Synapse Id containing files for Dataset] -c [CSV with dataset and folder Synapse Ids] -f [File formats to include in Dataset]

author: orion.banks
"""

import synapseclient
import synapseutils
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
        help="Synapse Id of a folder that contains files to add to the Dataset",
        required=False
    )
    parser.add_argument(
        "-c",
        type=str,
        help="Path to a CSV file with Dataset and Folder Synapse Ids.",
        required=False
    )
    parser.add_argument(
        "-f",
        nargs="+",
        help="Space-separated list of file formats to include in the Dataset.",
        required=False,
        default=None
    )
    return parser.parse_args()


def filter_files_in_folder(syn, scopeId: str, formats: list, dataset):
    """Capture all files in provided scope and select files that match a list of formats,
    add files with matching format(s) to dataset object,
    return dataset object"""

    all_files = []
    walkPath = synapseutils.walk(syn, scopeId, ["file"])
    for dirpath, dirname, filename in walkPath:
        all_files = all_files + filename
    files = [file[1] for file in all_files for format in formats if format in file[0]]
    dataset_items = [{"entityId":f, "versionNumber":syn.get(f, downloadFile=False).versionLabel} for f in files]
    dataset.add_items(dataset_items)

    return dataset

def main():

    syn = synapseclient.login()

    args = get_args()

    dataset_id, scope_id, id_sheet, formats = args.d, args.s, args.c, args.f

    if id_sheet:
        id_set = pd.read_csv(id_sheet, header=None)
        if id_set.iat[0,0] == "DatasetView_id" and id_set.iat[0,1] == "Folder_id":
            print(f"\nInput sheet read successfully...\n\nCreating Datasets now:")
            count = 0
            for _, row in id_set.iterrows():
                dataset_id = row["DatasetView_id"]
                scope_id = row["Folder_id"]
                formats = list(row["format_list"])
                dataset = syn.get(dataset_id)
                if len(formats) > 0:
                    dataset = filter_files_in_folder(syn, scope_id, formats, dataset)
                else:
                    dataset.add_folder(scope_id, force=True)
                dataset = syn.store(dataset)
                print(f"\nDataset {dataset_id} successfully updated with files from {scope_id}")
                count += 1
            print(f"\n\nDONE ✅\n{count} Datasets processed")
        else:
            print(f"\n❗❗❗ The table provided does not appear to be formatted for this operation.❗❗❗\nPlease check its contents and try again.")
    
    else:
        if dataset_id and scope_id:
            dataset = syn.get(dataset_id)
            if formats is not None:
                dataset = filter_files_in_folder(syn, scope_id, formats, dataset)
            else:    
                dataset.add_folder(scope_id, force=True)
            dataset = syn.store(dataset)
        else:
            print(f"\n❗❗❗ No dataset information provided.❗❗❗\nPlease check your command line inputs and try again.")


if __name__ == "__main__":
    main()
