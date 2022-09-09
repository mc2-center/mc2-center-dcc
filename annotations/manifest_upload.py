import synapseclient
from synapseclient import Table
import argparse
import pandas as pd

# Script to upload manifests to table (before being added to merged table).
# Run only after validating on Schematic. This script is temporary, until
# Schematic bugs are fixed for manifest upload using the table feature.


### Login to Synapse ###
def login():

    syn = synapseclient.Synapse()
    syn.login()

    return syn


def get_args():

    parser = argparse.ArgumentParser(
        description='Get synapse project id, file path, and name of table')
    parser.add_argument('table_id',
                        type=str,
                        help='Synapse table id to upload manifest to.')
    parser.add_argument('file', type=str, help='Path to file')

    return parser.parse_args()


# Get Schema for table and create dictionary
def get_schema(syn, table_id):

    cols = syn.getTableColumns(table_id)

    col_dict = {}
    for col in cols:
        for k, v in col.items():
            if k == 'name':
                col_dict[v] = col['columnType']

    # create dictionary to map to python data types
    data_type_dict = {
        'STRING': str,
        'INTEGER': int,
        'LARGETEXT': str,
        'STRING_LIST': list,
        'DOUBLE': str,
        'LINK': str
    }
    col_dict = {k: data_type_dict.get(v, v) for k, v in col_dict.items()}

    return (col_dict)


# Edit manifest to accomadate table schema
def edit_manifest(file_path, col_dict):

    df = pd.read_csv(file_path, index_col=False).fillna("")

    for columnName in df:
        if col_dict[columnName] == list:
            df[columnName] = df[columnName].astype(str)
            df[columnName] = df[columnName].str.split(', ')
        else:
            df[columnName] = df[columnName].map(col_dict)

    return df


def manifest_upload(syn, table_id, df):

    syn.store(Table(table_id, df))

    print("\nManifest uploaded to table")

    print("\n\nPlease add the manifest to the corresponding folder")


def main():

    choice = input(
        "\n\nDid you validate the manifest using Schematic before running this script? Type 'y' for yes, 'n' for no"
    )
    if choice == 'y':

        syn = login()
        args = get_args()
        column_dictionary = get_schema(syn, args.table_id)
        edited_manifest = edit_manifest(args.file, column_dictionary)

        manifest_upload(syn, args.table_id, edited_manifest)

    elif choice == 'n':
        print("\n\nPlease validate first, then rerun this script to upload!")

    else:
        print("\n\nNot a valid input, try running again.")


if __name__ == "__main__":
    main()