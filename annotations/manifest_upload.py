"""Upload manifests to Admin Tables in Synapse

This script uploads manifests to admin tables (before being added 
to merged table). Run only after validating on Schematic. This script 
is temporary, until Schematic bugs are fixed for manifest upload using
the table feature
"""
import argparse
import synapseclient
from synapseclient import Table
import pandas as pd


def login():
    """Login to Synapse"""

    syn = synapseclient.Synapse()
    syn.login()

    return syn


def get_args():
    """Set up command-line arguments"""

    parser = argparse.ArgumentParser(
        description='Get synapse project id, file path, and name of table')
    parser.add_argument('table_id',
                        type=str,
                        help='Synapse table id to upload manifest to.')
    parser.add_argument('file', type=str, help='Path to file')

    return parser.parse_args()


def column_dict(syn, table_id):
    """Create dictionary of table column data types"""

    cols = syn.getTableColumns(table_id)

    col_dict = {}
    for col in cols:
        for k, v in col.items():
            if k == 'name':
                col_dict[v] = col['columnType']

    return (col_dict)


def col_data_type_dict(syn, table_id):

    col_dict = column_dict(syn, table_id)

    # create dictionary to map to python data types
    data_type_dict = {
        'STRING': str,
        'INTEGER': int,
        'LARGETEXT': str,
        'STRING_LIST': list,
        'DOUBLE': float,
        'LINK': str,
        'USERID': str,
        'BOOLEAN': bool
    }
    col_types_dict = {k: data_type_dict.get(v, v) for k, v in col_dict.items()}

    return col_types_dict


def edit_manifest(file_path, col_types_dict, col_dict):
    """Edit manifest to accomadate table schema"""

    df = pd.read_csv(file_path, index_col=False).fillna("")

    # Fix column names to match table schema names
    col_name_dict = {}
    for column in df.columns:
        col_name_dict[column] = column[0].lower() + column[1:].replace(" ", "")
    for k, v in col_name_dict.items():
        df.rename(columns={k: v}, inplace=True)

    # Adjust data types to match table schema
    for column_name in df:
        if col_types_dict[column_name] == list:
            df[column_name] = df[column_name].astype(str)
            df[column_name] = df[column_name].apply(
                lambda x: [y.strip() for y in x.split(',')])
        else:
            df[column_name] = df[column_name].astype(
                col_types_dict[column_name])

            # For columns with USERID as datatype, remove .0 tacked on in
            # data type conversion.
            if col_dict.get(column_name) == 'USERID':
                df[column_name] = df[column_name].replace("\.0$",
                                                          "",
                                                          regex=True)

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
        column_dictionary = column_dict(syn, args.table_id)
        data_type_dict = col_data_type_dict(syn, args.table_id)
        edited_manifest = edit_manifest(args.file, data_type_dict,
                                        column_dictionary)

        manifest_upload(syn, args.table_id, edited_manifest)

    elif choice == 'n':
        print("\n\nPlease validate first, then rerun this script to upload!")

    else:
        print("\n\nNot a valid input!")
        main()


if __name__ == "__main__":
    main()