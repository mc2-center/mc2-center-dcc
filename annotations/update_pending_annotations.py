""" Update Restricted Publication Annotations 

This script will update preivously 'Restricted' Publications
with their updated annotations after they have become 'Open Access' """

import synapseclient
from synapseclient import Table, RowSet
import argparse
import pandas as pd
from attribute_dictionary import PUBLICATION_DICT


def login():
    """ Login to Synapse """

    syn = synapseclient.Synapse()
    syn.login()

    return syn


def get_args():

    parser = argparse.ArgumentParser(
        description='Get synapse table id of annotations to be edited and '
        'synapse table id of controlled vocabulary mappings')
    parser.add_argument(
        '-t',
        '--table_id',
        type=str,
        default='syn21868591',
        help='Synapse table id where annotations will be updated. '
        'Probably the Publications Merged table.')
    parser.add_argument('-m',
                        '--manifest_path',
                        type=str,
                        help='Path where updated manifest is stored.')
    parser.add_argument("--dryrun", action="store_true")
    return parser.parse_args()


def get_updated_df(manifest):

    updated_df = pd.read_csv(manifest, index_col=False).fillna("")

    return updated_df


def get_annotations(table_id, updated_df, syn):

    pubmed_list = updated_df['pubMedId'].astype(str).tolist()
    pubmed_string = ', '.join(pubmed_list)
    print(f'list of pubmeds to be updated: {pubmed_string}')

    annots_query = syn.tableQuery(
        ("SELECT pubMedId, assay, tumorType, tissue, dataset, accessibility"
         f" FROM {table_id} WHERE pubMedId IN ({pubmed_string})"))

    return annots_query


def edit_annotations(updated_df, annots_query, syn, table_id, dryrun):

    # Convert annotations to data frame
    annots_df = annots_query.asDataFrame().fillna("")
    # preserve original index
    index_rows = dict(zip(annots_df.pubMedId, annots_df.index))

    # rename updated_df columns to match annots_df column and set index to pubMedId
    updated_df = updated_df.rename(
        columns=PUBLICATION_DICT).set_index('pubMedId')

    # update annots_df to match updated_df
    final_df = annots_df.set_index('pubMedId')
    final_df.update(updated_df)
    # resetting index so pubMedId can be used to map original index
    final_df.reset_index(inplace=True)
    # Reset index to match original annots_df using index_rows dict.
    final_df['index'] = final_df['pubMedId'].map(index_rows)
    final_df.set_index(keys='index', inplace=True)

    # Get column data types from synapse table
    cols = syn.getTableColumns(table_id)
    col_dict = {}
    for col in cols:
        col_name = col['name']
        col_dict[col_name] = col['columnType']

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

    # Create a new dictionary with column names as keys and values as data types
    col_types_dict = {k: data_type_dict.get(v, v) for k, v in col_dict.items()}

    # Fix data types in annots_df to match synapse table
    for columnName in final_df:
        if col_types_dict[columnName] == list:
            final_df[columnName] = final_df[columnName].str.split(', ')
        else:
            final_df[columnName] = final_df[columnName].astype(
                col_types_dict[columnName])

    if dryrun:
        annots_df.to_csv('updated_annotations.csv', index=False)

    else:
        return final_df


def manifest_upload(syn, table_id, final_df, annots_query):

    columns = final_df.columns
    print(columns)

    syn.store(Table(table_id, final_df, etag=annots_query.etag))

    print("\nAnnotations Updated")

    print(
        "\n\nPlease add the manifest to the Status Check folder saved as FINAL"
    )


def main():

    choice = input(
        "\n\nDid you validate the manifest using Schematic before running this script?"
        " AND update the portal table-friendly manifest?"
        " Type 'y' for yes, 'n' for no")
    if choice == 'y':

        syn = login()
        args = get_args()
        update_df = get_updated_df(args.manifest_path)
        annots_results = get_annotations(args.table_id, update_df, syn)
        final_df = edit_annotations(update_df, annots_results, syn,
                                    args.table_id, args.dryrun)

        manifest_upload(syn, args.table_id, final_df, annots_results)

    elif choice == 'n':
        print("\n\nPlease validate first, then rerun this script to upload!")

    else:
        print("\n\nNot a valid input!")
        main()


if __name__ == "__main__":
    main()
