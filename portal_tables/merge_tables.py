"""
Create/store UNION tables from grant-specific metadata tables in 
Synapse.

Inputs:
- synID for source tableview, queried for table synIDs to combine into UNION table
- synID for the project in which to store the UNION table
- manifest type contained in source tables

Outputs:
- a MaterializedViewSchema table, containing all entries from
the tables provided as input
- if table already exists, scope will be updated and table will be regenerated in-place

author: orion.banks
"""

import synapseclient
from synapseclient import MaterializedViewSchema
import argparse
import pandas as pd


### Login to Synapse ###
def login():

    syn = synapseclient.Synapse()
    syn.login()

    return syn


def get_args():

    parser = argparse.ArgumentParser(
        description="Create UNION tables from metadata stored in Synapse project tables"
    )
    parser.add_argument(
        "-s", type=str, help="Synapse ID of entityview with table information to query."
    )
    parser.add_argument(
        "-t", type=str, help="Synapse ID of target project to store merged table."
    )
    parser.add_argument(
        "-n",
        type=str,
        choices=["PublicationView", "DatasetView", "ToolView", "EducationalResource"],
        help="Name of metadata component being merged into table.",
    )
    return parser.parse_args()


def get_table_ids(syn, source_id, table_type, column_name):

    table_id_sheet = syn.tableQuery(
        f"SELECT {column_name} FROM {source_id} WHERE name='{table_type}'"
    )

    table_id_df = pd.DataFrame(table_id_sheet)

    table_id_list = table_id_df[0].to_list()

    return table_id_list


def build_query(table_ids):

    operation_list = []

    for id in table_ids:

        operation = f"SELECT * FROM {id}"
        operation_list.append(operation)

    full_query = " UNION ".join(operation_list)

    return full_query


def main():

    syn = login()
    args = get_args()
    source, target, table_type = args.s, args.t, args.n

    if table_type in [
        "PublicationView",
        "DatasetView",
        "ToolView",
        "EducationalResource",
    ]:

        label = f"{table_type}_UNION"

        form_table_type = table_type.lower()

        view_type = f"{form_table_type}_synapse_storage_manifest_table"

    else:
        print(
            f"{table_type} is not a valid table type. Please select a different component."
        )

    table_ids_from_view = get_table_ids(syn, source, view_type, "id")
    print(table_ids_from_view)

    table_query = build_query(table_ids_from_view)

    table = MaterializedViewSchema(name=label, parent=target, definingSQL=table_query)

    merged_table = syn.store(table)


if __name__ == "__main__":
    main()
