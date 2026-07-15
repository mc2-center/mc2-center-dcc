"""
Create/store a value lookup list from CCKP metadata
tables in Synapse. Converts stringlist columns to single
entries to support portal search suggestions.

Inputs:
- synID for the project in which to store the table

Outputs:
- Synapse table with harmonized column containing selected terms

author: orion.banks
"""

import synapseclient
from synapseclient.models import Table, SchemaStorageStrategy
import argparse
import pandas as pd
import utils


def get_args():

    parser = argparse.ArgumentParser(
        description="Create search term lookup table from metadata stored in Synapse project tables"
    )
    parser.add_argument(
        "-t", type=str, help="Synapse ID of target project to store merged table."
    )
    return parser.parse_args()


def get_col_values(syn, source_id, column_name):

    table_id_sheet = syn.tableQuery(
        f"SELECT {column_name} FROM {source_id}"
    )

    table_id_df = pd.DataFrame(table_id_sheet, columns=["row", "col", "name"]).drop(columns=["row", "col"])

    return pd.DataFrame(table_id_df["name"].explode(ignore_index=True).drop_duplicates())


def main():

    syn = synapseclient.Synapse()
    syn.login()
    args = get_args()
    target = args.t

    folder_map = {
        "publication" : "assay",
        "dataset" : "species",
        "tool" : "topic",
        "grant" : "theme"
    }

    term_df_list = [get_col_values(syn, utils.get_portal_tables(k), v) for k,v in folder_map.items()]
    term_df = pd.concat(term_df_list).drop_duplicates().reset_index(drop=True)
    table = Table(name="TEST_search_suggestions", parent_id=target).store()
    table.store_rows(values=term_df, schema_storage_strategy=SchemaStorageStrategy.INFER_FROM_DATA)


if __name__ == "__main__":
    main()
