"""
Check number of rows in a list of tables, to verify metadata uploads.

Inputs:
- list of table synIds to query

Outputs:
- CSV of table Ids and row counts

author: orion.banks
"""

import synapseclient
import argparse
import pandas as pd
from datetime import datetime

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
        "-t", nargs="+", help="Space-separated list of table Synapse Ids to query"
    )
    return parser.parse_args()


def count_table_rows(syn, table_id):

    table_row_count_list = []
    current_day = datetime.now().strftime("%Y%m%d")

    for table in table_id:
        table_df = pd.DataFrame(
            syn.tableQuery(
            f"SELECT * FROM {table}"
            )
        )

        table_row_count = len(table_df.index)

        id_count_date = (table, table_row_count, current_day)
        
        table_row_count_list.append(id_count_date)

        print(f"\n\nTable {table} contains {table_row_count} rows\n\n")

    table_row_count_df = pd.DataFrame.from_records(table_row_count_list, columns=["Table Synapse Id", "Row Count", "Time of Count"])
    
    return table_row_count_df, current_day


def main():

    syn = login()
    args = get_args()
    tables = list(args.t)

    print(f"Counting rows for {len(tables)} Synapse tables")
    table_count_df, day = count_table_rows(syn, tables)
    print(f"Counting complete:\n\n {table_count_df}")
    output = table_count_df.to_csv("table_count.csv", index=False)

if __name__ == "__main__":
    main()
