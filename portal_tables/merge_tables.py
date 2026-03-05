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
from synapseclient.models import RecordSet, SchemaStorageStrategy, Table
import argparse
import pandas as pd


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
    parser.add_argument(
        "-rec",
        "--target_record_set",
        action="store_true",
        default=None,
        help="Boolean. If provided, the program will search for Synapse RecordSets of the requested data type in MC2 Center projects"
    )
    return parser.parse_args()


def get_table_ids(syn, source_id, table_type, column_name):

    table_id_sheet = syn.tableQuery(
        f"SELECT {column_name} FROM {source_id} WHERE name='{table_type}'"
    )

    table_id_df = pd.DataFrame(table_id_sheet)

    return table_id_df[0].to_list()


def build_query(table_ids):
    
    return " UNION ".join([f"SELECT * FROM {id}" for id in table_ids])


def get_record_sets(syn, record_type, folder_name, source = "syn21918972", org = "MC2Center"):
    
    project_ids = syn.tableQuery(f"SELECT 'grantId' FROM {source}")
    
    record_df = pd.DataFrame(project_ids)
    record_df["recordId"] = ""
    record_name = "_".join([org, record_type])

    for _,row in record_df.iterrows():
        folder_id = syn.findEntityId(name=folder_name, parent=row["grantId"])
        record_id = syn.findEntityId(name=record_name, parent=folder_id)
        record_set = RecordSet(id=record_id).get()
        record_set_df = pd.read_csv(record_set.path, header=0)  # apply sorting, column naming, extraction, etc. as needed
        record_set_table = Table(name=f"{record_name}_table", parent_id=row["grantId"]).store()
        record_set_table.store_rows(values=record_set_df, schema_storage_strategy=SchemaStorageStrategy.INFER_FROM_DATA)
        record_df.at[_,"recordId"] = record_set_table.id
    
    return record_df


def get_record_sets(syn, record_type, folder_name, source = "syn21918972", org = "MC2Center"):
    
    project_ids = syn.tableQuery(f"SELECT 'grantId' FROM {source}")
    
    record_df = pd.DataFrame(project_ids)
    record_df["recordId"] = ""
    record_name = "_".join([org, record_type])

    for _,row in record_df.iterrows():
        folder_id = syn.findEntityId(name=folder_name, parent=row["grantId"])
        record_id = syn.findEntityId(name=record_name, parent=folder_id)
        record_set = RecordSet(id=record_id).get()
        record_set_df = pd.read_csv(record_set.path, header=0)  # apply sorting, column naming, extraction, etc. as needed
        record_set_table = Table(name=f"{record_name}_table", parent_id=row["grantId"]).store()
        record_set_table.store_rows(values=record_set_df, schema_storage_strategy=SchemaStorageStrategy.INFER_FROM_DATA)
        record_df.at[_,"recordId"] = record_set_table.id
    
    return record_df

def main():

    syn = synapseclient.Synapse()
    syn.login()
    args = get_args()
    source, target, table_type, record_sets = args.s, args.t, args.n, args.target_record_set

    folder_map = {
        "PublicationView" : "publications",
        "DatasetView" : "datasets",
        "ToolView" : "tools",
        "EducationalResource" : "education"
    }

    if table_type in [
        "PublicationView",
        "DatasetView",
        "ToolView",
        "EducationalResource"
    ]:

        label = f"{table_type}_UNION"
        view_type = f"{table_type.lower()}_synapse_storage_manifest_table"
        record_type =  f"{table_type}_RecordSet"
        folder_name = folder_map[table_type]

    else:
        print(
            f"{table_type} is not a valid table type. Please select a different component."
        )
    
    if record_sets is not None:
        record_df = get_record_sets(syn, record_type, folder_name)
        table_ids_from_view = record_df["recordId"].to_list()
    else:
        table_ids_from_view = get_table_ids(syn, source, view_type, "id")
    
    print(table_ids_from_view)

    table_query = build_query(table_ids_from_view)

    table = MaterializedViewSchema(name=label, parent=target, definingSQL=table_query)

    syn.store(table)


if __name__ == "__main__":
    main()
