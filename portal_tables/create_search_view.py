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
import re


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
        "-s", type=str, nargs="+", help="List of Synapse IDs with table information to query."
    )
    parser.add_argument(
        "-t", type=str, help="Synapse ID of target project to store merged table."
    )
    parser.add_argument(
        "-n", type=str, help="Name for the materialized view table."
    )
    return parser.parse_args()



def build_query(syn, sources, prefix_regex, component_regex, view_list, column_types):

    query_list = []

    for source in sources:
        source_query = ["SELECT"]
        from_statement = f"FROM {source}"
        table = syn.get(source)
        table_name = table.name
        column_prefix = re.match(prefix_regex, table_name)[0]

        if column_prefix in view_list:
            component = re.match(component_regex, column_prefix)[0]
        
        elif column_prefix == "EducationalResource":
            component = "Resource"
    
        else:
            component = column_prefix

        data_type = "Component,"
        source_query.append(data_type)
        
        id_mapping = f"{column_prefix}_id AS Asset_id,"
        source_query.append(id_mapping)

        for column_type in column_types:
            resource_column = f"{component}{column_type}"
            union_column = f"Asset{column_type}"
            column_mapping = f'"{resource_column}" AS "{union_column}"'
            source_query.append(column_mapping)
        
        source_query.append(from_statement)
        
        source_query_string = " ".join(source_query)

        query_list.append(source_query_string)

    full_query = " UNION ".join(query_list)

    print(f"The following query will be used to generate a materialized view: \n\n{full_query}")
        
    return full_query


def main():

    syn = login()
    args = get_args()
    sources, target, label = args.s, args.t, args.n

    prefix_regex = re.compile("^[^_]*")
    
    component_regex = re.compile(".*?(?=View)")

    view_list = [
            "PublicationView",
            "DatasetView",
            "ToolView"
            ] 

    column_types = [
        " Grant Number"
        ]
        
    table_query = build_query(syn, sources, prefix_regex, component_regex, view_list, column_types)

    table = MaterializedViewSchema(name=label, parent=target, definingSQL=table_query)

    merged_table = syn.store(table)


if __name__ == "__main__":
    main()
