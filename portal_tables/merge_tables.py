"""
Create/store UNION tables from grant-specific metadata tables in 
Synapse.

Inputs:
- a list of synIDs for source tables to combine into UNION table
- synID for the project in which to store the UNION table
- manifest type contained in source tables

Outputs:
- a MaterializedViewSchema table, containing all entries from
the tables provided as input

TO-DO: 
- add a Synapse tableQuery to get table ids from Synapse (after the 
distributed tables are created/info is logged) and use as input for 
'build_query' call in 'main'

- include filtering in query, to combine entries for identical 
publications across projects (?)

author: orion.banks
"""

import synapseclient
from synapseclient import MaterializedViewSchema
import argparse

### Login to Synapse ###
def login():

    syn = synapseclient.Synapse()
    syn.login()

    return syn


def get_args():
	
	parser = argparse.ArgumentParser(
        description='Create UNION tables from metadata stored in Synapse project tables')
	parser.add_argument('-l',
						nargs='+',
                        help='Synapse table IDs to query.')
	parser.add_argument('-t',
						type=str,
                        help='Synapse ID of target project to store merged table.')
	parser.add_argument('-n',
						type=str,
                        help='Name of metadata type being merged into table.')
	return parser.parse_args()

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

	label = f"{args.n}_UNION"
	table_query = build_query(args.l) 
	
	table = MaterializedViewSchema(name=label, parent=args.t, definingSQL=table_query)
	merged_table = syn.store(table)
	
if __name__ == "__main__":
    main()