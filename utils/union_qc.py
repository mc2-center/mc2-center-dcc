"""
union_qc.py

Submits a query to get all information from a Synapse table
Validates table entries against a schematic data model
Returns row identifer and validation state
Stores a table in Synapse with id and validation info

author: orion.banks
"""

import synapseclient
import argparse
import pandas as pd
from pathlib import Path
import subprocess
import sys

### Login to Synapse ###
def login():

    syn = synapseclient.Synapse()
    syn.login()

    return syn


def get_args():
	
	parser = argparse.ArgumentParser(
        description='Access and validate tables from Synapse')
	parser.add_argument('-l',
						nargs='+',
                        help='Synapse table IDs to query.')
	parser.add_argument('-c',
                        help='path to schematic config.yml')
	parser.add_argument('-m',
						action='store_true',
                        help='Boolean; if flag is provided, manifest rows will be merged by primary key.')
	return parser.parse_args()


def get_tables(syn, tableIdList):
	
	tables = []
	names = []
	
	for tableId in tableIdList:
		table = syn.tableQuery(f"SELECT * FROM {tableId}").asDataFrame()
		name = table.iat[1,0]
		manifestPath = Path(f"output/{name}.csv")
		manifestPath.parent.mkdir(parents=True, exist_ok=True)
		table.to_csv(manifestPath, index=False, lineterminator='\n')
		tables.append(manifestPath)
		names.append(name)
	
	return list(zip(tables, names))

def combine_rows(args):
	
	newTables, newNames = args

	groups = []
	names = []
	
	for table, name in newTables, newNames:
		nameParts = [name, "id"]
		grantParts = [name[:-4], "Grant Number"]
		idColumn = "_".join(nameParts)
		grantColumn = " ".join(grantParts)
		mergedTable = table.groupby(idColumn, as_index=False).agg({grantColumn : ','.join}).reset_index()
		mergePath = Path(f"output/{name}_merged.csv")
		mergePath.parent.mkdir(parents=True, exist_ok=True)
		mergedTable.to_csv(mergePath, index=False)
		groups.append(mergePath)
		names.append(nameParts[0])
		
	return list(zip(groups, names))

def validate_tables(args, config):

	paths, names = args

	validNames = []
	validOuts = []
	
	for path, name in paths, names:
		
		command = [
			"schematic",
        	"model",
			"-c",
			config,
			"validate",
			"-dt",
			name,
			"-mp",
			str(path)]

		print(f"Validating manifest at: {str(path)}...")

		outPath = Path(f"output/{name}_out.txt")
		outPath.parent.mkdir(parents=True, exist_ok=True)

		errPath = Path(f"output/{name}_error.txt")
		errPath.parent.mkdir(parents=True, exist_ok=True)

		commandOut = open(outPath, "w")
		errOut = open(errPath, "w")
		
		process = subprocess.run(
			command,
			text=True,
			check=True,
			stdout=commandOut,
			stderr=errOut)
		
		validNames.append(name)
		validOuts.append(commandOut)
	
	return list(zip(validNames, validOuts))

def parse_out(args):

	names, outs = args
	
	for name, out in names, outs:
		parsePath = Path(f"output/{name}_out.csv")
		parsePath.parent.mkdir(parents=True, exist_ok=True)
		parsed = pd.read_table(out, sep="[", header=None)
		parsedOut = parsed.to_csv(parsePath, index=False, sep="\n", header=False, columns=None, quoting=None)


def main():
	
	syn = login()
	
	args = get_args()
	inputList, config, merge = args.l, args.c, args.m 

	print("Accessing requested tables...")
	newTables = get_tables(syn, inputList)
	print("Table(s) downloaded from Synapse and converted to data frames!")
	
	if merge:
		print("Merging rows with matching primary keys...")
		newTables = combine_rows(newTables)
		print("Matching rows merged!")

	print("Converting dataframes to CSV and validating...")
	checkTables = validate_tables(newTables, config)
	print("Validation reports generated!")
	
	print("Converting validation info to create reference table...")
	validEntries = parse_out(checkTables)


if __name__ == "__main__":
    main()