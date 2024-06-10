"""
union_qc.py

Submits a query to get all information from a Synapse table
Validates table entries against a schematic data model
Returns row identifer and validation state
Trims invalid entries using schematic error log

Custom trim config can be provided at run time
CSV can be passed at run time for validation, merging, and trimming

author: orion.banks
"""

import synapseclient
import argparse
import pandas as pd
from pathlib import Path
import subprocess
import re


### Login to Synapse ###
def login():

    syn = synapseclient.login()

    return syn


def get_args():

    parser = argparse.ArgumentParser(
        description="Access and validate tables from Synapse"
    )
    parser.add_argument("-l", nargs="+", help="Synapse table IDs to query.")
    parser.add_argument("-c", help="path to schematic config.yml")
    parser.add_argument(
        "-bl",
        required=False,
        default=None,
        help="path to CSV with row numbers to trim from manifest. Numbers should be provided on separate rows.",
    )
    parser.add_argument(
        "-tp", required=False, default=None, help="path to manifest CSV to trim."
    )
    parser.add_argument(
        "-m",
        action="store_true",
        help="Boolean; if flag is provided, manifest rows will be merged by model-specific key.",
    )
    parser.add_argument(
        "-t",
        action="store_true",
        help="Boolean; if flag is provided, manifest rows with errors will be trimmed.",
    )
    return parser.parse_args()


def get_tables(syn, tableIdList, mergeFlag):

    tables = []  # set up lists to store info
    names = []

    for tableId in tableIdList:

        table = (
            syn.tableQuery(f"SELECT * FROM {tableId}").asDataFrame().fillna("")
        )  # pull table from Synapse
        name = table.iat[
            1, 0
        ]  # grab name of data type from table; assumes "Component" is first column in table

        manifestPath = Path(f"output/{name}.csv")  # build path to store table as CSV
        manifestPath.parent.mkdir(
            parents=True, exist_ok=True
        )  # create folder to store CSVs

        table.to_csv(
            manifestPath, index=False, lineterminator="\n"
        )  # convert df to CSV

        if mergeFlag:
            tables.append(table)  # if merging, store the table for the next function
        else:
            tables.append(
                manifestPath
            )  # if not merging, store the file path for the next function

        names.append(name)  # store the name for next functions

    return list(zip(tables, names))


def combine_rows(args):

    newTables, newNames = zip(*args)  # unpack the input

    groups = []
    names = []

    for table, name in zip(newTables, newNames):
        table = table.astype(
            str
        )  # make everything strings so they can be joined as needed

        nameParts = [name, "id"]  # define parts of component_id column name

        componentColumn = "Component"
        idColumn = "_".join(nameParts)  # build component_id column name

        if name in ["PublicationView", "DatasetView", "ToolView"]:
            # define parts of column names with common formats between manifests
            # build column names
            # access mapping dictionaries associated with manifest types

            grantParts = [name[:-4], "Grant Number"]
            grantColumn = " ".join(grantParts)

            if name in ["PublicationView", "DatasetView"]:
                assayParts = [name[:-4], "Assay"]
                tumorParts = [name[:-4], "Tumor Type"]
                tissueParts = [name[:-4], "Tissue"]

                assayColumn = " ".join(assayParts)
                tumorColumn = " ".join(tumorParts)
                tissueColumn = " ".join(tissueParts)

                if name == "PublicationView":

                    aliasColumn = "Pubmed Id"  # column to group entries by

                    mapping = {  # defines how info in each column is handled by row merging function
                        componentColumn: "first",
                        idColumn: ",".join,
                        grantColumn: ",".join,
                        "Publication Doi": "first",
                        "Publication Journal": "first",
                        "Pubmed Url": "first",
                        "Publication Title": "first",
                        "Publication Year": "first",
                        "Publication Keywords": "first",
                        "Publication Authors": "first",
                        "Publication Abstract": "first",
                        assayColumn: "first",
                        tumorColumn: "first",
                        tissueColumn: "first",
                        "Publication Accessibility": "first",
                        "Publication Dataset Alias": "first",
                        "entityId": ",".join,
                    }

                elif name == "DatasetView":

                    aliasColumn = "Dataset Alias"

                    mapping = {
                        componentColumn: "first",
                        idColumn: ",".join,
                        "Dataset Pubmed Id": "first",
                        grantColumn: ",".join,
                        "Dataset Name": "first",
                        "Dataset Description": "first",
                        "Dataset Design": "first",
                        assayColumn: "first",
                        "Dataset Species": "first",
                        tumorColumn: "first",
                        tissueColumn: "first",
                        "Dataset Url": "first",
                        "Dataset File Formats": "first",
                        "entityId": ",".join,
                    }

            elif name == "ToolView":

                aliasColumn = "Tool Name"

                mapping = {
                    componentColumn: "first",
                    idColumn: ",".join,
                    "Tool Pubmed Id": "first",
                    grantColumn: ",".join,
                    "Tool Description": "first",
                    "Tool Homepage": "first",
                    "Tool Version": "first",
                    "Tool Operation": "first",
                    "Tool Input Data": "first",
                    "Tool Output Data": "first",
                    "Tool Input Format": "first",
                    "Tool Output Format": "first",
                    "Tool Function Note": "first",
                    "Tool Cmd": "first",
                    "Tool Type": "first",
                    "Tool Topic": "first",
                    "Tool Operating System": "first",
                    "Tool Language": "first",
                    "Tool License": "first",
                    "Tool Cost": "first",
                    "Tool Accessibility": "first",
                    "Tool Download Url": "first",
                    "Tool Download Type": "first",
                    "Tool Download Note": "first",
                    "Tool Download Version": "first",
                    "Tool Documentation Url": "first",
                    "Tool Documentation Type": "first",
                    "Tool Documentation Note": "first",
                    "Tool Link Url": "first",
                    "Tool Link Type": "first",
                    "Tool Link Note": "first",
                    "entityId": ",".join,
                }

        elif name == "EducationalResource":

            aliasColumn = "Resource Alias"

            mapping = {
                componentColumn: "first",
                idColumn: ",".join,
                "Resource Title": "first",
                "Resource Link": "first",
                "Resource Topic": "first",
                "Resource Activity Type": "first",
                "Resource Primary Format": "first",
                "Resource Intended Use": "first",
                "Resource Primary Audience": "first",
                "Resource Educational Level": "first",
                "Resource Description": "first",
                "Resource Origin Institution": "first",
                "Resource Language": "first",
                "Resource Contributors": "first",
                "Resource Grant Number": ",".join,
                "Resource Secondary Topic": "first",
                "Resource License": "first",
                "Resource Use Requirements": "first",
                "Resource Internal Identifier": "first",
                "Resource Media Accessibility": "first",
                "Resource Access Hazard": "first",
                "Resource Dataset Alias": "first",
                "Resource Tool Link": "first",
                "entityId": ",".join,
            }

        mergedTable = (
            table.groupby(aliasColumn, as_index=False).agg(mapping).reset_index()
        )  # group rows by designated identifier and map attributes
        mergedTable = mergedTable.iloc[:, 1:]  # remove unnecessary "id" column

        mergePath = Path(f"output/{name}_merged.csv")
        mergePath.parent.mkdir(parents=True, exist_ok=True)

        mergedTable.to_csv(mergePath, index=False)

        groups.append(mergePath)
        names.append(nameParts[0])

    return list(zip(groups, names))

def get_ref_tables(syn, args):

    tables, names = zip(*args)

    ref_paths = []
    table_paths = []
    ref_names = []
    
    for table, name in tables, names:

        if name == "PublicationView":
            ref = "syn53478776"
        
        elif name == "DatasetView":
            ref = "syn53478774"

        elif name == "ToolView":
            ref = "syn53479671"

        elif name == "EducationalResource":
            ref = "syn53651540"

        ref_table = syn.get(ref, downloadLocation="./output")
        ref_paths.append(ref_table.path)
        table_paths.append(table)
        ref_names.append(name)

    return list(zip(ref_paths, table_paths, ref_names))

def compare_and_subset_tables(args):

    current, updated, names = zip(*args)

    updatePaths = []
    updateNames = []

    for ref, new, name in current, updated, names:

        if name == "PublicationView":
            key = ["Pubmed Id"]
        
        elif name == "DatasetView":
            key = ["Dataset Alias"]

        elif name == "ToolView":
            key = ["Tool Name"]

        elif name == "EducationalResource":
            key = ["Resource Alias"]

        current_table = pd.read_csv(ref, header=0).sort_values(by=key)
        new_table = pd.read_csv(new, header=0).sort_values(by=key)

        updated = new_table[~new_table.isin(current_table).all(axis=1)] #report all non-matching entries

        updatePath = Path((f"output/{name}_updated.csv"))
        updatePath.parent.mkdir(parents=True, exist_ok=True)

        updated.to_csv(updatePath, index=False)

        updatePaths.append(updatePath)
        updateNames.append(name)
    
    return list(zip(updatePaths, updateNames))

def validate_tables(args, config):

    paths, names = zip(*args)

    validNames = []
    validOuts = []
    validPaths = []

    for path, name in zip(paths, names):

        command = (
            [  # pass config, datatype, and CSV path(s) to schematic for validation
                "schematic",
                "model",
                "-c",
                config,
                "validate",
                "-dt",
                name,
                "-mp",
                str(path),
            ]
        )

        print(f"\n\nValidating manifest at: {str(path)}...")

        outPath = Path(f"output/{name}_out.txt")
        outPath.parent.mkdir(parents=True, exist_ok=True)

        errPath = Path(f"output/{name}_error.txt")
        errPath.parent.mkdir(parents=True, exist_ok=True)

        commandOut = open(outPath, "w")  # store logs from schematic validation
        errOut = open(errPath, "w")

        process = subprocess.run(
            command, text=True, check=True, stdout=commandOut, stderr=errOut
        )

        validNames.append(name)
        validOuts.append(outPath)
        validPaths.append(path)

    return list(zip(validNames, validOuts, validPaths))


def parse_out(args):

    names, outs, paths = zip(*args)

    parsedNames = []
    parsedOuts = []
    parsedPaths = []

    for name, out, path in zip(names, outs, paths):

        parsePath = Path(f"output/{name}_trim_config.csv")
        parsePath.parent.mkdir(parents=True, exist_ok=True)

        parsed = pd.read_table(
            out, sep="], ", header=None, engine="python"
        )  # load output from schematic validation

        parsedOut = parsed.to_csv(
            parsePath, index=False, sep="\n", header=False, columns=None, quoting=None
        )  # convert log to useable format

        parsedNames.append(name)
        parsedOuts.append(parsePath)
        parsedPaths.append(path)

    return list(zip(parsedNames, parsedOuts, parsedPaths))


def trim_tables(args):

    trimmedTables = []

    names, outs, paths = zip(*args)

    for name, out, path in zip(names, outs, paths):
        trimPath = Path(f"output/{name}_trimmed.csv")
        trimPath.parent.mkdir(parents=True, exist_ok=True)

        validationTable = pd.read_csv(out, header=None)
        processedTable = pd.read_csv(path, header=0)
        print(processedTable)

        flaggedRows = (
            validationTable.iloc[:, 0]
            .str.extract(r"(\d{1,5})", expand=False)
            .astype(int)
        )
        flaggedRows = set(flaggedRows)
        flaggedRows = list(flaggedRows)
        flaggedRows = sorted(flaggedRows)

        flaggedRows = [(x - 2) for x in flaggedRows]
        print(flaggedRows)

        trimmedTable = processedTable.drop(flaggedRows, inplace=False)

        trimmedTable.to_csv(trimPath, index=False, header=True)

        trimmedTables.append(trimPath)

    return trimmedTables

    # upload to CCKP - Admin using base CSV name and date of upload as label


def main():

    args = get_args()

    inputList, config, trimList, inputManifest, merge, trim = (
        args.l,
        args.c,
        args.bl,
        args.tp,
        args.m,
        args.t,
    )

    if trimList is None:

        if inputManifest is None:
            print("Accessing requested tables...")

            syn = login()

            newTables = get_tables(syn, inputList, merge)
            print(
                "\n\nTable(s) downloaded from Synapse and stored as CSVs in output folder!"
            )

        elif inputManifest is not None:

            tables, newNames = [], []

            table = pd.read_csv(inputManifest, header=0)
            name = table.loc[:, "Component"].iat[1]

            if merge:
                tables.append(table)

            else:
                tables.append(inputManifest)

            newNames.append(name)

            newTables = list(zip(tables, newNames))
            print(f"\n\nReading provided table at {inputManifest} of type {name}.")

        if merge:
            print("\n\nMerging rows with matching identifier...")

            newTables = combine_rows(newTables)

            print("\n\nMatching rows merged!")
            print(
                "\n\nMerged table(s) converted to CSV and stored in local output folder!"
            )

            print("\n\nValidating merged manifest(s)...")

        else:
            print("\n\nValidating unmerged manifest(s)...")
        
        

        checkTables = validate_tables(newTables, config)
        print("\n\nValidation logs stored in local output folder!")

        print("\n\nConverting validation logs to create reference tables...")

        print(checkTables)

        validEntries = parse_out(checkTables)
        print("\n\nValidation logs parsed and saved as CSVs!")

        if trim:
            print("\n\nTrimming invalid entries from manifests...")
            cleanTables = trim_tables(validEntries)
            print("\n\nInvalid entries trimmed!")

        else:
            print("\n\nNo trimming performed. Manifests may contain invalid entries.")

    if trimList is not None:

        if inputManifest is not None:

            names, outs, paths = [], [], []

            name = re.search("\/(\w*)(_trim_config)", str(trimList))

            if name is None:
                print(
                    "\n\nPlease provide a trim config that uses the expected naming convention."
                )
                exit

            else:
                print(
                    f"\n\nThe file {str(inputManifest)} will be trimmed based on {str(trimList)}"
                )

                names.append(name[1])

                out = str(trimList)
                outs.append(out)

                path = str(inputManifest)
                paths.append(path)

                validEntries = list(zip(names, outs, paths))

                print("\n\nTrimming invalid entries from manifests...")
                cleanTables = trim_tables(validEntries)
                print("\n\nInvalid entries trimmed!")

        elif inputManifest is None:
            print(f"\n\nNo manifest provided. Please designate a manifest to trim.")


if __name__ == "__main__":
    main()
