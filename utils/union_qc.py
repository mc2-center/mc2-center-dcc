"""
union_qc.py

Submits a query to get all information from a Synapse table
Checks new Synapse table against current CCKP database entries and
reports non-matching entries
Validates non-matching table entries against a schematic data model
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
        help="""path to CSV with row numbers to trim from manifest.
        Numbers should be provided on separate rows.""",
    )
    parser.add_argument(
        "-tp", required=False, default=None, help="path to manifest CSV to trim."
    )
    parser.add_argument(
        "-m",
        action="store_true",
        default=None,
        help="""Boolean; if flag is provided,
        manifest rows will be merged by model-specific key.""",
    )
    parser.add_argument(
        "-t",
        action="store_true",
        help="""Boolean; if flag is provided,
        manifest rows with errors will be trimmed.""",
    )
    parser.add_argument(
        "-s",
        action="store_true",
        help="""Boolean; if flag is provided,
        an extended column set will be used to 
        identify updated entries.""",
    )
    parser.add_argument(
        "-db",
        action="store_true",
        default=None,
        help="""Boolean; if flag is provided,
        column labels will be added to updated output to indicate table source.""",
    )
    return parser.parse_args()


def get_tables(syn: synapseclient.login, tableIdList: list[str], mergeFlag: bool) -> list[tuple[pd.DataFrame | str, str]]:

    tables = []  # set up lists to store info
    names = []

    for tableId in tableIdList:  # pull table from Synapse
        table = syn.tableQuery(f"SELECT * FROM {tableId}").asDataFrame().fillna("")
        name = table.iat[1, 0]  # grab name of data type from table, assumes "Component" is first column in table
        manifestPath = Path(f"output/{name}/{name}.csv")  # build path to store table as CSV
        manifestPath.parent.mkdir(parents=True, exist_ok=True)  # create folder to store CSVs
        table.to_csv(manifestPath, index=False, lineterminator="\n")  # convert df to CSV
        
        if mergeFlag:  # if merging store the table for the next function
            tables.append(table)
        else:  # if not merging, store the file path for the next function
            tables.append(manifestPath)
        names.append(name)  # store the name for next functions

    return list(zip(tables, names))


def combine_rows(args: list[tuple[pd.DataFrame | str, str]]) -> list[tuple[Path, str]]:

    newTables, newNames = zip(*args)  # unpack the input

    groups = []
    names = []
    
    for table, name in zip(newTables, newNames):
        table = table.astype(str)  # make everything strings so they can be joined as needed
        nameParts = [name, "id"]  # define parts of component_id column name
        componentColumn = "Component"
        idColumn = "_".join(nameParts)  # build component_id column name
        grantColumn = "GrantView Key"
        studyColumn = "Study Key"
        pubsColumn = "PublicationView Key"
        datasetsColumn = "DatasetView Key"
        toolsColumn = "ToolView Key"

        if name in ["PublicationView", "DatasetView", "ToolView"]:
            if name in ["PublicationView", "DatasetView"]:  # define parts of column names with common formats
                assayParts = [name[:-4], "Assay"]
                tumorParts = [name[:-4], "Tumor Type"]
                tissueParts = [name[:-4], "Tissue"]
                assayColumn = " ".join(assayParts)  # build column names
                tumorColumn = " ".join(tumorParts)
                tissueColumn = " ".join(tissueParts)
                
                # access mapping dictionaries associated with manifest types
                if name == "PublicationView":
                    aliasColumn = "Pubmed Id"  # column to group entries by
                    mapping = {  # defines how info in each column is handled by row merging function
                        aliasColumn: "first",
                        componentColumn: "first",
                        idColumn: ",".join,
                        grantColumn: ",".join,
                        studyColumn: ",".join,
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
                        aliasColumn: "first",
                        componentColumn: "first",
                        idColumn: ",".join,
                        grantColumn: ",".join,
                        studyColumn: ",".join,
                        pubsColumn: ",".join,
                        "Dataset Name": "first",
                        "Dataset Description": "first",
                        "Dataset Design": "first",
                        assayColumn: "first",
                        "Dataset Species": "first",
                        tumorColumn: "first",
                        tissueColumn: "first",
                        "Dataset Url": "first",
                        "Dataset Doi": "first",
                        "Dataset File Formats": "first",
                        "Data Use Codes": ",".join,
                        "entityId": ",".join,
                    }

            elif name == "ToolView":
                aliasColumn = "Tool Name"
                mapping = {
                    aliasColumn: "first",
                    componentColumn: "first",
                    idColumn: ",".join,
                    pubsColumn: ",".join,
                    grantColumn: ",".join,
                    studyColumn: ",".join,
                    datasetsColumn: ",".join,
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
                    "Tool Doi": "first",
                    "Tool Date Last Modified": "first",
                    "Tool Release Date": "first",
                    "Tool Package Dependencies": "first",
                    "Tool Package Dependencies Present": "first",
                    "Tool Compute Requirements": "first",
                    "Tool Entity Name": "first",
                    "Tool Entity Type": "first",
                    "Tool Entity Role": "first",
                    "entityId": ",".join
                }

        elif name == "EducationalResource":
            aliasColumn = "Resource Alias"
            mapping = {
                aliasColumn: "first",
                componentColumn: "first",
                idColumn: ",".join,
                pubsColumn: ",".join,
                grantColumn: ",".join,
                studyColumn: ",".join,
                datasetsColumn: ",".join,
                toolsColumn: ",".join,
                "Resource Title": "first",
                "Resource Link": "first",
                "Resource Doi": "first",
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

        mergedTable = (  # group rows by designated identifier and map attributes
            table.groupby(aliasColumn, as_index=False).agg(mapping).reset_index()
        )
        mergedTable = mergedTable.iloc[:, 1:]  # remove unnecessary "id" column
        
        mergePath = Path(f"output/{name}/{name}_merged.csv")
        mergePath.parent.mkdir(parents=True, exist_ok=True)
        mergedTable.to_csv(mergePath, index=False)

        groups.append(mergePath)
        names.append(nameParts[0])

    return list(zip(groups, names))


def get_ref_tables(syn: synapseclient.login, args: list[tuple[Path, str]]) -> list[tuple[Path, Path, str]]:

    tables, names = zip(*args)

    ref_paths = []
    table_paths = []
    ref_names = []

    for table, name in zip(tables, names):

        if name == "PublicationView":
            ref = "syn53478776"

        elif name == "DatasetView":
            ref = "syn53478774"

        elif name == "ToolView":
            ref = "syn53479671"

        elif name == "EducationalResource":
            ref = "syn53651540"

        ref_table = syn.get(ref, downloadLocation=f"output/{name}")
        ref_paths.append(ref_table.path)
        table_paths.append(table)
        ref_names.append(name)

    return list(zip(ref_paths, table_paths, ref_names))


def compare_and_subset_tables(args: list[tuple[Path, Path, str]], strict: bool, debug: bool) -> list[tuple[Path, str]]:

    current, updated, names = zip(*args)

    updatePaths = []
    updateNames = []

    for ref, new, name in zip(current, updated, names):

        if name == "PublicationView":
            key = ["Pubmed Id"]
            if strict:
                cols = [
                    "PublicationView_id",
                    "Pubmed Id",
                    "Pubmed Url",
                    "Publication Assay",
                    "Publication Tissue",
                    "Publication Tumor Type",
                    "Publication Accessibility",
                ]
            else:
                cols = ["Pubmed Id", "Publication Accessibility"]

        elif name == "DatasetView":
            key = ["Dataset Alias"]
            if strict:
                cols = [
                    "DatasetView_id",
                    "Dataset Alias",
                    "Dataset Assay",
                    "Dataset Tissue",
                    "Dataset Tumor Type",
                    "Dataset File Formats",
                    "Dataset Url",
                    "Dataset Species",
                ]
            else:
                cols = ["DatasetView_id", "Dataset Alias", "Dataset Url"]

        elif name == "ToolView":
            key = ["Tool Name"]
            if strict:
                cols = [
                    "Tool Name",
                    "ToolView_id",
                    "Tool Homepage",
                    "Tool Type",
                    "Tool Topic",
                    "Tool Language",
                    "Tool Documentation Url",
                    "Tool Documentation Type",
                ]
            else:
                cols = ["Tool Name"]

        elif name == "EducationalResource":
            key = ["Resource Alias"]
            cols = ["Resource Alias", "EducationalResource_id"]

        current_table = pd.read_csv(ref, header=0).sort_values(by=key).fillna("")
        new_table = pd.read_csv(new, header=0).sort_values(by=key).fillna("")
        
        if debug:
            current_table["Source"] = "Database"
            new_table["Source"] = "Updated"

        tables = [current_table, new_table]

        updated = (
            pd.concat(tables, ignore_index=True).reset_index(drop=True).astype(str)
        )
        updated.drop_duplicates(
            subset=cols, keep=False, ignore_index=True, inplace=True
        )

        updatePath = Path(f"output/{name}/{name}_updated.csv")
        updatePath.parent.mkdir(parents=True, exist_ok=True)

        updated.to_csv(updatePath, index=False)

        updatePaths.append(updatePath)
        updateNames.append(name)

    return list(zip(updatePaths, updateNames))


def validate_tables(args: list[tuple[Path, str]], config: str) -> list[tuple[str, Path, str]]:

    paths, names = zip(*args)

    validNames = []
    validOuts = []
    validPaths = []

    for path, name in zip(paths, names):
        # pass config, datatype, and CSV path(s) to schematic for validation
        command = [
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

        print(f"\n\nValidating manifest at: {str(path)}...")

        outPath = Path(f"output/{name}/{name}_out.txt")
        outPath.parent.mkdir(parents=True, exist_ok=True)

        errPath = Path(f"output/{name}/{name}_error.txt")
        errPath.parent.mkdir(parents=True, exist_ok=True)
        # store logs from schematic validation
        commandOut = open(outPath, "w")
        errOut = open(errPath, "w")

        subprocess.run(command, text=True, check=True, stdout=commandOut, stderr=errOut)

        validNames.append(name)
        validOuts.append(outPath)
        validPaths.append(path)

    return list(zip(validNames, validOuts, validPaths))


def parse_out(args: list[tuple[str, Path, str]]) -> list[tuple[str, Path, str]]:

    names, outs, paths = zip(*args)

    parsedNames = []
    parsedOuts = []
    parsedPaths = []

    for name, out, path in zip(names, outs, paths):

        parsePath = Path(f"output/{name}/{name}_trim_config.csv")
        parsePath.parent.mkdir(parents=True, exist_ok=True)
        parsed = pd.read_table(out, sep="], ", header=None, engine="python")  # load output from schematic validation
        parsed.to_csv(  # convert log to useable format
            parsePath, index=False, sep="\n", header=False, columns=None, quoting=None
        )

        parsedNames.append(name)
        parsedOuts.append(parsePath)
        parsedPaths.append(path)

    return list(zip(parsedNames, parsedOuts, parsedPaths))


def trim_tables(args: list[tuple[str, Path, str]]) -> list[Path]:

    trimmedTables = []

    names, outs, paths = zip(*args)

    for name, out, path in zip(names, outs, paths):
        trimPath = Path(f"output/{name}/{name}_trimmed.csv")
        trimPath.parent.mkdir(parents=True, exist_ok=True)

        validationTable = pd.read_csv(out, header=None)
        processedTable = pd.read_csv(path, header=0)

        flaggedRows = (
            validationTable.iloc[:, 0]
            .str.extract(r"(\d{1,5})", expand=False)
            .astype(int)
        )
        flaggedRows = sorted(set(flaggedRows))
        flaggedRows = [(x - 2) for x in flaggedRows]  # offset for schematic output format
        print(f"The following rows have been flagged for trimming: {flaggedRows}")

        trimmedTable = processedTable.drop(flaggedRows, inplace=False)

        trimmedTable.to_csv(trimPath, index=False, header=True)

        trimmedTables.append(trimPath)

    return trimmedTables


def main():

    args = get_args()
    syn = synapseclient.login()

    inputList, config, trimList, inputManifest, merge, trim, strict, debug = (
        args.l,
        args.c,
        args.bl,
        args.tp,
        args.m,
        args.t,
        args.s,
        args.db
    )

    if trimList is None:
        if inputManifest is None:
            print("Accessing requested tables...")
            newTables = get_tables(syn, inputList, merge)
            print(
                """\n\nTable(s) downloaded from Synapse
                and stored as CSVs in output folder!"""
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
            print(
                f"""\n\nReading provided table
                at {inputManifest} of type {name}."""
            )

        if merge:
            print("\n\nMerging rows with matching identifier...")
            newTables = combine_rows(newTables)
            print("\n\nMatching rows merged!")
            print(
                """\n\nMerged table(s) converted to CSV
                and stored in local output folder!"""
            )
            print("\n\nValidating merged manifest(s)...")
        else:
            print("\n\nValidating unmerged manifest(s)...")

        refTables = get_ref_tables(syn, newTables)
        updatedTables = compare_and_subset_tables(refTables, strict, debug)
        checkTables = validate_tables(updatedTables, config)
        print("\n\nValidation logs stored in local output folder!")
        print("\n\nConverting validation logs to trim config files...")
        print(checkTables)
        validEntries = parse_out(checkTables)
        print("\n\nValidation logs parsed and saved as CSVs!")
        
        if trim:
            print("\n\nTrimming invalid entries from manifests...")
            trim_tables(validEntries)
            print("\n\nInvalid entries trimmed!")
        else:
            print(
                """\n\nNo trimming performed.
                  Manifests may contain invalid entries."""
            )

    if trimList is not None:
        if inputManifest is not None:
            names, outs, paths = [], [], []
            name = re.search("\/(\w*)(_trim_config)", str(trimList))
            if name is None:
                print(
                    """\n\nPlease provide a trim config that
                    uses the expected naming convention."""
                )
                exit
            else:
                print(
                    f"""\n\nThe file {str(inputManifest)}
                    will be trimmed based on {str(trimList)}"""
                )
                names.append(name[1])
                out = str(trimList)
                outs.append(out)
                path = str(inputManifest)
                paths.append(path)
                validEntries = list(zip(names, outs, paths))
                print("\n\nTrimming invalid entries from manifests...")
                trim_tables(validEntries)
                print("\n\nInvalid entries trimmed!")

        elif inputManifest is None:
            print(
                """\n\nNo manifest provided.
                Please designate a manifest to trim."""
            )


if __name__ == "__main__":
    main()
