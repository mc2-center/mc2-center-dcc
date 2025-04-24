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
    parser.add_argument("-p", help="path to mapping CSV")
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


def combine_rows(args: list[tuple[pd.DataFrame | str, str]], mapping: pd.DataFrame) -> list[tuple[Path, str]]:

    newTables, newNames = zip(*args)  # unpack the input

    groups = []
    names = []
    
    for table, name in zip(newTables, newNames):
        table = table.astype(str)  # make everything strings so they can be joined as needed
        mappingDict = {}
        
        for _, row in mapping.iterrows():
            if row["component"] == name:
                mappingDict[row["attribute"]] = row["mapping"]
                if row["tag"] == "aliasColumn":
                    aliasColumn = row["attribute"]
        
        for k, v in mappingDict.items():
            if v == '",".join':
                mappingDict[k] = ",".join
        
        mergedTable = (  # group rows by designated identifier and map attributes
            table.groupby(aliasColumn, as_index=False).agg(mappingDict).reset_index()
        )
        mergedTable = mergedTable.iloc[:, 1:]  # remove unnecessary "id" column
        
        mergePath = Path(f"output/{name}/{name}_merged.csv")
        mergePath.parent.mkdir(parents=True, exist_ok=True)
        mergedTable.to_csv(mergePath, index=False)

        groups.append(mergePath)
        names.append(name)

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


def compare_and_subset_tables(args: list[tuple[Path, Path, str]], mapping: pd.DataFrame, strict: bool, debug: bool) -> list[tuple[Path, str]]:

    current, updated, names = zip(*args)

    updatePaths = []
    updateNames = []

    filter = "strict" if strict else "all"

    for ref, new, name in zip(current, updated, names):
        cols = []
        for _, row in mapping.iterrows():
            if row["component"] == name:
                if row["filter"] in set([filter, "all"]):
                    cols.append(row["attribute"])
                if row["tag"] == "aliasColumn":
                    key = row["attribute"]

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

    inputList, config, attributeMap, trimList, inputManifest, merge, trim, strict, debug = (
        args.l,
        args.c,
        args.p,
        args.bl,
        args.tp,
        args.m,
        args.t,
        args.s,
        args.db
    )

    mapping = pd.read_csv(attributeMap, header=0)

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
            newTables = combine_rows(newTables, mapping)
            print("\n\nMatching rows merged!")
            print(
                """\n\nMerged table(s) converted to CSV
                and stored in local output folder!"""
            )
            print("\n\nValidating merged manifest(s)...")
        else:
            print("\n\nValidating unmerged manifest(s)...")

        refTables = get_ref_tables(syn, newTables)
        updatedTables = compare_and_subset_tables(refTables, mapping, strict, debug)
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
