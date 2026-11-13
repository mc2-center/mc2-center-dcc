"""Split Manifests CSV
This script will split a manifest csv by grant number and output
results into individual Excel or CSV files.
author: verena.chung
author: brynn.zalmanek
author: orion.banks
"""

import os
import sys
import argparse
import logging

import pandas as pd
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.styles import Font

# removed when the mc2dcc package (CLI consolidation plan) lands
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "portal_tables"))
import model_contract
import utils

logger = logging.getLogger(__name__)

# Sheet names accepted when the input manifest is an Excel workbook, in order.
MANIFEST_SHEETS = ("manifest", "Sheet1")


def get_args(argv=None):
    """Set up command-line interface and get arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("manifest", type=str, help="path of manifest to be split")
    parser.add_argument(
        "manifest_type",
        type=str,
        choices=["publication", "dataset", "tool", "project", "resource", "grant"],
        help="type of manifest to split, e.g. publication",
    )
    parser.add_argument(
        "folder", type=str, help="folder path to save split manifests in"
    )
    parser.add_argument(
        "--csv",
        action="store_true",
        help="If this flag is provided, manifests will be output as CSV files with no CV sheet",
    )
    parser.add_argument(
        "--cv-file",
        type=str,
        default=model_contract.ALL_VALID_VALUES_URL,
        help="path or URL of all_valid_values.csv (default: the pinned data-models release)",
    )
    parser.add_argument(
        "-db",
        action="store_true",
        help="If this flag is provided, the input manifest will be processed as a backpopulation database",
    )
    return parser.parse_args(argv)


def read_manifest(path):
    """Read a manifest from CSV, or from the `manifest` / `Sheet1` sheet of an xlsx."""
    if str(path).lower().endswith((".xlsx", ".xlsm")):
        sheets = pd.ExcelFile(path).sheet_names
        for name in MANIFEST_SHEETS:
            if name in sheets:
                return pd.read_excel(path, sheet_name=name)
        raise ValueError(
            f"{path} has no sheet named one of {MANIFEST_SHEETS} (found {sheets})"
        )
    return pd.read_csv(path)


def load_cv_terms(manifest_type, cv_file=model_contract.ALL_VALID_VALUES_URL):
    """Return the valid values relevant to the manifest type."""
    annots = ["assay", "tissue", "tumorType"]
    cv_terms = pd.read_csv(cv_file)
    return cv_terms.loc[
        cv_terms["category"].str.contains(manifest_type)
        | cv_terms["category"].isin(annots)
    ]


def generate_manifest_as_excel(df, cv_terms, output):
    """Generate manifest file (xlsx) with given df."""
    wb = Workbook()
    ws = wb.active
    ws.title = "manifest"
    for r in dataframe_to_rows(df, index=False, header=True):
        ws.append(r)

    ws2 = wb.create_sheet("standard_terms")
    for row in dataframe_to_rows(cv_terms, index=False, header=True):
        ws2.append(row)

    # Style the worksheet.
    ft = Font(bold=True)
    ws2["A1"].font = ft
    ws2["B1"].font = ft
    ws2["C1"].font = ft
    ws2.column_dimensions["A"].width = 18
    ws2.column_dimensions["B"].width = 60
    ws2.column_dimensions["C"].width = 12
    ws2.protection.sheet = True
    wb.save(output)


def split_manifest(df, manifest_type, database):
    """Split manifest into multiple manifests by grant number."""
    if manifest_type != "grant":
        colname = "GrantView Key"
        if manifest_type == "publication" and colname not in df.columns:
            if "Publication Grant Number" in df.columns:
                logger.warning(
                    "Deprecated: splitting on legacy 'Publication Grant Number'; "
                    "the manifest should use 'GrantView Key'."
                )
                colname = "Publication Grant Number"
    else:
        colname = "Grant Number"

    if manifest_type != "education":
        idColumn = f"{manifest_type.capitalize()}View_id"

    else:
        idColumn = "EducationalResource_id"

    if database:
        cols = [colname, idColumn]
        df[idColumn] = df[idColumn].map(utils.split_cell)

    else:
        cols = [colname]

    df[colname] = df[colname].map(utils.split_cell)

    df = df.drop(["entityId", "Id"], axis=1, errors="ignore")

    grouped = df.explode(cols).groupby(colname)
    print(f"Found {len(grouped.groups)} grant numbers in table " "- splitting now...")
    return grouped


def main(argv=None):
    """Main function."""

    args = get_args(argv)

    # Create output directory if it does not already exist.
    output_dir = args.folder
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Get latest CV terms to save as "standard_terms" - only keeping
    # the terms relevant to the manifest type.
    manifest_type = args.manifest_type
    cv_terms = load_cv_terms(manifest_type, args.cv_file)

    if manifest_type == "resource":
        manifest_type = "education"

    # Read in manifest then split by grant number.  For each grant, generate a new
    # manifest as an Excel file.
    manifest = read_manifest(args.manifest)
    database = args.db
    split_manifests = split_manifest(manifest, manifest_type, database)

    for grant_number in split_manifests.groups:
        df = split_manifests.get_group(grant_number)
        grant_number = grant_number.translate(str.maketrans("", "", "[]:/!@#$<> "))
        if args.csv:
            path = os.path.join(output_dir, f"{grant_number}_{manifest_type}.csv")
            df.to_csv(path, index=False)
        else:
            path = os.path.join(output_dir, f"{grant_number}_{manifest_type}.xlsx")
            generate_manifest_as_excel(df, cv_terms, path)
    print("manifests split!")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    main()
