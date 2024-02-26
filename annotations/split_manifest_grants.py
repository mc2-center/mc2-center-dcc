"""Split Manifests CSV
This script will split a manifest csv by grant number and output
results into individual Excel or CSV files.
author: verena.chung
author: brynn.zalmanek
author: orion.banks
"""

import os
import argparse

import pandas as pd
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.styles import Font


def get_args():
    """Set up command-line interface and get arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("manifest", type=str, help="path of manifest to be split")
    parser.add_argument(
        "manifest_type",
        type=str,
        choices=["publication", "dataset", "tool", "project", "resource"],
        help="type of manifest to split, e.g. publicaiton",
    )
    parser.add_argument(
        "folder", type=str, help="folder path to save split manifests in"
    )
    parser.add_argument(
        "--csv",
        action="store_true",
        help="If this flag is provided, manifests will be output as CSV files with no CV sheet",
    )
    return parser.parse_args()


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


def split_manifest(df, manifest_type):
    """Split manifest into multiple manifests by grant number."""
    colname = f"{manifest_type.capitalize()} Grant Number"

    df[colname] = df[colname].str.split(", ")

    grouped = df.explode(colname).groupby(colname)
    print(f"Found {len(grouped.groups)} grant numbers in table " "- splitting now...")
    return grouped


def main():
    """Main function."""

    args = get_args()

    # Create output directory if it does not already exist.
    output_dir = args.folder
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Get latest CV terms to save as "standard_terms" - only keeping
    # the terms relevant to the manifest type.
    manifest_type = args.manifest_type
    annots = ["assay", "tissue", "tumorType"]

    cv_file = "https://raw.githubusercontent.com/mc2-center/data-models/main/all_valid_values.csv"
    cv_terms = pd.read_csv(cv_file)
    cv_terms = cv_terms.loc[
        cv_terms["category"].str.contains(manifest_type)
        | cv_terms["category"].isin(annots)
    ]

    # Read in manifest then split by grant number.  For each grant, generate a new
    # manifest as an Excel file.
    manifest = pd.read_csv(args.manifest)
    split_manifests = split_manifest(manifest, manifest_type)

    if manifest_type == "resource":
        manifest_type = "education"

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
    main()
