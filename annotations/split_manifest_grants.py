"""Split Manifests CSV
This script will split a manifest csv by grant number and output
results into individual CSVs.
author: verena.chung
author: brynn.zalmanek
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
    parser.add_argument("manifest",
                        type=str,
                        help="path of manifest to be split")
    parser.add_argument("manifest_type",
                        type=str,
                        choices=["publication", "dataset", "tool", "project"],
                        help="type of manifest to split, e.g. publicaiton")
    parser.add_argument("folder",
                        type=str,
                        help="folder path to save split manifests in")
    return parser.parse_args()


def generate_manifest_as_excel(table, cv_terms, output):
    """Generate manifest file (xlsx) with given publications data."""
    wb = Workbook()
    ws = wb.active
    ws.title = "manifest"
    for r in dataframe_to_rows(table, index=False, header=True):
        ws.append(r)

    ws2 = wb.create_sheet("standard_terms")
    for row in dataframe_to_rows(cv_terms, index=False, header=True):
        ws2.append(row)

    # Style the worksheet.
    ft = Font(bold=True)
    ws2["A1"].font = ft
    ws2["B1"].font = ft
    ws2["C1"].font = ft
    ws2.column_dimensions['A'].width = 18
    ws2.column_dimensions['B'].width = 60
    ws2.column_dimensions['C'].width = 12
    ws2.protection.sheet = True
    wb.save(output)


def split_manifest(df, manifest_type):
    """Split manifest into multiple manifests by grant number"""
    colname = f"{manifest_type.capitalize()} Grant Number"

    df[colname] = df[colname].str.split(", ")

    grouped = df.explode(colname).groupby(colname)
    print(f"Found {len(grouped.groups)} grant numbers in table "
          "- splitting now...")
    return grouped


def main():
    """Main function."""

    args = get_args()

    df = pd.read_csv(args.manifest)

    split_manifest(df, args.manifest_type, args.folder)

    # Read in manifest then split by grant number.  For each grant, generate a new
    # manifest as an Excel file.
    manifest = pd.read_csv(args.manifest)
    split_manifests = split_manifest(manifest, args.manifest_type)
    for grant_number in split_manifests.groups:
        df = split_manifests.get_group(grant_number)
        path = os.path.join(
            output_dir, f"{grant_number}_{args.manifest_type}.xlsx")
    print("manifests split!")


if __name__ == "__main__":
    main()