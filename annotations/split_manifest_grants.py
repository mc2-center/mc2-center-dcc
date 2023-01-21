"""Split Manifests CSV
This script will split a manifest csv by grant number and output
results into individual CSVs.
author: verena.chung
author: brynn.zalmanek
"""

import os
import argparse
import pandas as pd


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


def get_df(manifest_csv):
    """Convert manifest to data frame."""
    df = pd.read_csv(manifest_csv)
    print(df)
    return (df)


def split_manifest(df, type, directory):
    """Split manifest into multiple manifests by grant number"""
    colname = f"{type.capitalize()} Grant Number"

    grouped = df.groupby(colname)
    print(f"Found {len(grouped.groups)} grant numbers in table "
          "- splitting now...")

    for grant_number in grouped.groups:
        df = grouped.get_group(grant_number)
        df.to_csv(f'{directory}/{grant_number}.csv', index=False)


def main():
    """Main function."""

    args = get_args()

    df = get_df(args.manifest)

    split_manifest(df, args.manifest_type, args.folder)

    print("manifests split!")


if __name__ == "__main__":
    main()