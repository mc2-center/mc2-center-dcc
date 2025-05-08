"""Clean and prep MC2 database tables for backpopulation

This script will reorder and modify database table manifest columns
to match the respective View-type schema.

author: orion.banks
"""

import argparse
import pandas as pd
import sys
import re

def get_args():
    """Set up command-line interface and get arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-t",
        type=str,
        help="Dataset metadata CSV",
        required=True,
    )
    parser.add_argument(
        "-v",
        type=str,
        help="Grant metadata CSV",
        required=False,
        default=None,
    )
    parser.add_argument(
        "-f",
        type=str,
        help="Consortium metadata CSV",
        required=False,
        default=None,
    )
    parser.add_argument(
        "-s",
        type=str,
        help="Study metadata CSV",
        required=False,
        default=None,
    )
    parser.add_argument(
        "-i",
        type=str,
        help="Target metadata CSV",
        required=False,
        default=None,
    )
    parser.add_argument(
        "-m",
        type=str,
        help="Target-to-source mapping CSV",
        required=False,
        default=None,
    )
    return parser.parse_args()

def extract_lists(df: pd.DataFrame, list_columns, pattern) -> pd.DataFrame:
    """Extract bracketed/quoted lists from sheets."""

    for col in list_columns:

        df[col] = (
            df[col]
            .apply(lambda x: re.findall(pattern, x))
            .str.join(", "))
        
    return df

def map_columns(df: pd.DataFrame, column_map: list[tuple]) -> pd.DataFrame:
    """Map outdated columns to new column names and drop old columns."""

    for start, end in column_map:

        df[f"{end}"] = [
            x for x in df[f"{start}"]
        ]

    return df


def main():
    """Main function."""
    
    args = get_args()

    dataset_input, grant_input, consortium_input, study_input, target_input, mapping = args.t, args.v, args.f, args.s, args.i, args.m

    dataset, grant, consortium, study = None, None, None, None

    source_metadata = [(dataset_input, dataset), (grant_input, grant), (consortium_input, consortium), (study_input, study)]

    target = pd.read_csv(target_input, header=0).fillna("")

    mapping = pd.read_csv(mapping, header=0).to_dict()

    for input_file in source_metadata:
        input_file[1] = pd.read_csv(input_file[0], header=0).fillna("")
        
    for col in target:
        for k, v in mapping.items():
            if col == k:
                source_col = v
        if source_col in dataset.columns:
            target[col] = dataset[source_col]
        elif source_col in grant.columns:
            target[col] = grant[source_col]
        elif source_col in consortium.columns:
            target[col] = consortium[source_col]
        elif source_col in study.columns:
            target[col] = study[source_col]

    target.to_csv("mapped_metadata.csv", index=False)

if __name__ == "__main__":
    main()
