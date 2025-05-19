"""Clean and prep MC2 database tables for backpopulation

This script will reorder and modify database table manifest columns
to match the respective View-type schema.

author: orion.banks
"""

import argparse
import pandas as pd
import synapseclient
import sys
import re

def get_args():
    """Set up command-line interface and get arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d",
        type=str,
        help="Dataset metadata CSV",
        required=True,
        default=None,
        ),
    parser.add_argument(
        "-t",
        type=str,
        help="Target template name",
        required=True,
        default=None,
    )
    parser.add_argument(
        "-m",
        type=str,
        help="Target-to-source mapping CSV",
        required=True,
        default=None,
    )
    return parser.parse_args()


def get_table(syn, source_id: str, cols: str | list = "*") -> pd.DataFrame:
    """Collect columns from a Synapse table entity and return as a Dataframe."""

    if type(cols) == list:
        cols = ", ".join(["".join(['"', col, '"']) for col in cols])

    query = f"SELECT {cols} FROM {source_id}"
    table = syn.tableQuery(query).asDataFrame().fillna("")
    print(f"Data acquired from Synapse table {source_id}")

    return table


def extract_lists(df: pd.DataFrame, list_columns, pattern) -> pd.DataFrame:
    """Extract bracketed/quoted lists from sheets."""

    for col in list_columns:

        df[col] = (
            df[col]
            .apply(lambda x: re.findall(pattern, x))
            .str.join(", "))
        
    return df


def main():
    """Main function."""
    
    args = get_args()

    manifests, target_output, mapping = args.d, args.t, args.m

    syn = synapseclient.login()

    manifests_df = pd.read_csv(manifests, header=0).fillna("")
    mapping_df = pd.read_csv(mapping, header=0).fillna("")

    source_metadata_dict = dict(zip(manifests_df["Component"], manifests_df["Table_syn_id"]))

    gc_template_dict = dict(zip(mapping_df["Property"], (zip(mapping_df["Node"], mapping_df["Acceptable Values"]))))

    gc_mc2_mapping_dict = dict(zip(mapping_df["Property"], mapping_df["MC2 attribute"]))

    for type, table in source_metadata_dict.items():
        table_df = get_table(syn, table, cols="*")
        source_metadata_dict[type] = (table_df, table_df.columns.tolist())

    template_df = pd.DataFrame()
    
    for attribute, (template, valid_values) in gc_template_dict.items():
        if template == target_output:
            template_df[attribute] = ""
            for component, (df, cols) in source_metadata_dict.items():
                if gc_mc2_mapping_dict[attribute] in cols:
                    template_df[attribute] = df[gc_mc2_mapping_dict[attribute]]

    template_df.to_csv("mapped_metadata.csv", index=False)

if __name__ == "__main__":
    main()
