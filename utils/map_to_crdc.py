"""Map MC2 Center metadata to GC models.

This script maps metadata from MC2 Center to the Genomic Commons (GC) models.
It retrieves metadata from Synapse tables, extracts relevant information,
and generates a CSV file with the mapped metadata.
The script requires the following command-line arguments:
1. -d: Path to the dataset metadata CSV file. An example of this file can be found here: https://docs.google.com/spreadsheets/d/1LLpSIFAh12YdKnGfzXMxGpoKCaEH90nDx-QvncaIJlk/edit?gid=288959359#gid=288959359
2. -t: Target template name.
3. -m: Path to the target-to-source mapping CSV file.

author: orion.banks
"""

import argparse
import pandas as pd
import synapseclient
from synapseclient.models import query
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


def get_table(syn, source_id: str, cols: str | list = "*", conditions: str | None = None) -> pd.DataFrame:
    """Collect columns from a Synapse table entity and return as a Dataframe."""

    if type(cols) == list:
        cols = ", ".join(["".join(['"', col, '"']) for col in cols])

    query = f"SELECT {cols} FROM {source_id}"
    if conditions is not None:
        query += f" WHERE {conditions}"
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

    source_metadata_dict = dict(zip(manifests_df["entity_id"], (zip(manifests_df["data_type"], manifests_df["study_key"]))))

    gc_template_dict = dict(zip(mapping_df["Property"], (zip(mapping_df["Node"], mapping_df["Acceptable Values"]))))

    gc_mc2_mapping_dict = dict(zip(mapping_df["Property"], mapping_df["MC2 attribute"]))

    template_df = pd.DataFrame()
    
    for attribute, (template, _) in gc_template_dict.items():
        if template == target_output:
            template_df[attribute] = ""  # create GC template columns
            print(f"{attribute} added to template \n")
    
    template_df["crdc_id"] = ""
    attribute_list = template_df.columns.tolist()

    for id, (data_type, study_key) in source_metadata_dict.items():
        if data_type == "Study" and target_output in ["study", "image"]:
            df = get_table(syn, id, cols="*", conditions=f"Study_id = '{study_key}'")
        elif target_output != "study":
            if data_type not in ["Study"]:
                df = query(query=f"SELECT * FROM {id}")
        else:
            df = pd.DataFrame()
        source_metadata_dict[id] = (data_type, df, df.columns.tolist())

    for _, (data_type, df, cols) in source_metadata_dict.items():
        mapped_attributes = [attribute for attribute in attribute_list if "".join("".join(str(gc_mc2_mapping_dict[attribute]).split(" ")).split("-")) in cols]
        mapped_df = df.rename(columns={"".join("".join(str(gc_mc2_mapping_dict[attribute]).split(" ")).split("-")): attribute for attribute in mapped_attributes})
        template_df = pd.concat([template_df, mapped_df]).drop_duplicates(subset=attribute_list, keep="first").reset_index(drop=True)

    template_df[attribute_list].to_csv(f"{target_output}_mapped_metadata.csv", index=False)
    print(f"Mapped metadata saved to {target_output}_mapped_metadata.csv")

if __name__ == "__main__":
    main()
