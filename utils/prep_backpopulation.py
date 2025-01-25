"""Clean and prep MC2 database tables for backpopulation

This script will reorder and modify database table manifest columns
to match the respective View-type schema.

author: orion.banks
"""

import pandas as pd
import sys
import re

def add_missing_info(
    df: pd.DataFrame,
    name: str,
) -> pd.DataFrame:
    """Add missing information into table before syncing."""

    prefix = "https://doi.org/"

    if name == "PublicationView":
        for entry in df.itertuples():
            fixed = []
            i = entry[0]
            doi = entry[5]
            matches = re.match('https', doi)
            if matches is None:
                doi = "".join([
                    "".join([prefix, doi])
                ])
            fixed.append(doi)

            fixed_dois = list(dict.fromkeys(fixed))
            df.at[i, 'Publication Doi'] = "".join(fixed_dois)
    
    if name == "DatasetView":
        df["Data Use Codes"] = ""
    
    if name == "ToolView":
        for new_col in [
            "DatasetView Key",
            "Tool Date Last Modified",
            "Tool Release Date",
            "Tool Package Dependencies",
            "Tool Package Dependencies Present",
            "Tool Compute Requirements",
            "Tool Entity Name",
            "Tool Entity Type",
            "Tool Entity Role"]:
            
            df[f"{new_col}"] = ""

    df["Study Key"] = ""

    return df

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

def clean_table(df: pd.DataFrame, data) -> pd.DataFrame:
    """Clean up the table one final time."""

    # Reorder columns to match the table order.
    if data == "PublicationView":
        col_order = [
            "Component",
            "PublicationView_id",
            "GrantView Key",
            "Study Key",
            "Publication Doi",
            "Publication Journal",
            "Pubmed Id",
            "Pubmed Url",
            "Publication Title",
            "Publication Year",
            "Publication Keywords",
            "Publication Authors",
            "Publication Abstract",
            "Publication Assay",
            "Publication Tumor Type",
            "Publication Tissue",
            "Publication Accessibility",
            "Publication Dataset Alias",
            "entityId"
        ]

    elif data == "DatasetView":
        col_order = [
            "Component",
            "DatasetView_id",
            "GrantView Key",
            "Study Key",
            "PublicationView Key",
            "Dataset Name",
            "Dataset Alias",
            "Dataset Description",
            "Dataset Design",
            "Dataset Assay",
            "Dataset Species",
            "Dataset Tumor Type",
            "Dataset Tissue",
            "Dataset Url",
            "Dataset File Formats",
            "Data Use Codes",
            "entityId"
        ]

    elif data == "ToolView":
        col_order = [
            "Component",
            "ToolView_id",
            "GrantView Key",
            "Study Key",
            "DatasetView Key",
            "PublicationView Key",
            "Tool Name",
            "Tool Description",
            "Tool Homepage",
            "Tool Version",
            "Tool Operation",
            "Tool Input Data",
            "Tool Output Data",
            "Tool Input Format",
            "Tool Output Format",
            "Tool Function Note",
            "Tool Cmd",
            "Tool Type",
            "Tool Topic",
            "Tool Operating System",
            "Tool Language",
            "Tool License",
            "Tool Cost",
            "Tool Accessibility",
            "Tool Download Url",
            "Tool Download Type",
            "Tool Download Note",
            "Tool Download Version",
            "Tool Documentation Url",
            "Tool Documentation Type",
            "Tool Documentation Note",
            "Tool Link Url",
            "Tool Link Type",
            "Tool Link Note",
            "Tool Date Last Modified",
            "Tool Release Date",
            "Tool Package Dependencies",
            "Tool Package Dependencies Present",
            "Tool Compute Requirements",
            "Tool Entity Name",
            "Tool Entity Type",
            "Tool Entity Role",
            "entityId"
        ]

    return df[col_order]


def main():
    """Main function."""
    input = sys.argv[1]
    output = sys.argv[2]
    clean = sys.argv[3]

    list_columns = []

    pubs_column_map = [("Publication Grant Number", "GrantView Key")]

    datasets_column_map = [("Dataset Grant Number", "GrantView Key"), ("Dataset Pubmed Id", "PublicationView Key")]
    
    tools_column_map = [("Tool Grant Number", "GrantView Key"), ("Tool Pubmed Id", "PublicationView Key")]

    pattern = re.compile('"(.*?)"')

    manifest = pd.read_csv(input, header=0).fillna("")
    name = manifest.loc[:, "Component"].iat[1]
    
    if name == "PublicationView":
        column_map = pubs_column_map
    
    if name == "DatasetView":
        column_map = datasets_column_map

    if name == "ToolView":
        column_map = tools_column_map

    if clean is not None:
        database = add_missing_info(manifest, name)

        if len(list_columns) > 0:
            database = extract_lists(database, list_columns, pattern)

        if len(column_map) > 0:
            database = map_columns(database, column_map)

    else:
        database = manifest
    
    final_database = clean_table(database, name)
    
    print(f"📄 Saving copy of final table to: {output}...")
    
    final_database.to_csv(output, index=False)
    
    print("\n\nDONE ✅")


if __name__ == "__main__":
    main()
