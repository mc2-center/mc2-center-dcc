"""merge_and_correct_manifests.py

This script performs the following operations on a manifest CSV:
- replaces entries based on a primary key field, using a corrected manifest
- fills empty cells in the updated database with 'Pending Annotation' or 'Not Applicable' for specific columns, depending on the data type.
- removes leading and trailing whitespace for list columns in Publication View and Dataset View metadata

author: orion.banks
"""

import argparse
from datetime import datetime
import os
import pandas as pd
import re

def get_args():
    """Set up command-line interface and get arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d",
        type=str,
        help="Path to merged metadata table with erroneous entries.",
        required=True,
        default=None,
    )
    parser.add_argument(
        "-n",
        type=str,
        help="Path to CSV with corrected metadata entries.",
        required=False,
        default=None,
    )
    return parser.parse_args()


def filter_updated_manifest(new_entries_df: pd.DataFrame, index_col: str, data_type: str) -> pd.DataFrame:
    """Update the database DataFrame with new entries based on the index column."""
    filtered_entries_df = pd.DataFrame(columns=new_entries_df.columns)
    updated_entries_df = pd.DataFrame(columns=new_entries_df.columns)
    updated_count = 0

    index_groups = new_entries_df.groupby(index_col, as_index=False)
    for name, rows in index_groups.groups.items():
        row_to_keep = index_groups.get_group(name)
        if len(rows) > 1:
            row_to_keep = row_to_keep[row_to_keep["Source"].isin(["Database"])]
        else:
            if row_to_keep["Source"].isin(["Updated"]).all():
                updated_entries_df = pd.concat([updated_entries_df, row_to_keep])
                updated_count += 1
        filtered_entries_df = pd.concat([filtered_entries_df, row_to_keep])

    updated_entries_df.to_csv(f"{os.getcwd()}/{data_type}_new_rows_{datetime.now().strftime('%Y%m%d')}.csv", index=False)

    return filtered_entries_df

def update_database(database_df: pd.DataFrame, new_entries_df: pd.DataFrame, index_col: str) -> pd.DataFrame:
    """Update the database DataFrame with new entries based on the index column."""
    database_df["index_col"] = database_df[index_col]
    new_entries_df["index_col"] = new_entries_df[index_col]
    database_df.set_index("index_col", inplace=True)
    new_entries_df.set_index("index_col", inplace=True)

    database_df.update(new_entries_df)

    return database_df


def fill_empty_cells(updated_database: pd.DataFrame, data_type: str) -> pd.DataFrame:
    """Fill empty cells in the updated database with 'Pending Annotation' or 'Not Applicable'."""
    
    cols_to_fill = [
        "Publication Assay",
        "Publication Tumor Type",
        "Publication Tissue",
        "Publication Dataset Alias",
        "PublicationView Key",
        "Dataset Description",
        "Dataset Design",
        "Dataset Assay",
        "Dataset Species",
        "Dataset Tissue",
        "Dataset Tumor Type",
        "Dataset File Formats",
        "Data Use Codes"       
    ]

    current_cols_to_fill = [col for col in updated_database.columns if col in cols_to_fill]

    for _,row in updated_database.iterrows():
        for col in current_cols_to_fill:
            if pd.isna(row[col]) or row[col] == "" or re.match(r"^,+", row[col]):
                if data_type == "PublicationView" and row["Publication Accessibility"] == "Open Access":
                    value = "Not Applicable"
                else:
                    value = "Pending Annotation"
                updated_database.at[row.name, col] = value

    return updated_database

def trim_whitespace(database: pd.DataFrame) -> pd.DataFrame:
    """Remove leading and trailing whitespace from entries in curated columns."""
    
    cols_to_clean = [
        "Publication Assay",
        "Publication Tumor Type",
        "Publication Tissue",
        "Dataset Assay",
        "Dataset Species",
        "Dataset Tissue",
        "Dataset Tumor Type"
    ]
    
    clean_cols = [col for col in database.columns if col in cols_to_clean]
    
    for _,row in database.iterrows():
        for col in clean_cols:
            database.at[row.name, col] = row[col].strip()

    return database

def fix_pub_doi(database: pd.DataFrame) -> pd.DataFrame:
    """Add 'https://doi.org/' to existing identifiers or record 'No DOI Listed' if cell is empty."""

    for _,row in database.iterrows():
        if row["Publication Doi"].startswith("https://doi.org/") is False:
            if row["Publication Doi"] == "":
                value = "No DOI Listed"
            else:
                value = "".join(["https://doi.org/", row["Publication Doi"]])
            database.at[row.name, "Publication Doi"] = value
            
    return database

def main():
    """Main function to merge and clean manifests."""
    args = get_args()

    database, new_entries = args.d, args.n

    for sheet in [database, new_entries]:
        if sheet is not None:
            if os.path.exists(sheet):
                continue
            else:
                print(f"\n❗❗❗ The file {sheet} does not exist! ❗❗❗")
                exit()

    data_type = os.path.basename(database).split("_")[0]
    
    index_col_dict = {"PublicationView": "Pubmed Id",
                      "DatasetView": "Dataset Alias",
                      "ToolView": "ToolView_id",
                      "EducationalResource": "EducationalResource_id"}
    
    index_col = index_col_dict.get(data_type)

    database_df = pd.read_csv(database, keep_default_na=False, index_col=False)
    print(f"\nDatabase read successfully!")
    
    if new_entries is not None:
        new_entries_df = pd.read_csv(new_entries, keep_default_na=False, index_col=False)
        print(f"\nNew_entries read successfully!")
        filtered_entries_df = filter_updated_manifest(new_entries_df, index_col, data_type)
        filtered_entries_df.drop(["entityId", "iconTags", "Source"], axis=1, errors="ignore", inplace=True)
        updated_database = update_database(database_df, filtered_entries_df, index_col)
        print(f"\nDatabase has been successfully updated!")
    else:
        print(f"\nCurrent database will not be updated with corrected entries.")
        updated_database = database_df

    updated_database = fill_empty_cells(updated_database, data_type)
    print(f"\nEmpty cells filled successfully!")
    updated_database = trim_whitespace(updated_database)
    print(f"\nWhitespace removed from list entries!")
    
    if data_type == "PublicationView":
        updated_database = fix_pub_doi(updated_database)

    updated_database_path = f"{os.getcwd()}/{data_type}_merged_corrected.csv"
    updated_database.to_csv(path_or_buf=updated_database_path, index=False)
    print(f"\nUpdated database has been saved\nPath: {updated_database_path}")


if __name__ == "__main__":
    main()
