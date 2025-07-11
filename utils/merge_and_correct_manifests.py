"""merge_and_correct_manifests.py

This script replaces entries in a metadata manifest based on a primary key field.
It also fills empty cells in the updated database with 'Pending Annotation' or 'Not Applicable'
for specific columns, depending on the data type.

author: orion.banks
"""

import argparse
import os
import pandas as pd

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
        required=True,
        default=None,
    )
    return parser.parse_args()


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

    for _,row in updated_database.iterrows():
        for col in updated_database.columns:
            if pd.isna(row[col]) or row[col] == "" and col in cols_to_fill:
                if data_type == "PublicationView" and row["Publication Accessibility"] == "Open Access":
                    value = "Not Applicable"
                else:
                    value = "Pending Annotation"
                updated_database.at[row.name, col] = value

    return updated_database


def main():
    """Main function to merge corrected manifests."""
    args = get_args()

    database, new_entries = args.d, args.n

    for sheet in [database, new_entries]:
        if os.path.exists(sheet):
            continue
        else:
            print(f"\n❗❗❗ The file {sheet} does not exist! ❗❗❗")
            exit()

    data_type = os.path.basename(database).split("_")[0]
    index_col = data_type + "_id"

    database_df = pd.read_csv(database, keep_default_na=False, index_col=False)
    print(f"\nDatabase read successfully!")
    new_entries_df = pd.read_csv(new_entries, keep_default_na=False, index_col=False)
    print(f"\nNew_entries read successfully!")

    updated_database = update_database(database_df, new_entries_df, index_col)
    print(f"\nDatabase has been successfully updated!")

    if data_type in ["PublicationView", "DatasetView"]:
        updated_database = fill_empty_cells(updated_database, data_type)
        print(f"\nEmpty cells filled successfully!")

    updated_database_path = f"{os.getcwd()}/{data_type}_merged_corrected.csv"
    updated_database.to_csv(path_or_buf=updated_database_path, index=False)
    print(f"\nUpdated database has been saved\nPath: {updated_database_path}")


if __name__ == "__main__":
    main()
