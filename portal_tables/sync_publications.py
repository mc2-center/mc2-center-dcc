"""Add Publications to the Cancer Complexity Knowledge Portal (CCKP).

This script will sync over new publications and its annotations to the
Publications portal table.
"""

import re
import argparse
from typing import List

import pandas as pd
import utils


def get_args():
    """Set up command-line interface and get arguments."""
    parser = argparse.ArgumentParser(description="Add new pubs to the CCKP")
    parser.add_argument(
        "-m",
        "--manifest",
        type=str,
        default="syn53478776",
        help="Synapse ID to the staging version of publication database CSV.",
    )
    parser.add_argument(
        "-t",
        "--portal_table_id",
        type=str,
        default="syn21868591",
        help=("Add publications to this specified " "table. (Default: syn21868591)"),
    )
    parser.add_argument(
        "-p",
        "--table_path",
        type=str,
        default="./final_publication_table.csv",
        help=(
            "Path at which to store the final CSV. " "Defaults to './final_table.csv'"
        ),
    )
    parser.add_argument("--dryrun", action="store_true")
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="If this flag is provided, manifest, database, and new_table will "
        "be printed to the command line.",
    )
    return parser.parse_args()


def add_missing_info(
    pubs: pd.DataFrame, grants: pd.DataFrame, new_cols: List[str]
) -> pd.DataFrame:
    """Add missing information into table before syncing."""
    pubs.loc[:, "Link"] = [
        "".join(["[PMID:", str(pmid), "](", url, ")"])
        for pmid, url in zip(pubs["Pubmed Id"], pubs["Pubmed Url"])
    ]

    pattern = re.compile(r"(')([\s\w/-]+)(')")
    for col in new_cols:
        pubs[col] = ""
        for row in pubs.itertuples():
            i = row[0]
            n = row[4].split(",")
            extracted = []
            for g in n:
                if len(grants[grants.grantNumber == g][col].values) > 0:
                    values = str(grants[grants.grantNumber == g][col].values[0])

                    if col == "grantName":
                        extracted.append(values)
                    else:
                        matches = pattern.findall(values)
                        for m in matches:
                            extracted.append(m[1])
                else:
                    print(f"No match found for grant number: {g}")
                    continue

            clean_values = list(dict.fromkeys(extracted))
            pubs.at[i, col] = clean_values
    return pubs


def clean_table(df: pd.DataFrame) -> pd.DataFrame:
    """Clean up the table one final time."""

    # Convert string columns to string-list.
    for col in [
        "Publication Assay",
        "Publication Tumor Type",
        "Publication Tissue",
        "Publication Grant Number",
    ]:
        df[col] = utils.convert_to_stringlist(df[col])

    # Reorder columns to match the table order.
    col_order = [
        "Publication Doi",
        "Publication Journal",
        "Pubmed Id",
        "Pubmed Url",
        "Link",
        "Publication Title",
        "Publication Year",
        "Publication Keywords",
        "Publication Authors",
        "Publication Abstract",
        "Publication Assay",
        "Publication Tumor Type",
        "Publication Tissue",
        "theme",
        "consortium",
        "Publication Grant Number",
        "grantName",
        "Publication Dataset Alias",
        "Publication Accessibility",
        "entityId",
    ]
    return df[col_order]


def main():
    """Main function."""
    syn = utils.syn_login()
    args = get_args()

    new_cols = ["theme", "consortium", "grantName"]

    if args.dryrun:
        print(
            "Inputs will be processed and provided for review",
            "\n\nDatabase will NOT be updated.",
        )

    manifest = pd.read_csv(syn.get(args.manifest).path, header=0).fillna("")
    if args.verbose:
        print("🔍 Preview of manifest CSV:\n" + "=" * 72)
        print(manifest)
        print()

    print("\nProcessing publications staging database...")
    grants = syn.tableQuery(
        f"SELECT grantNumber, {','.join(new_cols)} FROM syn21918972"
    ).asDataFrame()

    database = add_missing_info(manifest, grants, new_cols)
    final_database = clean_table(database)
    if args.verbose:
        print("\n🔍 Publication(s) to be synced:\n" + "=" * 72)
        print(final_database)
        print()

    if not args.dryrun:
        utils.update_table(syn, args.portal_table_id, final_database)
        print()

    print(f"📄 Saving copy of final table to: {args.table_path}...")
    final_database.to_csv(args.table_path, index=False)
    print("\n\nDONE ✅")


if __name__ == "__main__":
    main()
