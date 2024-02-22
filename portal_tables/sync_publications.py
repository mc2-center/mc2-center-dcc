"""Add Publications to the Cancer Complexity Knowledge Portal (CCKP).

This script will sync over new publications and its annotations to the
Publications portal table.
"""
import re
from typing import List

import pandas as pd
import utils


def add_missing_info(
    pubs: pd.DataFrame, grants: pd.DataFrame, new_cols: List[str]
) -> pd.DataFrame:
    """Add missing information into table before syncing."""
    pubs["link"] = [
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
        "link",
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
    args = utils.get_args("publication")

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
