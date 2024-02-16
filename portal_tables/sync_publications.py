"""Add Publications to the Cancer Complexity Knowledge Portal (CCKP).

This script will sync over new publications and its annotations to the
Publications portal table.
"""

import argparse
from synapseclient import Table
import pandas as pd
import re
import utils

def get_args():
    """Set up command-line interface and get arguments."""
    parser = argparse.ArgumentParser(description="Add new pubs to the CCKP")
    parser.add_argument("-m", "--manifest",
                        type=str, default="syn53478776",
                        help="Synapse ID to the staging version of publication database CSV.")
    parser.add_argument("-t", "--portal_table",
                        type=str, default="syn21868591",
                        help=("Add publications to this specified "
                              "table. (Default: syn21868591)"))
    parser.add_argument("-p", "--table_path",
                        type=str, default="./final_table.csv",
                        help=("Path at which to store the final CSV. "
                              "Defaults to './final_table.csv'"))
    parser.add_argument("--dryrun", action="store_true")
    return parser.parse_args()


def add_missing_info(pubs, grants, new_cols):
    """Add missing information into table before syncing.

    Returns:
        pubs: Data frame
    """
    pubs.loc[:, 'Link'] = [
        "".join(["[PMID:", str(pmid), "](", url, ")"])
        for pmid, url
        in zip(pubs['Pubmed Id'], pubs['Pubmed Url'])
    ]
    
    pattern = re.compile("(\')([\s\w/-]+)(\')")
    
    for col in new_cols: 
        pubs[col] = ""
        for row in pubs.itertuples():
            i = row[0]
            n = row[4].split(",")
            extracted = []
            for g in n:
                if len(grants[grants.grantNumber == g][col].values) > 0:
                    values = str(grants[grants.grantNumber == g][col].values[0])
                    
                    if col == 'grantName':
                        extracted.append(values)
                    
                    else:
                        matches = pattern.findall(values)
                        for m in matches:
                            extracted.append(m[1])

                else:
                    print(f"No match found for grant number: {g}")
                    continue
            
            clean_values = list((dict.fromkeys(extracted)))

            pubs.at[i, col] = clean_values
    
    return pubs


def sync_table(syn, pubs, table, dryrun):
    """Add pubs annotations to the Synapse table."""
    schema = syn.get(table)

    # Reorder columns to match the table order.
    col_order = [
        'Publication Doi', 'Publication Journal', 'Pubmed Id', 'Pubmed Url',
        'Link', 'Publication Title', 'Publication Year', 'Publication Keywords',
        'Publication Authors', 'Publication Abstract', 'Publication Assay', 
        'Publication Tumor Type', 'Publication Tissue', 'theme',
        'consortium', 'Publication Grant Number', 'grantName',
        'Publication Dataset Alias', 'Publication Accessibility',
        'entityId'
    ]
    pubs = pubs[col_order]

    if dryrun:
        print(u"\u26A0", "WARNING:",
            "dryrun is enabled (no updates will be done)\n")

    else:
        print("Synchronizing publications staging database to production database...\n", pubs)
        table_rows = pubs.values.tolist()
        syn.store(Table(schema, table_rows))

    return pubs

def main():
    """Main function."""
    syn = utils.syn_login()
    args = get_args()

    new_cols = [
        'theme',
        'consortium',
        'grantName'
    ]

    if args.dryrun:
        print(
        "Inputs will be processed and provided for review", 
        "\n\nDatabase will NOT be updated."
    )
    
    manifest = (
        pd.read_csv(
        syn.get(args.manifest).path,
        header=0).fillna("")
    )

    print("\n\nCSV downloaded from Synapse:\n", manifest)

    grants = (
        syn.tableQuery(
            f"SELECT grantNumber, {','.join(new_cols)} FROM syn21918972")
            .asDataFrame()
    )
    
    print("\n\nProcessing publications staging database...")
        
    database = add_missing_info(manifest.copy(), grants, new_cols)
    print("\n\nTable with all requested columns added:\n", database)

    new_table = sync_table(syn, database.copy(), args.portal_table, args.dryrun)
    print("\n\nReordered final table - to be synced to database:\n", new_table)

    new_table.to_csv(args.table_path, index=False)
    print(f"Copy of final table stored at: {args.table_path}")
    print("DONE ✓")
        
if __name__ == "__main__":
    main()
