"""Add Publications to the Cancer Complexity Knowledge Portal (CCKP).

This script will sync over new publications and its annotations to the
Publications portal table.

author: verena.chung
"""

import os
import argparse
import getpass

import synapseclient
from synapseclient import Table


def login():
    """Log into Synapse. If env variables not found, prompt user.

    Returns:
        syn: Synapse object
    """
    try:
        syn = synapseclient.login(
            authToken=os.getenv('SYNAPSE_AUTH_TOKEN'),
            silent=True)
    except synapseclient.core.exceptions.SynapseNoCredentialsError:
        print("Credentials not found; please manually provide your",
              "Synapse username and password.")
        username = input("Synapse username: ")
        password = getpass.getpass("Synapse password: ")
        syn = synapseclient.login(username, password, silent=True)
    return syn


def get_args():
    """Set up command-line interface and get arguments."""
    parser = argparse.ArgumentParser(description="Add new pubs to the CCKP")
    parser.add_argument("-m", "--manifest",
                        type=str, required=True,
                        help="Synapse ID to the manifest table/fileview.")
    parser.add_argument("-t", "--portal_table",
                        type=str, default="syn21868591",
                        help=("Add publications to this specified "
                              "table. (Default: syn21868591)"))
    return parser.parse_args()


def add_missing_info(pubs, grants):
    """Add missing information into table before syncing.

    Returns:
        pubs: Data frame
    """
    pubs['Link'] = [
        "".join(["[PMID:", str(pmid), "](", url, ")"])
        for pmid, url
        in zip(pubs['Pubmed Id'], pubs['Pubmed Url'])
    ]
    for i, row in pubs.iterrows():
        grant_names = []
        for g in row['Publication Grant Number']:
            grant_names.append(
                grants[grants.grantNumber == g]['grantName'].values[0])
        pubs.at[i, 'GrantName'] = grant_names
    return pubs


def sync_table(syn, pubs, table):
    """Add pubs annotations to the Synapse table."""
    schema = syn.get(table)

    # Reorder columns to match the table order.
    col_order = [
        'Publication Doi', 'Publication Journal', 'Pubmed Id', 'Pubmed Url',
        'Link', 'Publication Title', 'Publication Year', 'Publication Keywords',
        'Publication Authors', 'Publication Assay', 'Publication Tumor Type',
        'Publication Tissue', 'Publication Theme Name', 'Publication Consortium Name',
        'Publication Grant Number', 'GrantName', 'Publication Dataset Alias'
    ]
    pubs = pubs[col_order]

    # Convert list column into string to match with table schema.
    pubs['Publication Dataset Alias'] = (
        pubs['Publication Dataset Alias']
        .apply(lambda x: ", ".join(map(str, x)))
    )

    new_rows = pubs.values.tolist()
    syn.store(Table(schema, new_rows))


def main():
    """Main function."""
    syn = login()
    args = get_args()

    manifest = (
        syn.tableQuery(f"SELECT * FROM {args.manifest}")
        .asDataFrame()
        .fillna("")
    )
    curr_pubs = (
        syn.tableQuery(f"SELECT pubMedId FROM {args.portal_table}")
        .asDataFrame()
        .pubMedId
        .to_list()
    )

    # Only add pubs not currently in the Publications table.
    new_pubs = manifest[~manifest['Pubmed Id'].isin(curr_pubs)]
    if new_pubs.empty:
        print("No new publications found!")
    else:
        print(f"{len(new_pubs)} new publications found!\n"
              "Adding new publications...")
        grants = (
            syn.tableQuery("SELECT grantNumber, grantName FROM syn21918972")
            .asDataFrame()
        )
        new_pubs = add_missing_info(new_pubs, grants)
        sync_table(syn, new_pubs, args.portal_table)
    print("DONE ✓")


if __name__ == "__main__":
    main()
