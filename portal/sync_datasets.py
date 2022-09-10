"""Add Datasets to the Cancer Complexity Knowledge Portal (CCKP).

This script will sync over new datasets and its annotations to the
Datasets portal table. A Synapse Folder will also be created for each
new dataset in its respective grant Project.

author: verena.chung
"""

import os
import argparse
import getpass

import synapseclient
from synapseclient import Table, Folder
import pandas as pd


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
    parser = argparse.ArgumentParser(
        description="Add new datasets to the CCKP")
    parser.add_argument("-m", "--manifest",
                        type=str, required=True,
                        help="Synapse ID to the manifest table/fileview.")
    parser.add_argument("-t", "--portal_table",
                        type=str, default="syn21897968",
                        help=("Add datasets to this specified "
                              "table. (Default: syn21897968)"))
    return parser.parse_args()

def create_folder(syn, name, parent):
    folder = Folder(name, parent=parent)
    folder = syn.store(folder)
    return folder.id

def add_missing_info(syn, datasets, grants, pubs):
    """Add missing information into table before syncing.

    Returns:
        datasets: Data frame
    """
    datasets['Link'] = [
        "".join(["[", d_id, "](", url, ")"])
        for d_id, url
        in zip(datasets['Dataset Alias'], datasets['Dataset Url'])
    ]
    for _, row in datasets.iterrows():
        grant_proj = grants[grants.grantNumber == row['Dataset Grant Number'][0]]['grantId'].values[0]
        folder_id = create_folder(syn, row['Dataset Alias'], grant_proj)
        datasets.at[_, 'Id'] = folder_id
        grant_names = []
        for g in row['Publication Grant Number']:
            grant_names.append(grants[grants.grantNumber == g]
                               ['grantName'].values[0])
        datasets.at[_, 'GrantName'] = grant_names
        pub_titles = []
        for p in row["Dataset Pubmed Id"]:
            pub_titles.append(pubs[pubs.pubMedId == int(p)]
                              ["publicationTitle"].values[0])
        datasets.at[_, "Pub"] = pub_titles
    return datasets

def sync_table(syn, datasets, table):
    """Add dataset annotations to the Synapse table."""
    schema = syn.get(table)

    # Reorder columns to match the table order.
    col_order = [
        'Id', 'Dataset Name', 'Dataset Alias', 'Dataset Description',
        'Dataset Design', 'Dataset Assay', 'Dataset Species', 'Dataset Tissue',
        'Dataset Tumor Type', 'Dataset Theme Name', 'Dataset Consortium Name',
        'Dataset Grant Number', 'GrantName', 'Dataset Pubmed Id', 'Pub', 'Link'
    ]
    datasets = datasets[col_order]

    new_rows = datasets.values.tolist()
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
    curr_datasets = (
        syn.tableQuery(
            f"SELECT datasetAlias, grantNumber FROM {args.portal_table}")
        .asDataFrame()
        .pubMedId
        .to_list()
    )

    # Only add datasets not currently in the Publications table.
    new_datasets = (
        pd.merge(
            manifest,
            curr_datasets,
            how="left",
            left_on=["Dataset Alias", "grantNumber"],
            right_on=["datasetAlias", "grantNumber"],
            indicator=True)
        .query("_merge=='both'")
    )
    if new_datasets.empty:
        print("No new publications found!")
    else:
        print(f"{len(new_datasets)} new publications found!\n"
              "Adding new publications...")
        grants = (
            syn.tableQuery("SELECT grantNumber, grantName FROM syn21918972")
            .asDataFrame()
        )
        pubs = (
            syn.tableQuery(
                "SELECT pubMedId, publicationTitle FROM syn21868591")
            .asDataFrame()
        )
        new_datasets = add_missing_info(syn, new_datasets, grants, pubs)
        sync_table(syn, new_datasets, args.portal_table)
    print("DONE ✓")


if __name__ == "__main__":
    main()
