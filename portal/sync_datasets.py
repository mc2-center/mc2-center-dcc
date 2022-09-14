"""Add Datasets to the Cancer Complexity Knowledge Portal (CCKP).

This script will sync over new datasets and its annotations to the
Datasets portal table. A Synapse Folder will also be created for each
new dataset in its respective grant Project.

author: verena.chung
"""

import os
import argparse
import getpass

import re
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
    parser.add_argument("--dryrun", action="store_true")
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
    datasets['link'] = [
        "".join(["[", d_id, "](", url, ")"])
        for d_id, url
        in zip(datasets['datasetAlias'], datasets['datasetUrl'])
    ]
    for _, row in datasets.iterrows():
        if re.search(r"^syn\d+$", row['datasetAlias']):
            folder_id = row['datasetAlias']
        else:
            grant_proj = grants[grants.grantNumber ==
                                row['datasetGrantNumber'][0]]['grantId'].values[0]
            folder_id = ""
            folder_id = create_folder(syn, row['datasetAlias'], grant_proj)
        datasets.at[_, 'id'] = folder_id
        grant_names = []
        for g in row['datasetGrantNumber']:
            grant_names.append(grants[grants.grantNumber == g]
                               ['grantName'].values[0])
        datasets.at[_, 'GrantName'] = grant_names
        pub_titles = []
        for p in row["datasetPubmedId"]:
            pub_titles.append(pubs[pubs.pubMedId == int(p)]
                              ["publicationTitle"].values[0])
        datasets.at[_, "pub"] = pub_titles
    return datasets


def sync_table(syn, datasets, table):
    """Add dataset annotations to the Synapse table."""
    schema = syn.get(table)

    # Reorder columns to match the table order.
    col_order = [
        'id', 'datasetName', 'datasetAlias', 'datasetDescription',
        'datasetDesign', 'datasetAssay', 'datasetSpecies', 'datasetTissue',
        'datasetTumorType', 'datasetThemeName', 'datasetConsortiumName',
        'datasetGrantNumber', 'GrantName', 'datasetPubmedId', 'pub', 'link'
    ]
    datasets = datasets[col_order]

    new_rows = datasets.values.tolist()
    syn.store(Table(schema, new_rows))


def sort_and_stringify_col(col):
    """Sort list col then join together as comma-sep string."""
    return col.apply(lambda x: ", ".join(map(str, sorted(x))))


def main():
    """Main function."""
    syn = login()
    args = get_args()

    manifest = (
        syn.tableQuery(f"SELECT * FROM {args.manifest}")
        .asDataFrame()
        .fillna("")
    )
    manifest["grantNumber"] = sort_and_stringify_col(
        manifest["datasetGrantNumber"])
    curr_datasets = (
        syn.tableQuery(
            f"SELECT datasetAlias, grantNumber FROM {args.portal_table}")
        .asDataFrame()
    )
    curr_datasets["grantNumber"] = sort_and_stringify_col(
        curr_datasets["grantNumber"])

    # Only add datasets not currently in the Publications table.
    new_datasets = (
        pd.merge(
            manifest,
            curr_datasets,
            how="left",
            left_on=["datasetAlias", "grantNumber"],
            right_on=["datasetAlias", "grantNumber"],
            indicator=True)
        .query("_merge=='left_only'")
    )
    if new_datasets.empty:
        print("No new datasets found!")
    else:
        print(f"{len(new_datasets)} new datasets found!\n")
        if args.dryrun:
            print(u"\u26A0", "WARNING:",
                  "dryrun is enabled (no updates will be done)\n")
        else:
            print("Adding new datasets...")
            grants = (
                syn.tableQuery(
                    "SELECT grantId, grantNumber, grantName FROM syn21918972")
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
