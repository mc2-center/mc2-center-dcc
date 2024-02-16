"""Add People to the Cancer Complexity Knowledge Portal (CCKP).

This script will "sync" the person manifest table to the People
portal table, by first truncating the table, then re-adding the rows.
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
    parser = argparse.ArgumentParser(description="Add new tools to the CCKP")
    parser.add_argument("-m", "--manifest",
                        type=str, default="syn38301033",
                        help=("Synapse ID to the manifest table/fileview."
                              " (Default: syn38301033"))
    parser.add_argument("-t", "--portal_table",
                        type=str, default="syn28073190",
                        help=("Add people to this specified table."
                              " (Default: syn28073190)"))
    parser.add_argument("--dryrun", action="store_true")
    return parser.parse_args()


def add_missing_info(people, grants):
    """Add missing information into table before syncing.

    Returns:
        tools: Data frame
    """
    # First, convert profile IDs to int (User column type is stored as a float).
    people.synapseProfileId = (
        people.synapseProfileId
        .astype(str)
        .replace(r"\.0$", "", regex=True))

    people['grantName'] = ""
    for _, row in people.iterrows():
        # Link markdown to Synapse profile.
        if row.synapseProfileId != "":
            people.at[_, 'Link'] = "[Synapse Profile](https://www.synapse.org/#!Profile:" + \
                str(int(float(row['synapseProfileId']))) + ")"

        # Grant names.
        grant_names = []
        if row.personGrantNumber != ["Affiliated/Non-Grant Associated"]:
            for g in row['personGrantNumber']:
                grant_names.append(
                    grants[grants.grantNumber == g]['grantName'].values[0])
        people.at[_, 'grantName'] = grant_names
    return people


def update_table(syn, table_id, people):
    """Truncate table then add rows from latest manifest."""
    current_rows = syn.tableQuery(f"SELECT * FROM {table_id}")
    syn.delete(current_rows)

    # Convert stringlist to string (temporary workaround).
    people.personPublications = people.personPublications.str.join(", ")
    people.personDatasets = people.personDatasets.str.join(", ")
    people.personTools = people.personTools.str.join(", ")

    # Reorder columns to match the table order.
    col_order = [
        'name', 'alternativeNames', 'email', 'synapseProfileId', 'Link', 'url',
        'orcidId', 'lastKnownInstitution', 'grantName', 'personGrantNumber',
        'personConsortiumName', 'workingGroupParticipation', 'chairRoles',
        'personPublications', 'personDatasets', 'personTools',
        'consentForPortalDisplay', 'portalDisplay'
    ]
    people = people[col_order]
    new_rows = people.values.tolist()
    syn.store(Table(table_id, new_rows))


def main():
    """Main function."""
    syn = login()
    args = get_args()

    manifest = (
        syn.tableQuery(f"SELECT * FROM {args.manifest}")
        .asDataFrame()
        .fillna("")
    )
    curr_people = (
        syn.tableQuery(f"SELECT name FROM {args.portal_table}")
        .asDataFrame()
        .name
        .to_list()
    )

    # Only sync if new persons are found.
    new_persons = manifest[~manifest.name.isin(curr_people)]
    if new_persons.empty:
        print("No new persons found!")
    else:
        print(f"{len(new_persons)} new persons found!\n")
        if args.dryrun:
            print(u"\u26A0", "WARNING:",
                  "dryrun is enabled (no updates will be done)\n")
        else:
            print("Syncing over people...")
            grants = (
                syn.tableQuery(
                    "SELECT grantNumber, grantName FROM syn21918972")
                .asDataFrame()
            )
            manifest = add_missing_info(manifest, grants)
            update_table(syn, args.portal_table, manifest)
    print("DONE ✓")


if __name__ == "__main__":
    main()
