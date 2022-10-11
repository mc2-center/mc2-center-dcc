"""Add Tools to the Cancer Complexity Knowledge Portal (CCKP).

This script will sync over new tools and its annotations to the
Tools portal table.

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
    parser = argparse.ArgumentParser(description="Add new tools to the CCKP")
    parser.add_argument("-m", "--manifest",
                        type=str, required=True,
                        help="Synapse ID to the manifest table/fileview.")
    parser.add_argument("-t", "--portal_table",
                        type=str, default="syn26127427",
                        help=("Add tools to this specified "
                              "table. (Default: syn26127427)"))
    parser.add_argument("--dryrun", action="store_true")
    return parser.parse_args()


def add_missing_info(tools):
    """Add missing information into table before syncing.

    Returns:
        tools: Data frame
    """
    tools['Link'] = "[Link](" + tools.toolHomepage + ")"
    tools['PortalDisplay'] = "true"
    return tools


def sync_table(syn, tools, table):
    """Add tools annotations to the Synapse table."""
    schema = syn.get(table)

    # Reorder columns to match the table order.
    col_order = [
        'toolName', 'toolDescription', 'toolHomepage', 'toolVersion',
        'toolGrantNumber', 'toolConsortiumName', 'toolPubmedId',
        'toolOperation', 'toolInputData', 'toolOutputData',
        'toolInputFormat', 'toolOutputFormat', 'toolFunctionNote',
        'toolCmd', 'toolType', 'toolTopic', 'toolOperatingSystem',
        'toolLanguage', 'toolLicense', 'toolCost', 'toolAccessibility',
        'toolDownloadUrl', 'Link', 'toolDownloadType', 'toolDownloadNote',
        'toolDownloadVersion', 'toolDocumentationUrl',
        'toolDocumentationType', 'toolDocumentationNote', 'toolLinkUrl',
        'toolLinkType', 'toolLinkNote', 'PortalDisplay'
    ]
    tools = tools[col_order]

    new_rows = tools.values.tolist()
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
    curr_tools = (
        syn.tableQuery(f"SELECT toolName FROM {args.portal_table}")
        .asDataFrame()
        .toolName
        .to_list()
    )

    # Only add tools not currently in the Tools table.
    new_tools = manifest[~manifest.toolName.isin(curr_tools)]
    if new_tools.empty:
        print("No new tools found!")
    else:
        print(f"{len(new_tools)} new tools found!\n")
        if args.dryrun:
            print(u"\u26A0", "WARNING:",
                  "dryrun is enabled (no updates will be done)\n")
        else:
            print("Adding new tools...")
            new_tools = add_missing_info(new_tools.copy())
            sync_table(syn, new_tools, args.portal_table)
    print("DONE ✓")


if __name__ == "__main__":
    main()
