"""Add Tools to the Cancer Complexity Knowledge Portal (CCKP).

This script will sync over new tools and its annotations to the
Tools portal table.
"""

import argparse

from synapseclient import Table
import utils


def get_args():
    """Set up command-line interface and get arguments."""
    parser = argparse.ArgumentParser(description="Update Tools Merged table for the CCKP")
    parser.add_argument(
        "-m",
        "--manifest_id",
        type=str,
        default="syn53479671",
        help="Synapse ID to the manifest CSV file.",
    )
    parser.add_argument(
        "-t",
        "--portal_table_id",
        type=str,
        default="syn26127427",
        help="Add tools to this specified table. (Default: syn26127427)",
    )
    parser.add_argument(
        "-o",
        "--output_csv",
        type=str,
        default="./final_tools_table.csv",
        help="Filepath to output CSV.",
    )
    parser.add_argument("--dryrun", action="store_true")
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Output all logs and interim tables.",
    )
    return parser.parse_args()


def add_missing_info(tools, grants):
    """Add missing information into table before syncing.

    Returns:
        tools: Data frame
    """
    tools['Link'] = "[Link](" + tools.toolHomepage + ")"
    tools['PortalDisplay'] = "true"
    tools['themes'] = ""
    tools['consortium'] = ""
    for _, row in tools.iterrows():
        themes = set()
        consortium = set()
        for g in row['toolGrantNumber']:
            themes.update(grants[grants.grantNumber == g]
                          ['theme'].values[0])
            consortium.update(grants[grants.grantNumber == g]['consortium'].values[0])
        tools.at[_, 'themes'] = list(themes)
        tools.at[_, 'consortium'] = list(consortium)
    return tools


def sync_table(syn, tools, table):
    """Add tools annotations to the Synapse table."""
    schema = syn.get(table)

    # Reorder columns to match the table order.
    col_order = [
        'toolName', 'toolDescription', 'toolHomepage', 'toolVersion',
        'toolGrantNumber', 'consortium', 'themes', 'toolPubmedId',
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
    syn = utils.syn_login()
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
    new_tools = manifest[~manifest['toolName'].isin(curr_tools)]
    if new_tools.empty:
        print("No new tools found!")
    else:
        print(f"{len(new_tools)} new tools found!\n")
        if args.dryrun:
            print(u"\u26A0", "WARNING:",
                  "dryrun is enabled (no updates will be done)\n")
            print(new_tools)
        else:
            print("Adding new tools...")
            grants = (
                syn.tableQuery(
                    "SELECT grantId, grantNumber, grantName, theme, consortium FROM syn21918972")
                .asDataFrame()
            )
            new_tools = add_missing_info(new_tools.copy(), grants)
            sync_table(syn, new_tools, args.portal_table)
    print("DONE ✓")


if __name__ == "__main__":
    main()
