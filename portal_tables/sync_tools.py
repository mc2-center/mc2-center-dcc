"""Add Tools to the Cancer Complexity Knowledge Portal (CCKP).

This script will sync over new tools and its annotations to the
Tools portal table.
"""

import pandas as pd
import utils


def add_missing_info(tools: pd.DataFrame, grants: pd.DataFrame) -> pd.DataFrame:
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
    args = utils.get_args("tool")

    if args.dryrun:
        print("\n❗❗❗ WARNING:", "dryrun is enabled (no updates will be done)\n")

    manifest = pd.read_csv(syn.get(args.manifest_id).path).fillna("")
    manifest.columns = manifest.columns.str.replace(" ", "")
    if args.verbose:
        print("🔍 Preview of manifest CSV:\n" + "=" * 72)
        print(manifest)
        print()

    print("Processing dataset staging database...")
    grants = syn.tableQuery(
        "SELECT grantId, grantNumber, grantName, theme, consortium FROM syn21918972"
    ).asDataFrame()

    database = add_missing_info(manifest, grants)
    final_database = clean_table(database)
    if args.verbose:
        print("\n🔍 Tool(s) to be synced:\n" + "=" * 72)
        print(final_database)
        print()

    if not args.dryrun:
        utils.update_table(syn, args.portal_table_id, final_database)
        print()

    print(f"📄 Saving copy of final table to: {args.output_csv}...")
    final_database.to_csv(args.output_csv, index=False)
    print("\n\nDONE ✅")


if __name__ == "__main__":
    main()
