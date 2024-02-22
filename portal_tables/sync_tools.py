"""Add Tools to the Cancer Complexity Knowledge Portal (CCKP).

This script will sync over new tools and its annotations to the
Tools portal table.
"""

import pandas as pd
import utils


def add_missing_info(tools: pd.DataFrame, grants: pd.DataFrame) -> pd.DataFrame:
    """Add missing information into table before syncing."""
    tools["link"] = "[Link](" + tools["ToolHomepage"] + ")"
    tools["portalDisplay"] = "true"
    tools["themes"] = ""
    tools["consortium"] = ""
    for _, row in tools.iterrows():
        themes = set()
        consortium = set()
        for g in row["ToolGrantNumber"].split(","):
            if g not in ["", "Affiliated/Non-Grant Associated"]:
                themes.update(grants[grants.grantNumber == g]["theme"].values[0])
                consortium.update(
                    grants[grants.grantNumber == g]["consortium"].values[0]
                )
        tools.at[_, "themes"] = list(themes)
        tools.at[_, "consortium"] = list(consortium)
    return tools


def clean_table(df: pd.DataFrame) -> pd.DataFrame:
    """Clean up the table one final time."""

    # Convert string columns to string-list.
    for col in [
        "ToolGrantNumber",
        "ToolOperation",
        "ToolInputData",
        "ToolOutputData",
        "ToolInputFormat",
        "ToolOutputFormat",
        "ToolType",
        "ToolTopic",
        "ToolOperatingSystem",
        "ToolLanguage",
        "ToolDownloadType",
        "ToolDocumentationType",
    ]:
        df[col] = utils.convert_to_stringlist(df[col])

    # Reorder columns to match the table order.
    col_order = [
        "ToolName",
        "ToolDescription",
        "ToolHomepage",
        "ToolVersion",
        "ToolGrantNumber",
        "consortium",
        "themes",
        "ToolPubmedId",
        "ToolOperation",
        "ToolInputData",
        "ToolOutputData",
        "ToolInputFormat",
        "ToolOutputFormat",
        "ToolFunctionNote",
        "ToolCmd",
        "ToolType",
        "ToolTopic",
        "ToolOperatingSystem",
        "ToolLanguage",
        "ToolLicense",
        "ToolCost",
        "ToolAccessibility",
        "ToolDownloadUrl",
        "link",
        "ToolDownloadType",
        "ToolDownloadNote",
        "ToolDownloadVersion",
        "ToolDocumentationUrl",
        "ToolDocumentationType",
        "ToolDocumentationNote",
        "ToolLinkUrl",
        "ToolLinkType",
        "ToolLinkNote",
        "portalDisplay",
    ]
    return df[col_order]


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
