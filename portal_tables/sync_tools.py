"""Add Tools to the Cancer Complexity Knowledge Portal (CCKP).

This script will "sync" the tool manifest table to the Tool portal
table, by first truncating the table, then re-adding the rows.
"""

import pandas as pd
import re
import utils


def add_missing_info(tools: pd.DataFrame, grants: pd.DataFrame) -> pd.DataFrame:
    """Add missing information into table before syncing."""
    url_pattern = re.compile(".*(synapse\.org).*")
    tools["link"] = "[Link](" + tools["ToolHomepage"] + ")"
    tools["portalDisplay"] = "true"
    tools["themes"] = ""
    tools["consortium"] = ""
    tools["synapseLink"] = ""
    for _, row in tools.iterrows():
        themes = set()
        consortium = set()
        for g in row["GrantViewKey"].split(","):
            if g not in ["", "Affiliated/Non-Grant Associated"]:
                themes.update(grants[grants.grantNumber == g]["theme"].values[0])
                consortium.update(
                    grants[grants.grantNumber == g]["consortium"].values[0]
                )
        tools.at[_, "themes"] = list(themes)
        tools.at[_, "consortium"] = list(consortium)
        
        synapse_links = []
        for s in [row["ToolDownloadUrl"], row["ToolLinkUrl"], row["ToolHomepage"]]:
            s_match = re.match(url_pattern, s)
            if s_match:
                synapse_links.append("".join(["[Link](", s , ")"]))
        tools.at[_, "synapseLink"] = ", ".join(set(synapse_links))
        
    return tools


def clean_table(df: pd.DataFrame) -> pd.DataFrame:
    """Clean up the table one final time."""

    df = df.rename(columns={
        "GrantViewKey": "ToolGrantNumber",
        "PublicationViewKey": "ToolPubmedId",
        "DatasetViewKey": "ToolDatasets"
        })
    
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
        "ToolDatasets",
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
        "ToolDoi",
        "synapseLink",
        "ToolDateLastModified",
        "ToolReleaseDate",
        "ToolPackageDependencies",
        "ToolPackageDependenciesPresent",
        "ToolComputeRequirements",
        "ToolEntityName",
        "ToolEntityType",
        "ToolEntityRole"
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

    print("Processing tool staging database...")
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

    if not args.noprint:
        print(f"📄 Saving copy of final table to: {args.output_csv}...")
        final_database.to_csv(args.output_csv, index=False)
    print("\n\nDONE ✅")


if __name__ == "__main__":
    main()
