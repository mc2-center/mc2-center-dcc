"""Add Tools to the Cancer Complexity Knowledge Portal (CCKP).

This script will "sync" the tool manifest table to the Tool portal
table, by first truncating the table, then re-adding the rows.
"""

import pandas as pd
import re
from synapseclient.models import Table
import crosswalk
import model_contract
import utils


def add_missing_info(tools: pd.DataFrame, grants: pd.DataFrame) -> pd.DataFrame:
    """Add missing information into table before syncing."""
    url_pattern = re.compile(".*(synapse\.org).*")
    tools["link"] = "[Link](" + tools["ToolHomepage"] + ")"
    tools["portalDisplay"] = "true"
    tools["theme"] = ""
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
        tools.at[_, "theme"] = list(themes)
        tools.at[_, "consortium"] = list(consortium)
        
        synapse_links = []
        for s in [row["ToolDownloadUrl"], row["ToolLinkUrl"], row["ToolHomepage"]]:
            s_match = re.match(url_pattern, s)
            if s_match:
                synapse_links.append("".join(["[Link](", s , ")"]))
        tools.at[_, "synapseLink"] = ", ".join(set(synapse_links))
        
        tools.at[_,"PublicationViewKey"] = tools.at[_,"PublicationViewKey"] or "Pending Annotation"
        
    return tools


def clean_table(df: pd.DataFrame, live_types: dict[str, str]) -> pd.DataFrame:
    """Convert to the portal table's shape and dedupe list cells."""

    if "iconTags" not in df.columns:
        df["iconTags"] = ""

    portal = crosswalk.to_portal(df, "ToolView", live_types)

    # Drop repeated values within each list cell, keeping first-seen order.
    for col in portal.columns:
        portal[col] = portal[col].map(
            lambda v: list(dict.fromkeys(v)) if isinstance(v, list) else v
        )

    return portal.sort_values(by="pubMedId", ascending=False)


def main():
    """Main function."""
    syn = utils.syn_login()
    args = utils.get_args("tool")

    if args.dryrun:
        print("\n❗❗❗ WARNING:", "dryrun is enabled (no updates will be done)\n")

    manifest = model_contract.read_manifest(syn, args.manifest_id).fillna("")
    manifest = model_contract.rename_legacy_headers(manifest, "ToolView")
    manifest = model_contract.normalize_headers(manifest, to="keys")
    if args.verbose:
        print("🔍 Preview of manifest CSV:\n" + "=" * 72)
        print(manifest)
        print()

    print("Processing tool staging database...")
    grants = Table(id="syn21918972").query(
        query="SELECT grantId, grantNumber, grantName, theme, consortium FROM syn21918972",
        synapse_client=syn,
    )

    database = add_missing_info(manifest, grants)
    live_types = crosswalk.check_crosswalk(syn, args.portal_table_id, "ToolView")
    final_database = clean_table(database, live_types)
    if args.verbose:
        print("\n🔍 Tool(s) to be synced:\n" + "=" * 72)
        print(final_database)
        print()

    if not args.dryrun:
        utils.update_table(
            syn, args.portal_table_id, final_database, component="ToolView"
        )
        print()

    if not args.noprint:
        print(f"📄 Saving copy of final table to: {args.output_csv}...")
        final_database.to_csv(args.output_csv, index=False)
    print("\n\nDONE ✅")


if __name__ == "__main__":
    main()
