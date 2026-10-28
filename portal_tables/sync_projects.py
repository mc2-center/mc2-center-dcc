"""Add Projects to the Cancer Complexity Knowledge Portal (CCKP).

This script will "sync" the project manifest table to the Grant portal
table, by first truncating the table, then re-adding the rows.
"""

import pandas as pd
from synapseclient.models import Table
import utils


def add_missing_info(
    projects: pd.DataFrame, grants: pd.DataFrame
) -> pd.DataFrame:
    """Add missing information into table before syncing."""
    new_cols = ["grantName", "themes", "consortia", "grantType"]
    projects[new_cols] = ""
    for _, row in projects.iterrows():
        grant_names = []
        themes = set()
        consortia = set()
        grant_type = ""
        for g in row["ProjectGrantNumber"].split(","):
            if g != "Affiliated/Non-Grant Associated":
                grant_names.append(
                    grants[grants.grantNumber == g]["grantName"].values[0]
                )
                themes.update(grants[grants.grantNumber == g]["theme"].values[0])
                consortia.update(
                    grants[grants.grantNumber == g]["consortium"].values[0]
                )
                grant_type = grants[grants.grantNumber == g]["grantType"].values[0]
        projects.at[_, "grantName"] = grant_names
        projects.at[_, "themes"] = list(themes)
        projects.at[_, "consortia"] = list(consortia)
        projects.at[_, "grantType"] = grant_type
    return projects


def clean_table(df: pd.DataFrame) -> pd.DataFrame:
    """Clean up the table one final time."""

    # Convert string columns to string-list.
    for col in [
        "ProjectGrantNumber",
        "grantType"
    ]:
        df[col] = utils.convert_to_stringlist(df[col])

    # Reorder columns to match the table order.
    col_order = [
        "ProjectName",
        "ProjectType",
        "ProjectDescription",
        "ProjectInvestigator",
        "themes",
        "consortia",
        "ProjectGrantNumber",
        "grantName",
        "grantType"
    ]
    return df[col_order]


def main():
    """Main function."""
    syn = utils.syn_login()
    args = utils.get_args("project")

    if args.dryrun:
        print("\n❗❗❗ WARNING:", "dryrun is enabled (no updates will be done)\n")

    manifest = Table(id=args.manifest_id).query(
        query=f"SELECT * FROM {args.manifest_id}", synapse_client=syn
    ).fillna("")
    manifest.columns = manifest.columns.str.replace(" ", "")
    if args.verbose:
        print("🔍 Preview of manifest CSV:\n" + "=" * 72)
        print(manifest)
        print()

    print("Processing project staging database...")
    grants = Table(id="syn21918972").query(
        query="SELECT grantId, grantNumber, grantName, theme, consortium, grantType FROM syn21918972",
        synapse_client=syn,
    )

    database = add_missing_info(manifest, grants)
    final_database = clean_table(database)
    if args.verbose:
        print("\n🔍 Grant(s) to be synced:\n" + "=" * 72)
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
