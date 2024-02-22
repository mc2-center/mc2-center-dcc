"""Add Grants to the Cancer Complexity Knowledge Portal (CCKP).

This script will "sync" the grant manifest table to the Grant portal
table, by first truncating the table, then re-adding the rows.
"""

import pandas as pd
import utils


def add_missing_info(grants: pd.DataFrame) -> pd.DataFrame:
    """Add missing information into table before syncing."""
    grants.loc[:, "project_id"] = grants["GrantSynapseProject"].str.extract(
        r":(syn\d*)/?"
    )
    return grants


def clean_table(df: pd.DataFrame) -> pd.DataFrame:
    """Clean up the table one final time."""

    # Convert string columns to string-list.
    for col in [
        "GrantThemeName",
        "GrantInstitutionAlias",
        "GrantInstitutionName",
        "GrantConsortiumName",
    ]:
        df[col] = utils.convert_to_stringlist(df[col])

    # Reorder columns to match the table order.
    col_order = [
        "project_id",
        "GrantView_id",
        "GrantName",
        "GrantNumber",
        "GrantAbstract",
        "GrantType",
        "GrantThemeName",
        "GrantInstitutionAlias",
        "GrantInstitutionName",
        "GrantInvestigator",
        "GrantConsortiumName",
        "GrantStartDate",
        "NIHRePORTERLink",
        "DurationofFunding",
        "EmbargoEndDate",
        "GrantSynapseTeam",
        "GrantSynapseProject",
    ]
    return df[col_order]


def main():
    """Main function."""
    syn = utils.syn_login()
    args = utils.get_args("grant")

    if args.dryrun:
        print("\n❗❗❗ WARNING:", "dryrun is enabled (no updates will be done)\n")

    # TODO: update to pd.read_csv once csv manifest is available.
    manifest = (
        syn.tableQuery(f"SELECT * FROM {args.manifest_id}").asDataFrame().fillna("")
    )
    manifest.columns = manifest.columns.str.replace(" ", "")
    if args.verbose:
        print("🔍 Preview of manifest CSV:\n" + "=" * 72)
        print(manifest)
        print()

    print("Processing grant staging database...")
    database = add_missing_info(manifest)
    final_database = clean_table(database)
    if args.verbose:
        print("\n🔍 Grant(s) to be synced:\n" + "=" * 72)
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
