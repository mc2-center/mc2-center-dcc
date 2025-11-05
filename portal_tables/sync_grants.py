"""Add Grants to the Cancer Complexity Knowledge Portal (CCKP).

This script will "sync" the grant manifest table to the Grant portal
table, by first truncating the table, then re-adding the rows.
"""

import pandas as pd
import utils
from create_grant_projects import process_new_grants


def clean_table(df: pd.DataFrame) -> pd.DataFrame:
    """Clean up the table one final time."""

    # Convert string columns to string-list.
    for col in [
        "theme",
        "institutionAlias",
        "grantInstitution",
        "consortium",
    ]:
        df[col] = utils.convert_to_stringlist(df[col].apply(lambda x: str(x)))

    # Reorder columns to match the table order.
    col_order = [
        "grantId",
        "grantViewId",
        "grantName",
        "grantNumber",
        "abstract",
        "grantType",
        "theme",
        "institutionAlias",
        "grantInstitution",
        "investigator",
        "consortium",
        "grantStartDate",
        "nihReporterLink",
        "durationOfFunding",
        "embargoEndDate",
        "grantSynapseTeam",
        "grantSynapseProject"
        ]
    return df[col_order]


def main():
    """Main function."""
    syn = utils.syn_login()
    args = utils.get_args("grant")

    if args.dryrun:
        print("\n❗❗❗ WARNING:", "dryrun is enabled (no updates will be done)\n")

    manifest = process_new_grants(args.manifest_id, args.portal_table_id, args.dryrun)
    
    if args.verbose:
        print("🔍 Preview of manifest CSV:\n" + "=" * 72)
        print(manifest)
        print()

    print("Processing grant staging database...")
    final_database = clean_table(manifest)

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
