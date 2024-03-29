"""Add Educational Resources to the Cancer Complexity Knowledge Portal (CCKP).

This script will "sync" the Educational Resource manifest table to the Educational Resource portal
table, by first truncating the table, then re-adding the rows.
"""

import pandas as pd
import utils


def clean_table(df: pd.DataFrame) -> pd.DataFrame:
    """Clean up the table one final time."""

    # Convert string columns to string-list.
    for col in [
        "ResourceTopic",
        "ResourceActivityType",
        "ResourcePrimaryFormat",
        "ResourceIntendedUse",
        "ResourcePrimaryAudience",
        "ResourceEducationalLevel",
        "ResourceOriginInstitution",
        "ResourceLanguage",
        "ResourceContributors",
        "ResourceGrantNumber",
        "ResourceSecondaryTopic",
        "ResourceMediaAccessibility",
        "ResourceAccessHazard",
    ]:
        df[col] = utils.convert_to_stringlist(df[col])

    # Ensure columns match the table order.
    col_order = [
        "Component",
        "ResourceTitle",
        "ResourceLink",
        "ResourceTopic",
        "ResourceActivityType",
        "ResourcePrimaryFormat",
        "ResourceIntendedUse",
        "ResourcePrimaryAudience",
        "ResourceEducationalLevel",
        "ResourceDescription",
        "ResourceOriginInstitution",
        "ResourceLanguage",
        "ResourceContributors",
        "ResourceGrantNumber",
        "ResourceSecondaryTopic",
        "ResourceLicense",
        "ResourceUseRequirements",
        "ResourceAlias",
        "ResourceInternalIdentifier",
        "ResourceMediaAccessibility",
        "ResourceAccessHazard",
        "ResourceDatasetAlias",
        "ResourceToolLink",
    ]
    return df[col_order]


def main():
    """Main function."""
    syn = utils.syn_login()
    args = utils.get_args("education")

    if args.dryrun:
        print("\n❗❗❗ WARNING:", "dryrun is enabled (no updates will be done)\n")

    manifest = pd.read_csv(syn.get(args.manifest_id).path).fillna("")
    manifest.columns = manifest.columns.str.replace(" ", "")
    if args.verbose:
        print("🔍 Preview of manifest CSV:\n" + "=" * 72)
        print(manifest)
        print()

    print("Processing educational resource staging database...")

    final_database = clean_table(manifest)
    if args.verbose:
        print("\n🔍 Educational resource(s) to be synced:\n" + "=" * 72)
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
