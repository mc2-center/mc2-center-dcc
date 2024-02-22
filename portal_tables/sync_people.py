"""Add People to the Cancer Complexity Knowledge Portal (CCKP).

This script will "sync" the person manifest table to the People
portal table, by first truncating the table, then re-adding the rows.
"""

import pandas as pd
import utils


def add_missing_info(people: pd.DataFrame, grants: pd.DataFrame) -> pd.DataFrame:
    """Add missing information into table before syncing."""

    # Convert profile IDs to int (User column type is stored as a float).
    people["synapseProfileId"] = (
        people["synapseProfileId"].astype(str).replace(r"\.0$", "", regex=True)
    )

    people["grantName"] = ""
    for _, row in people.iterrows():
        # Link markdown to Synapse profile.
        if row["synapseProfileId"] != "":
            people.at[_, "Link"] = (
                "[Synapse Profile](https://www.synapse.org/#!Profile:"
                + str(int(float(row["synapseProfileId"])))
                + ")"
            )
        grant_names = []
        for g in row["personGrantNumber"]:
            if g != "Affiliated/Non-Grant Associated":
                grant_names.append(
                    grants[grants.grantNumber == g]["grantName"].values[0]
                )
        people.at[_, "grantName"] = grant_names
    return people


def clean_table(df: pd.DataFrame) -> pd.DataFrame:
    """Clean up the table one final time."""

    # Convert stringlist to string (temporary workaround).
    df["personPublications"] = df["personPublications"].str.join(", ")
    df["personDatasets"] = df["personDatasets"].str.join(", ")
    df["personTools"] = df["personTools"].str.join(", ")

    # Reorder columns to match the table order.
    col_order = [
        "name",
        "alternativeNames",
        "email",
        "synapseProfileId",
        "Link",
        "url",
        "orcidId",
        "lastKnownInstitution",
        "grantName",
        "personGrantNumber",
        "personConsortiumName",
        "workingGroupParticipation",
        "chairRoles",
        "personPublications",
        "personDatasets",
        "personTools",
        "consentForPortalDisplay",
        "portalDisplay",
    ]
    return df[col_order]


def main():
    """Main function."""
    syn = utils.syn_login()
    args = utils.get_args("people")

    # TODO: update to pd.read_csv once csv manifest is available.
    manifest = (
        syn.tableQuery(f"SELECT * FROM {args.manifest_id}").asDataFrame().fillna("")
    )
    manifest.columns = manifest.columns.str.replace(" ", "")
    if args.verbose:
        print("🔍 Preview of manifest CSV:\n" + "=" * 72)
        print(manifest)
        print()

    print("Processing people staging database...")
    grants = syn.tableQuery(
        "SELECT grantNumber, grantName FROM syn21918972"
    ).asDataFrame()

    database = add_missing_info(manifest, grants)
    final_database = clean_table(database)
    if args.verbose:
        print("\n🔍 People to be synced:\n" + "=" * 72)
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
