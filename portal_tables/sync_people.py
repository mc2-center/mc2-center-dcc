"""Add People to the Cancer Complexity Knowledge Portal (CCKP).

This script will "sync" the person manifest table to the People
portal table, by first truncating the table, then re-adding the rows.
"""

import pandas as pd
from synapseclient.models import Table

import crosswalk
import model_contract
import utils
from sync_projects import NO_GRANT, resolve_consortium_keys, split_cell

COMPONENT = "PersonView"
LIST_TO_STRING_COLS = ["PersonPublications", "PersonDatasets", "PersonTools"]


def prepare_manifest(manifest: pd.DataFrame) -> pd.DataFrame:
    """Rename manifest headers to class-label keys (14.0.0 or legacy input)."""
    manifest = model_contract.rename_legacy_headers(manifest, COMPONENT)
    return model_contract.normalize_headers(
        manifest, to="keys", legacy_component=COMPONENT
    )


def add_missing_info(
    people: pd.DataFrame, grants: pd.DataFrame, consortia: list[list[str]]
) -> pd.DataFrame:
    """Add missing information into table before syncing."""
    people = people.copy()

    # Convert profile IDs to int (User column type is stored as a float).
    people["SynapseProfileId"] = (
        people["SynapseProfileId"].astype(str).replace(r"\.0$", "", regex=True)
    )
    people["link"] = people["SynapseProfileId"].apply(
        lambda x: (
            "".join(
                [
                    "[Synapse Profile](https://www.synapse.org/#!Profile:",
                    str(int(float(x))),
                    ")",
                ]
            )
            if x
            else ""
        )
    )
    people["GrantViewKey"] = [split_cell(v) for v in people["GrantViewKey"]]
    people["consortium"] = pd.Series(consortia, index=people.index, dtype=object)
    people["grantName"] = ""
    for idx, row in people.iterrows():
        grant_names = []
        for g in row["GrantViewKey"]:
            if g != NO_GRANT:
                grant_names.append(
                    grants[grants.grantNumber == g]["grantName"].values[0]
                )
        people.at[idx, "grantName"] = grant_names
    return people


def clean_table(df: pd.DataFrame, live_types: dict[str, str]) -> pd.DataFrame:
    """Order and convert the table to the portal table's shape."""
    df = df.copy()

    # Convert stringlist to string (temporary workaround): the portal columns
    # are STRING, so the crosswalk passes list cells through untouched.
    for col in LIST_TO_STRING_COLS:
        df[col] = df[col].map(lambda v: ", ".join(v) if isinstance(v, list) else v)

    return crosswalk.to_portal(df, COMPONENT, live_types)


def main():
    """Main function."""
    syn = utils.syn_login()
    args = utils.get_args("people")

    manifest = model_contract.read_manifest(syn, args.manifest_id).fillna("")
    manifest = prepare_manifest(manifest)
    if args.verbose:
        print("🔍 Preview of manifest CSV:\n" + "=" * 72)
        print(manifest)
        print()

    print("Processing people staging database...")
    grants = Table(id="syn21918972").query(
        query="SELECT grantNumber, grantName FROM syn21918972",
        synapse_client=syn,
    )

    live_types = crosswalk.check_crosswalk(syn, args.portal_table_id, COMPONENT)
    consortia = resolve_consortium_keys(syn, manifest["ConsortiumKey"])
    database = add_missing_info(manifest, grants, consortia)
    final_database = clean_table(database, live_types)
    if args.verbose:
        print("\n🔍 People to be synced:\n" + "=" * 72)
        print(final_database)
        print()

    if not args.dryrun:
        utils.update_table(
            syn, args.portal_table_id, final_database, component=COMPONENT
        )
        print()

    if not args.noprint:
        print(f"📄 Saving copy of final table to: {args.output_csv}...")
        final_database.to_csv(args.output_csv, index=False)
    print("\n\nDONE ✅")


if __name__ == "__main__":
    main()
