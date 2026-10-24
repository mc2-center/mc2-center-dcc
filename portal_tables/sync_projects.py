"""Add Projects to the Cancer Complexity Knowledge Portal (CCKP).

This script will "sync" the project manifest table to the Grant portal
table, by first truncating the table, then re-adding the rows.
"""

import pandas as pd
from synapseclient.models import Table

import crosswalk
import model_contract
import utils
from utils import resolve_consortium_keys, split_cell

COMPONENT = "ProjectView"
NO_GRANT = "Affiliated/Non-Grant Associated"


def prepare_manifest(manifest: pd.DataFrame) -> pd.DataFrame:
    """Rename manifest headers to class-label keys (14.0.0 or legacy input)."""
    manifest = model_contract.rename_legacy_headers(manifest, COMPONENT)
    return model_contract.normalize_headers(
        manifest, to="keys", legacy_component=COMPONENT
    )


def add_missing_info(
    projects: pd.DataFrame, grants: pd.DataFrame, consortia: list[list[str]]
) -> pd.DataFrame:
    """Add missing information into table before syncing."""
    projects = projects.copy()
    new_cols = ["grantName", "theme", "consortium", "grantType"]
    projects[new_cols] = ""
    projects["consortium"] = pd.Series(consortia, index=projects.index, dtype=object)
    for idx, row in projects.iterrows():
        grant_names = []
        themes = []
        grant_type = ""
        for g in split_cell(row["GrantViewKey"]):
            if g != NO_GRANT:
                match = grants[grants.grantNumber == g]
                grant_names.append(match["grantName"].values[0])
                for t in match["theme"].values[0]:
                    if t not in themes:
                        themes.append(t)
                grant_type = match["grantType"].values[0]
        projects.at[idx, "grantName"] = grant_names
        projects.at[idx, "theme"] = themes
        projects.at[idx, "grantType"] = grant_type
    return projects


def clean_table(df: pd.DataFrame, live_types: dict[str, str]) -> pd.DataFrame:
    """Order and convert the table to the portal table's shape."""
    return crosswalk.to_portal(df, COMPONENT, live_types)


def main():
    """Main function."""
    syn = utils.syn_login()
    args = utils.get_args("project")

    if args.dryrun:
        print("\n❗❗❗ WARNING:", "dryrun is enabled (no updates will be done)\n")

    manifest = model_contract.read_manifest(syn, args.manifest_id).fillna("")
    manifest = prepare_manifest(manifest)
    if args.verbose:
        print("🔍 Preview of manifest CSV:\n" + "=" * 72)
        print(manifest)
        print()

    print("Processing project staging database...")
    grants = Table(id="syn21918972").query(
        query="SELECT grantId, grantNumber, grantName, theme, consortium, grantType FROM syn21918972",
        synapse_client=syn,
    )

    live_types = crosswalk.check_crosswalk(syn, args.portal_table_id, COMPONENT)
    consortia = resolve_consortium_keys(syn, manifest["ConsortiumKey"])
    database = add_missing_info(manifest, grants, consortia)
    final_database = clean_table(database, live_types)
    if args.verbose:
        print("\n🔍 Grant(s) to be synced:\n" + "=" * 72)
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
