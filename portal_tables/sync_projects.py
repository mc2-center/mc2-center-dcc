"""Add Projects to the Cancer Complexity Knowledge Portal (CCKP).

This script will "sync" the project manifest table to the Grant portal
table, by first truncating the table, then re-adding the rows.
"""

import pandas as pd
from synapseclient.models import Table

import crosswalk
import model_contract
import utils

COMPONENT = "ProjectView"
NO_GRANT = "Affiliated/Non-Grant Associated"


def split_cell(value) -> list[str]:
    """A manifest cell (comma-separated string or list) as a list of strings."""
    if isinstance(value, (list, tuple)):
        items = [str(v) for v in value]
    elif value is None or (not isinstance(value, str) and pd.isna(value)):
        items = []
    else:
        items = str(value).split(",")
    return [i.strip() for i in items if i and i.strip()]


def known_consortium_names(syn) -> set[str]:
    """Display names in the Consortium table (read-only)."""
    table_id = utils.CONFIG["consortium"]["table"]
    rows = Table(id=table_id).query(
        query=f"SELECT consortiumDisplayName FROM {table_id}",
        include_row_id_and_row_version=False,
        synapse_client=syn,
    )
    return set(rows["consortiumDisplayName"])


def resolve_consortium_keys(syn, cells, known_names=None) -> list[list[str]]:
    """Resolve ConsortiumKey cells to lists of consortium display names.

    Until infrastructure step I5 regenerates the manifests, cells hold display
    names (``CSBC``); afterwards they hold ``program.*`` IDs. A value that is
    a display name the Consortium table knows passes through; a ``program.*``
    ID is resolved with ``utils.consortium_display_names``. Any other value is
    fatal: every unknown value is collected and raised in one ``ValueError``.
    """
    if known_names is None:
        known_names = known_consortium_names(syn)
    resolved: dict[str, str] = {}
    unknown = []
    for cell in cells:
        for token in split_cell(cell):
            if token in resolved or token in unknown:
                continue
            if token in known_names:
                resolved[token] = token
            elif token.startswith("program."):
                try:
                    resolved[token] = utils.consortium_display_names(syn, [token])[0]
                except ValueError:
                    unknown.append(token)
            else:
                unknown.append(token)
    if unknown:
        raise ValueError(
            f"Unknown ConsortiumKey value(s) {sorted(unknown)}: neither a display "
            f"name nor a program.* ID in the Consortium table "
            f"({sorted(known_names)})"
        )
    out = []
    for cell in cells:
        names = []
        for token in split_cell(cell):
            if resolved[token] not in names:
                names.append(resolved[token])
        out.append(names)
    return out


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

    manifest = Table(id=args.manifest_id).query(
        query=f"SELECT * FROM {args.manifest_id}",
        include_row_id_and_row_version=False,
        synapse_client=syn,
    ).fillna("")
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
