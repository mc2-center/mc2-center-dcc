"""Add Grants to the Cancer Complexity Knowledge Portal (CCKP).

This script will sync over new grants and its annotations to the
Grants portal table.
"""

import argparse

import synapseclient
from synapseclient import Table


# def _join_listlike_col(col, join_by="_", delim=","):
#     """Join list-like column values by specified value.

#     Expects a list, but if string is given, then split (and strip
#     whitespace) by delimiter first.
#     """
#     if isinstance(col, str):
#         col = [el.strip() for el in col.split(delim)]
#     return join_by.join(col).replace("'", "")


def get_args():
    """Set up command-line interface and get arguments."""
    parser = argparse.ArgumentParser(description="Add new grants to the CCKP")
    parser.add_argument(
        "-m",
        "--manifest",
        type=str,
        default="syn53259587",
        help=("Synapse ID to the manifest table/fileview." "(Default: syn35242677)"),
    )
    parser.add_argument(
        "-t",
        "--portal_table",
        type=str,
        default="syn21918972",
        help=("Add grants to this specified table. " "(Default: syn21918972)"),
    )
    parser.add_argument("--dryrun", action="store_true")
    return parser.parse_args()


def sync_table(syn, grants, table):
    """Add grants annotations to the Synapse table.

    Assumptions:
        `grants` matches the same schema as `table`
    """
    schema = syn.get(table)

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
    grants = grants[col_order]

    # Convert columns into STRINGLIST.
    for col in [
        "GrantThemeName",
        "GrantInstitutionAlias",
        "GrantInstitutionName",
        "GrantConsortiumName",
    ]:
        grants.loc[:, col] = grants[col].str.replace(", ", ",").str.split(",")

    new_rows = grants.values.tolist()
    syn.store(Table(schema, new_rows))


def main():
    """Main function."""
    syn = synapseclient.Synapse()
    syn.login(silent=True)
    args = get_args()

    manifest = syn.tableQuery(f"SELECT * FROM {args.manifest}").asDataFrame().fillna("")
    manifest.columns = manifest.columns.str.replace(" ", "")
    curr_grants = (
        syn.tableQuery(f"SELECT grantNumber FROM {args.portal_table}")
        .asDataFrame()
        .grantNumber.to_list()
    )

    # Only add grants not currently in the Grants table.
    new_grants = manifest[~manifest.GrantNumber.isin(curr_grants)]
    if new_grants.empty:
        print("No new grants found!")
    else:
        print(f"{len(new_grants)} new grants found!\n")
        if args.dryrun:
            print("\u26A0", "WARNING: dryrun is enabled (no updates will be done)\n")
            print(new_grants)
        else:
            print("Adding new grants...")
            new_grants.loc[
                :, "project_id"
            ] = new_grants.GrantSynapseProject.str.extract(r":(syn\d*?)/wiki")
            sync_table(syn, new_grants, args.portal_table)
    print("DONE ✓")


if __name__ == "__main__":
    main()
