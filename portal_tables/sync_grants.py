"""Add Grants to the Cancer Complexity Knowledge Portal (CCKP).

This script will sync over new grants and its annotations to the
Grants portal table. A Synapse Project with pre-filled Wikis and
Folders will also be created for each new grant found, as well as
a Synapse team.
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
    parser.add_argument("-m", "--manifest",
                        type=str, default="syn35242677",
                        help=("Synapse ID to the manifest table/fileview."
                              "(Default: syn35242677)"))
    parser.add_argument("-t", "--portal_table",
                        type=str, default="syn21918972",
                        help=("Add grants to this specified table. "
                              "(Default: syn21918972)"))
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
    grants.loc[:, "grantThemeName"] = grants.grantThemeName.str.split(", ")
    grants.loc[:, "grantInstitutionName"] = (
        grants["grantInstitutionName"]
        .str
        .split(", "))
    grants.loc[:, "grantInstitutionAlias"] = (
        grants["grantInstitutionAlias"]
        .str
        .split(", "))

    new_rows = grants.values.tolist()
    syn.store(Table(schema, new_rows))


def main():
    """Main function."""
    syn = synapseclient.Synapse()
    syn.login(silent=True)
    args = get_args()

    manifest = syn.tableQuery(f"SELECT * FROM {args.manifest}").asDataFrame()
    curr_grants = (
        syn.tableQuery(f"SELECT grantNumber FROM {args.portal_table}")
        .asDataFrame()
        .grantNumber
        .to_list()
    )

    # Only add grants not currently in the Grants table.
    new_grants = manifest[~manifest.grantNumber.isin(curr_grants)]
    if new_grants.empty:
        print("No new grants found!")
    else:
        print(f"{len(new_grants)} new grants found!\n")
        if args.dryrun:
            print("\u26A0", "WARNING:",
                  "dryrun is enabled (no updates will be done)\n")
            print(new_grants)
        else:
            print("Adding new grants...")
            added_grants = create_grant_projects(syn, new_grants)
            sync_table(syn, added_grants, args.portal_table)
    print("DONE ✓")


if __name__ == "__main__":
    main()
