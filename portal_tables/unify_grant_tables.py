"""Combine Synapse projects that are separated 
by grant into the CCKP admin project table.

This script assumes the folders for PublicationView,
DatasetView, and ToolView, respectively, remain 
consistent in the CCKP admin project table.

command line arguments: 1. Portal - Grants Merged Synapse ID 2. CCKP Admin Project Synapse ID
test value for argument 1: syn42801895
test value for argument 2: syn35629947 

author: victor.baham
"""

import argparse
import synapseclient
from synapseclient import Project, File, Folder
from synapseclient import Schema, Column, Table, Row, RowSet, as_table_columns
import itertools
import pandas as pd


def get_args():
    """This function gets the Synapse tables to be
    processed from command line arguments."""
    parser = argparse.ArgumentParser(
        description="Merge publications, datasets, and tools from Grants Merged table into CCKP admin table"
    )
    parser.add_argument(
        "-p1",
        "--project1",
        type=str,
        default="syn42801895",
        help="Portal - Grants Merged Synapse ID.",
    )
    parser.add_argument(
        "-p2",
        "--project2",
        type=str,
        default="syn35629947",
        help=("CCKP - MC2 Admin Synapse ID"),
    )
    parser.add_argument("--dryrun", action="store_true")
    return parser.parse_args()


def get_manifests_from_grant(syn, the_parent):
    """This function examines the GrantView table
    and returns a list of all Synapse table IDs
    separated by manifest type (publications, datasets,
    tools) for each grant."""

    df = syn.tableQuery(f"select {'grantId'} from {the_parent}").asDataFrame()
    g_Ids = df["grantId"].tolist()

    m_Types = []
    m_Ids = []

    list_to_flatten = []

    for g_Id in g_Ids:
        grant_X = syn.get(g_Id)
        id_to_manifest_types = {
            m.get("id"): m.get("name")
            for m in syn.getChildren(grant_X, includeTypes=["table"])
        }
        list_to_flatten.append(id_to_manifest_types)

    id_to_man_final = {k: v for d in list_to_flatten for k, v in d.items()}

    pubs_Id = []
    datasets_Id = []
    tools_Id = []

    for key, val in id_to_man_final.items():
        if "publication" in val.lower():
            pubs_Id.append(key)
        elif "dataset" in val.lower():
            datasets_Id.append(key)
        elif "tool" in val.lower():
            tools_Id.append(key)

    return pubs_Id, datasets_Id, tools_Id


def get_each_manifest_from_CCKP(syn, the_new_parent):
    """This function reads in the Synapse ID of the
    CCKP Admin table and finds the table IDs of the
    PublicationView, DatasetView, and ToolView manifests."""

    p_Man = syn.findEntityId("PublicationView", the_new_parent)
    d_Man = syn.findEntityId("DatasetView", the_new_parent)
    t_Man = syn.findEntityId("ToolView", the_new_parent)

    return p_Man, d_Man, t_Man


def write_manifest_to_CCKP(syn, manifests_by_type, table_id):
    """This function merges all manifests of the same type
    from each grant project into one DataFrame and appends
    the collective rows to the CCKP admin table.
    """
    man_dfs = []
    for man in manifests_by_type:
        man_df = syn.tableQuery(f"select * from {man}").asDataFrame()
        man_dfs.append(man_df)
    final_mans = pd.concat(man_dfs, ignore_index=True)
    syn.store(Table(table_id, final_mans))


def main():
    syn = synapseclient.Synapse()
    syn.login()

    args = get_args()
    origin_table = args.project1
    destination_table = args.project2

    all_Pub, all_Data, all_Tool = get_manifests_from_grant(syn, origin_table)

    syn_Pub, syn_Data, syn_Tool = get_each_manifest_from_CCKP(syn, destination_table)

    write_manifest_to_CCKP(syn, all_Pub, syn_Pub)
    write_manifest_to_CCKP(syn, all_Data, syn_Data)
    write_manifest_to_CCKP(syn, all_Tool, syn_Tool)

    print("The tables have been successfully merged into the CCKP Admin project.")


if __name__ == "__main__":
    main()
