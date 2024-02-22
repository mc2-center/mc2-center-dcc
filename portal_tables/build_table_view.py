"""
Create/store entityview, based on a list of SynIDs to include in scope.

Inputs:
- space-separated list of project synIDs for scope
- synID for the project in which to store the view table
- entity type to show in view
- name for the view

Outputs:
- an EntityViewSchema table stored in Synapse
- if table exists, scope will be updated and table will be regenerated in-place

author: orion.banks
"""

import argparse
import synapseclient
from synapseclient import EntityViewSchema, EntityViewType
import argparse


def get_args():
    """Set up command-line interface and get arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-s",
        nargs="+",
        help="space-separated list of Synapse IDs corresponding to projects with entities for the view",
    )
    parser.add_argument(
        "-p",
        type=str,
        help="Synapse ID of project where the entity view should be created",
    )
    parser.add_argument(
        "-c",
        choices=["file", "project", "table", "folder", "view", "docker"],
        help="the type of entities to display in the view table",
    )
    parser.add_argument(
        "-a",
        action="store_true",
        help="Boolean; if this flag is provided, default view columns will be included in the table",
    )
    parser.add_argument("-n", type=str, help="name of new view table")
    return parser.parse_args()


def build_schema(view_name, view_parent, view_scope, view_types, view_default):
    view = EntityViewSchema(
        name=view_name,
        parent=view_parent,
        scopes=view_scope,
        includeEntityTypes=[view_types],
        addDefaultViewColumns=view_default,
    )
    return view


def main():

    syn = synapseclient.Synapse()

    syn.login()
    args = get_args()

    if args.c == "table":
        view_type = EntityViewType.TABLE

    elif args.c == "file":
        view_type = EntityViewType.FILE

    new_view = build_schema(args.n, args.p, args.s, view_type, args.a)

    new_view = syn.store(new_view)


if __name__ == "__main__":
    main()
