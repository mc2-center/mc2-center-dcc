"""Sync Dataset IDs in Publications Table

This script will ensure the Publications portal table includes
the latest datasets information.
"""

import argparse

import numpy as np
from synapseclient.models import Table
import utils


def get_args():
    """Set up command-line interface and get arguments."""
    parser = argparse.ArgumentParser(description="Add new datasets to the CCKP")
    parser.add_argument(
        "-d",
        "--dataset_table",
        type=str,
        default=None,
        help=("Synapse ID to the datasets table. (Default: the staging datasets table)"),
    )
    parser.add_argument(
        "-p",
        "--pubs_table",
        type=str,
        default=None,
        help=("Synapse ID to the publications table. (Default: the staging publications table)"),
    )
    parser.add_argument("--dryrun", action="store_true")
    args = parser.parse_args()
    if args.dataset_table is None:
        args.dataset_table = utils.portal_table("dataset")
    if args.pubs_table is None:
        args.pubs_table = utils.portal_table("publication")
    for table_id in (args.dataset_table, args.pubs_table):
        if table_id in utils.PRODUCTION_TABLES:
            parser.error(utils._production_message(table_id))
    return args


def sync_table(datasets, pubs):
    """Add dataset IDs to publications table, then return."""
    curr_dataset_pmids = set(datasets["pubMedId"].to_list()) - {np.nan}

    df = pubs
    for _, row in df.iterrows():
        pmid = str(row.pubMedId)
        if pmid in curr_dataset_pmids:
            pub_datasets = (
                datasets[datasets.pubMedId == pmid]
                .groupby("pubMedId", as_index=False)["datasetAlias"]
                .apply(", ".join)
                .datasetAlias.values[0]
            )
            df.at[_, "dataset"] = pub_datasets
    return df


def main():
    """Main function."""
    syn = utils.syn_login()
    args = get_args()

    datasets = Table(id=args.dataset_table).query(
        query=f"SELECT datasetAlias, pubMedId FROM {args.dataset_table}",
        synapse_client=syn,
    ).explode("pubMedId")

    # NOTE: selecting all columns (not just pubMedId/dataset) is deliberate.
    # Table.store_rows() does a full-row replacement -- any column not
    # present in the DataFrame gets nulled out on the updated rows. The
    # legacy `syn.store(Table(pubs_table, df, etag=...))` this replaced
    # tolerated a partial column set; store_rows() does not.
    pubs = Table(id=args.pubs_table).query(
        query=f"SELECT * FROM {args.pubs_table}", synapse_client=syn
    )

    updated = sync_table(datasets, pubs)
    if args.dryrun:
        print(updated)
    else:
        utils.refuse_production(args.pubs_table)
        Table(id=args.pubs_table).store_rows(values=utils.strip_newlines(updated), synapse_client=syn)
    print("DONE ✓")


if __name__ == "__main__":
    main()
