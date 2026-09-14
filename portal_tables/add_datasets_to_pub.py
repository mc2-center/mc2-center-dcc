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
        default="syn21897968",
        help=("Synapse ID to the datasets table. " "(Default: syn21897968"),
    )
    parser.add_argument(
        "-p",
        "--pubs_table",
        type=str,
        default="syn21868591",
        help=("Synapse ID to the publications table. " "(Default: syn21868591)"),
    )
    parser.add_argument("--dryrun", action="store_true")
    return parser.parse_args()


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
        Table(id=args.pubs_table).store_rows(values=updated, synapse_client=syn)
    print("DONE ✓")


if __name__ == "__main__":
    main()
