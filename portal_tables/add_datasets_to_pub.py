"""Sync Dataset IDs in Publications Table

This script will ensure the Publications portal table includes
the latest datasets information.
"""

import argparse

import numpy as np
import synapseclient
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

    df = pubs.asDataFrame()
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

    datasets = (
        syn.tableQuery(f"SELECT datasetAlias, pubMedId FROM {args.dataset_table}")
        .asDataFrame()
        .explode("pubMedId")
    )
    pubs = syn.tableQuery(f"SELECT pubMedId, dataset FROM {args.pubs_table}")

    updated = sync_table(datasets, pubs)
    if args.dryrun:
        print(updated)
    else:
        syn.store(synapseclient.Table(args.pubs_table, updated, etag=pubs.etag))
    print("DONE ✓")


if __name__ == "__main__":
    main()
