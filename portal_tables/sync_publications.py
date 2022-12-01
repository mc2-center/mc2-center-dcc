"""Add Publications to the Cancer Complexity Knowledge Portal (CCKP).

This script will sync over new publications and its annotations to the
Publications portal table.
"""

import argparse

from synapseclient import Table
import utils



def get_args():
    """Set up command-line interface and get arguments."""
    parser = argparse.ArgumentParser(description="Add new pubs to the CCKP")
    parser.add_argument("-m", "--manifest",
                        type=str, required=True,
                        help="Synapse ID to the manifest table/fileview.")
    parser.add_argument("-t", "--portal_table",
                        type=str, default="syn21868591",
                        help=("Add publications to this specified "
                              "table. (Default: syn21868591)"))
    parser.add_argument("--dryrun", action="store_true")
    return parser.parse_args()


def add_missing_info(pubs, grants):
    """Add missing information into table before syncing.

    Returns:
        pubs: Data frame
    """
    pubs.loc[:, 'Link'] = [
        "".join(["[PMID:", str(pmid), "](", url, ")"])
        for pmid, url
        in zip(pubs.pubmedId, pubs.pubmedUrl)
    ]
    pubs['grantName'] = ""
    for i, row in pubs.iterrows():
        grant_names = []
        for g in row['publicationGrantNumber']:
            grant_names.append(
                grants[grants.grantNumber == g]['grantName'].values[0])
        pubs.at[i, 'grantName'] = grant_names
    return pubs


def sync_table(syn, pubs, table):
    """Add pubs annotations to the Synapse table."""
    schema = syn.get(table)

    # Reorder columns to match the table order.
    col_order = [
        'publicationDoi', 'publicationJournal', 'pubmedId', 'pubmedUrl',
        'Link', 'publicationTitle', 'publicationYear', 'publicationKeywords',
        'publicationAuthors', 'publicationAbstract', 'publicationAssay', 
        'publicationTumorType', 'publicationTissue', 'publicationThemeName',
        'publicationConsortiumName', 'publicationGrantNumber', 'grantName',
        'publicationDatasetAlias', 'publicationAccessibility'
    ]
    pubs = pubs[col_order]

    # Convert list column into string to match with table schema.
    pubs.loc[:, 'publicationDatasetAlias'] = (
        pubs['publicationDatasetAlias']
        .str.join(", ")
    )

    new_rows = pubs.values.tolist()
    syn.store(Table(schema, new_rows))


def main():
    """Main function."""
    syn = utils.syn_login()
    args = get_args()

    manifest = (
        syn.tableQuery(f"SELECT * FROM {args.manifest}")
        .asDataFrame()
        .fillna("")
    )
    curr_pubs = (
        syn.tableQuery(f"SELECT pubMedId FROM {args.portal_table}")
        .asDataFrame()
        .pubMedId
        .to_list()
    )

    # Only add pubs not currently in the Publications table.
    new_pubs = manifest[~manifest['pubmedId'].isin(curr_pubs)]
    if new_pubs.empty:
        print("No new publications found!")
    else:
        print(f"{len(new_pubs)} new publications found!\n")
        if args.dryrun:
            print(u"\u26A0", "WARNING:",
                  "dryrun is enabled (no updates will be done)\n")
        else:
            print("Adding new publications...")
            grants = (
                syn.tableQuery(
                    "SELECT grantNumber, grantName FROM syn21918972")
                .asDataFrame()
            )
            new_pubs = add_missing_info(new_pubs.copy(), grants)
            sync_table(syn, new_pubs.copy(), args.portal_table)
    print("DONE ✓")


if __name__ == "__main__":
    main()
