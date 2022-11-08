"""Check Availability Status of Publications

This script uses the Europe PMC Articles RESTful APIs
(https://europepmc.org/RestfulWebService) to check for status updates of
previously paywalled publications and return the PMIDs of now-available
publications.
"""

import os
import argparse
import requests
from datetime import datetime

import pandas as pd
import synapseclient
from synapseclient import File
from bs4 import BeautifulSoup


def get_args():
    """Set up command-line interface and get arguments."""
    parser = argparse.ArgumentParser(
        description="Perform a status check of publications that were "
                    "previously paywalled, indicated by 'Pending Annotation' "
                    "values in certain columns.")
    parser.add_argument("-p", "--portal_table",
                        type=str, default="syn21868591",
                        help="Synapse ID of the publications table. "
                              "(Default: syn21868591)")
    parser.add_argument("-c", "--colname",
                        type=str, default="pubMedId",
                        help="Column name for publication IDs. "
                              "(Default: `pubMedId`)")
    parser.add_argument("-a", "--annotation_cols",
                        type=str, nargs="+",
                        default=["assay", "tissue", "tumorType"],
                        help="Column(s) containing 'Pending Annotation' "
                             "values (must be STRINGLIST type). (Default: "
                             "`assay`, `tissue`, `tumorType`)")
    parser.add_argument("-f", "--folder_id",
                        type=str, default="syn44266568",
                        help="Syanpse ID of folder where results are uploaded "
                        "(Default: syn44266568)")
    parser.add_argument("--send_email", type=str, nargs="+",
                        help="Send email report to listed persons.")
    parser.add_argument("--dryrun", action="store_true",
                        help="Prints query to be used in Synapse table; "
                        "status check will not be performed.")
    return parser.parse_args()


def where_clause(cols):
    """Return WHERE clause to be used in Synapse query."""
    clause = f"{cols[0]} HAS ('Pending Annotation')"
    for col in cols[1:]:
        clause += f" OR {col} HAS ('Pending Annotation')"
    return clause


def status_check(syn, query, colname):
    """
    Check availability of publications and return df of open/accessible
    publications, their PMIDs, and current annotations on Synapse.
    """
    print("Checking for status updates...")
    session = requests.Session()

    ready_for_review = []
    url = "https://www.ebi.ac.uk/europepmc/webservices/rest/searchPOST"
    res = syn.tableQuery(query).asDataFrame()
    pending_pmids = res[colname].tolist()
    for pmid in pending_pmids:
        data = {'query': pmid, 'resultType': "core"}
        publication = BeautifulSoup(
            session.post(url=url, data=data).content,
            features="xml")
        latest_availability = publication.find_all('availabilityCode')[-1].text
        if latest_availability in ['F', 'OA']:
            row = res[res[colname] == pmid]
            ready_for_review.append(row)

    return pd.concat(ready_for_review)


def upload_results(syn, results, parent):
    """Upload results to Synapse as CSV file."""
    print("Uploading results to Synapse...")
    output_file = f"status_check_{datetime.today().strftime('%Y-%m-%d')}.csv"
    results.to_csv(output_file, index=False)
    results_file = File(output_file, parent=parent)
    results_file = syn.store(results_file)
    os.remove(output_file)  # Clean up file.
    return results_file.id


def main():
    """Main function."""
    syn = synapseclient.login(silent=True)
    args = get_args()

    query = (
        f"SELECT {args.colname}, {', '.join(args.annotation_cols)} "
        f"FROM {args.portal_table} "
        f"WHERE {where_clause(args.annotation_cols)}"
    )
    if args.dryrun:
        print(u"\u26A0", "WARNING:",
              "dryrun is enabled (no status check will be done)\n")
        print(f"Query to be used:\n  {query}")
    else:
        ready_for_review = status_check(syn, query, args.colname)
        file_id = upload_results(syn, ready_for_review, args.folder_id)
        print(f"Results ID: {file_id}")

        if args.send_email:
            message = (
                "Hey team,",
                f"{len(ready_for_review)} publications are now marked as "
                "Free and/or Open Access. Find the results here: "
                f"https://www.synapse.org/#!Synapse:{file_id}",
                "Have fun! :)"
            )
            syn.sendMessage(
                userIds=args.send_email,
                messageSubject="Publications Status Check Results",
                messageBody="\n\n".join(message)
            )
        print("DONE ✓")


if __name__ == "__main__":
    main()
