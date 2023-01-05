"""Check Availability Status of Publications

This script uses the unpaywall APIs (https://unpaywall.org/products/api)
to check for Open-Access statuses of previously paywalled publications
and returns information of previously inaccessible publications as CSV.
"""

import os
import argparse
import json
from datetime import datetime
import requests

import pandas as pd
import synapseclient
from synapseclient import File
import sys


def get_args():
    """Set up command-line interface and get arguments."""
    parser = argparse.ArgumentParser(
        description="Perform a status check of publications that were "
        "previously paywalled, indicated by 'Restricted Access' "
        "in the `accessibility` column.")
    parser.add_argument("-p",
                        "--portal_table",
                        type=str,
                        default="syn21868591",
                        help="Synapse ID of the publications table. "
                        "(Default: syn21868591)")
    parser.add_argument("-c",
                        "--colname",
                        type=str,
                        default="doi",
                        help="Column name for DOIs. (Default: `doi`)")
    parser.add_argument("-f",
                        "--folder_id",
                        type=str,
                        default="syn44266568",
                        help="Syanpse ID of folder where results are uploaded "
                        "(Default: syn44266568)")
    parser.add_argument("--send_email",
                        type=str,
                        nargs="+",
                        help="Send email report to listed persons.")
    return parser.parse_args()


def status_check(syn, query, colname, email, publication_dict):
    """
    Check availability of publications and return df of open/accessible
    publications and their current annotations on the portal.
    """
    df = syn.tableQuery(query).asDataFrame()
    doi_list = df[~df[colname].isnull()]['doi']

    ready_for_review = []
    with requests.Session() as session:
        for doi in doi_list:
            url = f"https://api.unpaywall.org/v2/{doi}?email={email}"
            response = json.loads(session.get(url).content)
            if response.get('is_oa'):
                row = df[df[colname] == doi]
                row.loc[row.index, 'accessibility'] = "Open Access"
                ready_for_review.append(row)

    ready_for_review = pd.concat(ready_for_review)
    # Switch column name dictionary key/value pairs
    column_names = {value: key for key, value in publication_dict.items()}
    # Edit data frame to match data model
    ready_for_review = ready_for_review.rename(columns=column_names).drop(
        ['pubMedLink', 'grantName', 'theme', 'consortium'], axis=1)
    ready_for_review['Component'] = 'PublicationView'
    # Convert grant number from list type to string
    ready_for_review['Publication Grant Number'] = [
        ','.join(map(str, l))
        for l in ready_for_review['Publication Grant Number']
    ]

    return ready_for_review


def upload_results(syn, results, parent):
    """Upload results to Synapse as CSV file."""
    output_file = f"status_check_{datetime.today().strftime('%Y-%m-%d')}.csv"
    results.to_csv(output_file, index=False)
    results_file = File(output_file, parent=parent)
    results_file = syn.store(results_file)
    os.remove(output_file)  # Clean up file.
    return results_file.id


def main():
    """Main function."""
    sys.path.insert(0, './../annotations')
    from attribute_dictionary import PUBLICATION_DICT
    syn = synapseclient.login(silent=True)
    args = get_args()

    query = (f"SELECT * FROM {args.portal_table} "
             f"WHERE accessibility = 'Restricted Access'")
    email = "smc2center@sagebase.org"
    ready_for_review = status_check(syn, query, args.colname, email,
                                    PUBLICATION_DICT)

    file_id = upload_results(syn, ready_for_review, args.folder_id)
    print(f"Results ID: {file_id}")

    if args.send_email:
        message = ("Hey data curators,",
                   f"{len(ready_for_review)} publications are now marked as "
                   "Free and/or Open Access. Find the results here: "
                   f"https://www.synapse.org/#!Synapse:{file_id}",
                   "Have fun! :)")
        syn.sendMessage(userIds=args.send_email,
                        messageSubject="Publications Status Check Results",
                        messageBody="\n\n".join(message))
    print("-- DONE --")


if __name__ == "__main__":
    main()
