"""Check Availability Status of Publications

This script uses the unpaywall APIs (https://unpaywall.org/products/api)
to check for Open-Access statuses of previously paywalled publications
and returns information of previously inaccessible publications as CSV.
"""

import os
import sys
import argparse
import json
from datetime import datetime
import requests

import pandas as pd
from synapseclient.models import File, Table

# removed when the mc2dcc package (CLI consolidation plan) lands
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "portal_tables"))
import header_maps

import importlib.util
# removed when the mc2dcc package (CLI consolidation plan) lands
# portal_tables/utils.py, loaded by path: "utils" would collide with this package
_spec = importlib.util.spec_from_file_location(
    "portal_tables_utils",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "portal_tables", "utils.py"),
)
_portal_utils = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_portal_utils)
syn_login = _portal_utils.syn_login


def get_args():
    """Set up command-line interface and get arguments."""
    parser = argparse.ArgumentParser(
        description="Perform a status check of publications that were "
        "previously paywalled, indicated by 'Restricted Access' "
        "in the `accessibility` column."
    )
    parser.add_argument(
        "-p",
        "--portal_table",
        type=str,
        default="syn21868591",
        help="Synapse ID of the publications table. " "(Default: syn21868591)",
    )
    parser.add_argument(
        "-c",
        "--colname",
        type=str,
        default="doi",
        help="Column name for DOIs. (Default: `doi`)",
    )
    parser.add_argument(
        "-f",
        "--folder_id",
        type=str,
        default="syn44266568",
        help="Syanpse ID of folder where results are uploaded "
        "(Default: syn44266568)",
    )
    parser.add_argument(
        "--send_email", type=str, nargs="+", help="Send email report to listed persons."
    )
    parser.add_argument(
        "--dryrun",
        action="store_true",
        help="Run the check and write the results CSV locally, but skip the "
        "Synapse File upload and the email (sendMessage).",
    )
    parser.add_argument(
        "--outdir",
        default=".",
        help="Directory for the local CSV written by --dryrun. (Default: .)",
    )
    return parser.parse_args()


def status_check(syn, table_id, query, colname, email):
    """
    Check availability of publications and return df of open/accessible
    publications and their current annotations on the portal.
    """
    df = Table(id=table_id).query(
        query=query, include_row_id_and_row_version=False, synapse_client=syn
    )
    doi_list = df[~df[colname].isnull()]["doi"]
    ready_for_review = []
    with requests.Session() as session:
        for doi in doi_list:
            unpaywall_url = f"https://api.unpaywall.org/v2/{doi}?email={email}"
            try:
                check_oa_status = session.get(unpaywall_url)
                check_oa_status.raise_for_status()
                is_open = json.loads(check_oa_status.content).get("is_oa")
                if is_open:
                    row = df[df[colname] == doi]
                    row.loc[row.index, "accessibility"] = "Open Access"
                    ready_for_review.append(row)
            except (requests.exceptions.HTTPError, json.JSONDecodeError):
                # Assumption: DOI does not exist yet; skip.
                pass
    if ready_for_review:
        ready_for_review = pd.concat(ready_for_review)
    else:
        ready_for_review = pd.DataFrame(columns=df.columns)

    return to_manifest(ready_for_review)


def to_manifest(portal_df: pd.DataFrame) -> pd.DataFrame:
    """Convert PublicationView portal-table rows to manifest (display-name) form.

    Portal columns map to display names through the crosswalk; the columns the
    sync script computes (``pubMedLink``, ``grantName``, ``theme``,
    ``consortium``, ``dataType``) are dropped.
    """
    component = "PublicationView"
    df = portal_df.drop(columns=header_maps.derived_portal_columns(component), errors="ignore")
    df = df.rename(columns=header_maps.portal_to_display(component))
    df["Component"] = component
    # Convert grant number from list type to string
    df["GrantView Key"] = [
        grant if isinstance(grant, str) else ",".join(map(str, grant))
        for grant in df["GrantView Key"]
    ]
    return df


def upload_results(syn, results, parent):
    """Upload results to Synapse as CSV file."""
    output_file = f"status_check_{datetime.today().strftime('%Y-%m-%d')}.csv"
    results.to_csv(output_file, index=False)
    results_file = File(path=output_file, parent_id=parent).store(synapse_client=syn)
    os.remove(output_file)  # Clean up file.
    return results_file.id


def main():
    """Main function."""
    syn = syn_login()
    args = get_args()

    query = (
        f"SELECT * FROM {args.portal_table} "
        f"WHERE accessibility = 'Restricted Access'"
    )
    email = "mc2center@sagebase.org"
    ready_for_review = status_check(
        syn, args.portal_table, query, args.colname, email
    )

    if args.dryrun:
        os.makedirs(args.outdir, exist_ok=True)
        output_file = os.path.join(
            args.outdir, f"status_check_{datetime.today().strftime('%Y-%m-%d')}.csv"
        )
        ready_for_review.to_csv(output_file, index=False)
        print(
            f"[dryrun] {len(ready_for_review)} publications now Open Access; "
            f"results written to {output_file}"
        )
        print(
            f"[dryrun] Skipped: File upload to {args.folder_id}"
            + (f", sendMessage to {args.send_email}" if args.send_email else "")
        )
        print("-- DONE --")
        return

    file_id = upload_results(syn, ready_for_review, args.folder_id)
    print(f"Results ID: {file_id}")

    if args.send_email:
        message = (
            "Hey MC2 Center curators,",
            f"According to Unpaywall, {len(ready_for_review)} publications "
            "are now marked as Free and/or Open Access.\n\n"
            f"Find the results here: https://www.synapse.org/#!Synapse:{file_id}",
        )
        syn.sendMessage(
            userIds=args.send_email,
            messageSubject="Publications Status Check Results",
            messageBody="\n\n".join(message),
        )
    print("-- DONE --")


if __name__ == "__main__":
    main()
