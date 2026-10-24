"""NIH RePORTER grant curation

Two-step workflow for collecting grant information from NIH RePORTER:

  query  Query the RePORTER API for grant numbers (and optionally fiscal
         years) and write the raw results to a CSV.
  clean  Read one or more raw CSVs from `query`, keep only the most recent
         fiscal year of each grant, and write the filtered CSV.
  run    `query` then `clean` in one go. The raw CSV is kept next to the
         final one (see --raw).

Examples:
  python reporter_grants.py query -g CA209971 CA214282 -y 2023 2024 -c raw.csv
  python reporter_grants.py clean -r raw.csv -c final.csv
  python reporter_grants.py run -g CA209971 CA214282 -c final.csv
"""

import argparse
import json
import math
import os
import re
from io import StringIO

import numpy as np
import pandas as pd
import requests

REPORTER_URL = "https://api.reporter.nih.gov/v2/projects/search"


def get_args():

    parser = argparse.ArgumentParser(
        description="Query NIH RePORTER for grant information and filter the "
        "results to the most recent fiscal year of each grant"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_query_args(p):
        p.add_argument(
            "-g",
            nargs="+",
            required=True,
            help="Space separated list of grant numbers to query NIH RePORTER",
        )
        p.add_argument(
            "-y",
            nargs="+",
            default=[],
            help="The list of years to use when searching for grant information.",
        )

    query = subparsers.add_parser(
        "query", help="Query NIH RePORTER and write the raw results to a CSV."
    )
    add_query_args(query)
    query.add_argument(
        "-c", required=True, help="Path for CSV where converted JSON will be stored."
    )

    clean = subparsers.add_parser(
        "clean", help="Filter raw RePORTER CSVs to the latest year of each grant."
    )
    clean.add_argument(
        "-r",
        nargs="+",
        required=True,
        help="List of paths to CSVs containing reporter entries from the `query` step",
    )
    clean.add_argument(
        "-c", required=True, help="Path for CSV where parsed report will be stored."
    )

    run = subparsers.add_parser(
        "run", help="Run `query` then `clean`; keeps the raw CSV."
    )
    add_query_args(run)
    run.add_argument(
        "-c", required=True, help="Path for CSV where the filtered report will be stored."
    )
    run.add_argument(
        "--raw",
        help="Path for the raw query CSV. (Default: the -c path with '_raw' "
        "added before the extension.)",
    )

    return parser.parse_args()


### Step 1: query ###


def build_payload(grant_numbers, years, lim):

    criteria_dict = dict(
        project_nums=grant_numbers, sub_project_only=True, fiscal_years=years
    )

    include = [
        "ProjectTitle",
        "ProjectNum",
        "SubprojectId",
        "FiscalYear",
        "ProjectEndDate",
    ]

    payload = dict(
        criteria=criteria_dict,
        include_fields=include,
        offset=0,
        limit=lim,
        sort_field="fiscal_year",
        sort_order="desc",
    )

    json_payload = json.dumps(payload)
    print("\n\nSUCCESS! Your query was converted to a valid JSON string")

    return json_payload


def get_reporter_info(search_payload, header_content):

    req = requests.post(
        url=REPORTER_URL,
        headers=header_content,
        data=search_payload,
    )

    return req


def build_report(report_list, out_path):

    df_list = []
    for report in report_list:
        report_df = pd.read_json(StringIO(report), orient="records")
        df_list.append(report_df)

    full_report = pd.concat(df_list).reset_index(drop=True)

    print(f"\n\nYour report is available at {out_path}.\n\n")

    full_report.to_csv(out_path, index=False)


def query_reporter(in_grants, in_years, csv_path):
    """Query RePORTER for the grants and write the raw results to csv_path."""

    grants = [g + "*" for g in in_grants]

    years = [int(y) for y in in_years]

    headers = {"content-type": "application/json"}

    max_request_length = int(400)
    grant_count = int(len(grants))
    req_per_grant = int(20)

    if grant_count > max_request_length / req_per_grant:
        split_count = math.ceil(grant_count / req_per_grant) + 1
        print(
            f"Due to API restrictions, your query will be split into {split_count} parts"
        )
        grant_chunks = [g.tolist() for g in np.array_split(grants, split_count)]

    else:
        grant_chunks = [grants]

    all_reports = []

    for grant_list in grant_chunks:

        print("Building your RePORTER query...")
        query = build_payload(
            grant_numbers=grant_list, years=years, lim=max_request_length
        )

        print("\n\nSubmitting your query to RePORTER...")
        report = get_reporter_info(query, headers)
        status = report.status_code

        if status == 200:
            print(
                f"\n\nSUCCESS! Reply included a {report.status_code} status code!\n\nBuilding report from results..."
            )
            json_report = report.json()
            json_report = json.dumps(json_report["results"])
            all_reports.append(json_report)

        else:
            print(
                f"Not quite! Reply included a {report.status_code} status code. Please review the response to diagnose the error.\n\n"
            )
            print(report.text)

    build_report(all_reports, csv_path)


### Step 2: clean ###


def extract_for_filtering(report_df):

    column_info = [
        ("grant", r"CA\d{6}", "project_num"),
        ("year", r"(?:-(\d{2}))", "project_num"),
    ]
    for tup in column_info:
        col_name = "_".join([tup[0], "num"])
        pattern = re.compile(tup[1])
        report_df[col_name] = ""
        for idx, row in report_df.iterrows():
            extract = pattern.findall(row[tup[2]])
            report_df.at[idx, col_name] = extract[0]
    return report_df


def filter_report(report):

    groups = report.groupby(["grant_num", "year_num"], as_index=False)
    max_year_table = report.loc[
        report.groupby(["grant_num"])["year_num"].idxmax()
    ].sort_values(by=["grant_num", "year_num"])
    entries_to_keep = []
    for _, row in max_year_table.iterrows():
        g = row["year_num"]
        n = row["grant_num"]
        for name, group in groups:
            if name[0] == n and name[1] == g:
                entries_to_keep.append(group)
                print(f"{name} matches {n} and {g} - added to entry list")
    filtered_report = (
        pd.concat(entries_to_keep).reset_index(drop=True).drop_duplicates()
    )

    return filtered_report


def clean_reports(report_paths, csv_path):
    """Combine raw report CSVs, keep the latest year of each grant, write csv_path."""

    report_list = []

    for report in report_paths:
        report_df = pd.read_csv(report, dtype={"subproject_id": "str"})
        report_list.append(report_df)

    full_report = pd.concat(report_list).reset_index(drop=True)

    report_to_filter = extract_for_filtering(full_report)

    final_report = filter_report(report_to_filter)

    final_report.to_csv(csv_path, index=False)


def default_raw_path(final_path):
    root, ext = os.path.splitext(final_path)
    return f"{root}_raw{ext or '.csv'}"


def main():

    args = get_args()

    if args.command == "query":
        query_reporter(args.g, args.y, args.c)
    elif args.command == "clean":
        clean_reports(args.r, args.c)
    else:
        raw_path = args.raw or default_raw_path(args.c)
        query_reporter(args.g, args.y, raw_path)
        clean_reports([raw_path], args.c)


if __name__ == "__main__":
    main()
