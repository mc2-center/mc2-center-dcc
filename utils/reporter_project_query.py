import json
import argparse
import requests
import numpy as np
import math
import pandas as pd


def get_args():

    parser = argparse.ArgumentParser(
        description="Query NIH RePORTER for grant information and return as a CSV"
    )
    parser.add_argument(
        "-g",
        nargs="+",
        help="Space separated list of grant numbers to query NIH RePORTER",
    )
    parser.add_argument(
        "-y",
        nargs="+",
        default=[],
        help="The list of years to use when searching for grant information.",
    )
    parser.add_argument("-c", help="Path for CSV where converted JSON will be stored.")

    return parser.parse_args()


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
    print(f"\n\nSUCCESS! Your query was converted to a valid JSON string")

    return json_payload


def get_reporter_info(search_payload, header_content):

    req = requests.post(
        url="https://api.reporter.nih.gov/v2/projects/search",
        headers=header_content,
        data=search_payload,
    )

    return req


def build_report(report_list, out_path):

    df_list = []
    for report in report_list:
        report_df = pd.read_json(report, orient="records")
        df_list.append(report_df)

    full_report = pd.concat(df_list).reset_index(drop=True)

    print(f"\n\nYour report is available at {out_path}.\n\n")

    full_report.to_csv(out_path, index=False)


def main():

    args = get_args()

    in_grants, in_years, csvPath = args.g, args.y, args.c

    grants = []

    years = []

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

        print(f"Building your RePORTER query...")
        query = build_payload(
            grant_numbers=grant_list, years=years, lim=max_request_length
        )

        print(f"\n\nSubmitting your query to RePORTER...")
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
            print(json_report)

    build_report(all_reports, csvPath)

if __name__ == "__main__":
    main()
