import argparse
import pandas as pd
import re


def get_args():

    parser = argparse.ArgumentParser(
        description="Access and validate tables from Synapse"
    )
    parser.add_argument(
        "-r",
        nargs="+",
        help="List of paths to CSVs containing reporter entries from import_json.py",
    )
    parser.add_argument("-c", help="Path for CSV where parsed report will be stored.")

    return parser.parse_args()


def extract_for_filtering(report_df):

    grant_pattern = re.compile("CA\d{6}")
    year_pattern = re.compile("(?:-(\d{2}))")
    report_df["main_grant"] = ""
    report_df["grant_year"] = ""
    for _, row in report_df.iterrows():
        extracted_grants = []
        extracted_year = []
        base_grant = grant_pattern.findall(row["project_num"])
        grant_year = year_pattern.findall(row["project_num"])
        extracted_grants.append(base_grant[0])
        extracted_year.append(grant_year[0])
        report_df.at[_, "main_grant"] = extracted_grants[0]
        report_df.at[_, "grant_year"] = extracted_year[0]
    return report_df


def filter_report(report):

    groups = report.groupby(["main_grant", "grant_year"], as_index=False)
    max_year_table = report.loc[
        report.groupby(["main_grant"])["grant_year"].idxmax()
    ].sort_values(by=["main_grant", "grant_year"])
    entries_to_keep = []
    for _, row in max_year_table.iterrows():
        g = row["grant_year"]
        n = row["main_grant"]
        for name, group in groups:
            if name[0] == n and name[1] == g:
                entries_to_keep.append(group)
                print(f"{name} matches {n} and {g} - added to entry list")
    filtered_report = pd.concat(entries_to_keep).reset_index(drop=True).drop_duplicates()

    return filtered_report


def main():
    args = get_args()

    reports, csvPath = args.r, args.c

    report_list = []

    for report in reports:
        report_df = pd.read_csv(report, dtype={"subproject_id": "str"})
        report_list.append(report_df)

    full_report = pd.concat(report_list).reset_index(drop=True)

    report_to_filter = extract_for_filtering(full_report)

    final_report = filter_report(report_to_filter)

    final_report.to_csv(csvPath, index=False)


if __name__ == "__main__":
    main()
