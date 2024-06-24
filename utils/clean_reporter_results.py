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

    column_info = [("grant", "CA\d{6}", "project_num"), ("year", "(?:-(\d{2}))", "project_num")]
    for tup in column_info:
        col_name = "_".join([tup[0], "num"])
        extract_name = "_".join([tup[0], "extracted"])
        pattern = re.compile(tup[1])
        report_df[col_name] = ""
        for _, row in report_df.iterrows():
            extract_name = []
            extract = pattern.findall(row[tup[2]])
            extract_name.append(extract[0])
            report_df.at[_, col_name] = extract_name[0]
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
