"""
Parse annotations and identify tag terms to associate with portal entries.

Inputs:
- manifest with annotations

Outputs:
- manifest with tags added

author: orion.banks
"""

import synapseclient
import argparse
import pandas as pd
from datetime import datetime

### Login to Synapse ###
def login():

    syn = synapseclient.Synapse()
    syn.login()

    return syn


def get_args():

    parser = argparse.ArgumentParser(
        description="Create lists of tags for icons on the CCKP"
    )
    parser.add_argument(
        "-d", help="Path to manifest with annotations."
    )
    parser.add_argument(
        "-t", help="Path to CSV with annotation-to-tag mappings."
    )
    return parser.parse_args()


def collect_labels(database, terms, attributes):

    database_df = pd.read_csv(database, header=0, na_values=None, keep_default_na=False)
    component = str(database_df.iat[1,1])
    data_type_prefix = component[:-4]
    tag_columns = [" ".join([data_type_prefix, a]) for a in attributes]

    term_df = pd.read_csv(terms, header=0)
    term_tuples = [(x, y) for x, y in zip(term_df["assay"].to_list(), term_df["label"].to_list())]

    database_df["iconTags"] = ""
    
    for column in tag_columns:
        print(f"Assigning labels based on column {column}")
        for _, row in database_df.iterrows():
            annotations = row[column].split(", ")
            labels = set([str(y) for x, y in term_tuples for n in annotations if n == x])
            label_str = ", ".join(labels)
            labels_str_trimmed = ", ".join(labels - {"nan"})
            print(label_str)
            if label_str != "nan":
                database_df.at[_, "iconTags"] = labels_str_trimmed
    print(f"\nTags collected for attribute(s) {tag_columns} and stored in database")
    
    return database_df, component


def main():

    #syn = login()
    args = get_args()
    database = args.d
    terms = args.t

    attribute_list = ["Assay"]
    
    tagged_df, data_type = collect_labels(database, terms, attribute_list)
    tagged_path = "_".join([data_type, "tagged", "-".join(attribute_list), datetime.now().strftime("%Y%m%d")])
    output = tagged_df.to_csv(tagged_path + ".csv", index=False)
    print(f"\nCount information is available at: {tagged_path}.csv")

if __name__ == "__main__":
    main()
