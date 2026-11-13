"""
Parse annotations and identify tag terms to associate with portal entries.

Inputs:
- manifest with annotations

Outputs:
- manifest with tags added

author: orion.banks
"""

import argparse
import pandas as pd
from datetime import datetime

def get_args():

    parser = argparse.ArgumentParser(
        description="Create lists of tags for icons on the CCKP"
    )
    parser.add_argument(
        "-d", help="Path to manifest with annotations.", required=True
    )
    parser.add_argument(
        "-t", help="Path to CSV with annotation-to-tag mappings.", required=True
    )
    return parser.parse_args()


def icon_tags(database_df, term_df, attributes):
    """The iconTags cell for each row: labels of the terms in the attribute columns, ", "-joined.

    An attribute column is found under its 14.x name (``Assay``) or its legacy
    prefixed name (``Publication Assay``).
    """
    component = str(database_df["Component"].iat[0])
    tag_columns = [a if a in database_df.columns else " ".join([component[:-4], a]) for a in attributes]
    label_of = {}
    for term, label in zip(term_df["term"], term_df["label"]):
        if pd.notna(label) and term not in label_of:
            label_of[term] = str(label)

    tags = []
    for _, row in database_df.iterrows():
        annotations = [a for column in tag_columns for a in str(row[column]).split(", ")]
        labels = dict.fromkeys(label_of[a] for a in annotations if a in label_of)
        tags.append(", ".join(labels))
    return pd.Series(tags, index=database_df.index)


def collect_labels(database, terms, attributes):

    database_df = pd.read_csv(database, header=0, na_values=None, keep_default_na=False)
    component = str(database_df["Component"].iat[0])
    term_df = pd.read_csv(terms, header=0)
    database_df["iconTags"] = icon_tags(database_df, term_df, attributes)
    print(f"\nTags collected for attribute(s) {attributes} and stored in database")

    return database_df, component


def main():

    args = get_args()
    database = args.d
    terms = args.t

    attribute_list = ["Assay"]
    
    tagged_df, data_type = collect_labels(database, terms, attribute_list)
    tagged_path = "_".join([data_type, "tagged", "-".join(attribute_list), datetime.now().strftime("%Y%m%d")])
    output = tagged_df.to_csv(tagged_path + ".csv", index=False)
    print(f"\nTagged resources are available at: {tagged_path}.csv")

if __name__ == "__main__":
    main()
