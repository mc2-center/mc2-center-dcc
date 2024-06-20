import argparse
from getpass import getpass

import pandas as pd
import synapseclient
from attribute_dictionary import ATTRIBUTE_DICT


def login() -> synapseclient.Synapse:
    """Log into Synapse. If env variables not found, prompt user."""
    try:
        syn = synapseclient.login(silent=True)
    except synapseclient.core.exceptions.SynapseNoCredentialsError:
        print(
            ".synapseConfig not found; please manually provide your",
            "Synapse Personal Access Token (PAT). You can generate"
            "one at https://www.synapse.org/#!PersonalAccessTokens:0",
        )
        pat = getpass("Your Synapse PAT: ")
        syn = synapseclient.login(authToken=pat, silent=True)
    return syn


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Update legacy annotations to latest standard terms."
    )
    parser.add_argument(
        "-a",
        "--annots_table_id",
        type=str,
        help="Table synID with annotations to update.",
    )
    parser.add_argument(
        "-cv",
        "--cv_list",
        type=str,
        default="https://raw.githubusercontent.com/mc2-center/data-models/minor-update/all_valid_values.csv",
        help="CSV of controlled terms and their non-preferred terms",
    )
    parser.add_argument(
        "-it",
        "--intermediate_table_id",
        type=str,
        help="Table synID of intermediary/qc table (if there is one)",
    )
    return parser.parse_args()


def annotation_df(annots_table, syn):

    annots_query = f"SELECT * FROM {annots_table}"
    annots_df = syn.tableQuery(annots_query).asDataFrame().fillna("")

    return annots_df


def cv_df(cv_table, syn):

    cv_query = f"SELECT attribute, preferredTerm, nonpreferredTerms FROM {cv_table}"
    cv_df = syn.tableQuery(cv_query).asDataFrame().fillna("")

    return cv_df


def cv_dictionary(cv_df):

    # Create dictionary from controlled vocabulary table
    cv_dict = cv_df.groupby("attribute").apply(
        lambda x: dict(zip(x["preferredTerm"], x["nonpreferredTerms"]))
    )

    return cv_dict


def edit_annotations(ATTRIBUTE_DICT, annots_df, cv_dict):

    # Iterate through attribute diciontary and match column names
    for k, v in ATTRIBUTE_DICT.items():
        if v in annots_df.columns:
            # Get nested dictionary for corresponding column/attribute from cv_dict
            column_dict = cv_dict.get(k)
            # Iterate through annotation rows for corresponding column/attribute
            for i, r in annots_df.iterrows():
                column_value = r[v]
                # If column value is a list, iterate through list of terms
                if type(column_value) == list:
                    for term in column_value:
                        # Iterate through corresponding column/attribute dictionary values to match terms.
                        for key, value in column_dict.items():
                            for item in value:
                                # If the terms match and the nonpreferred term is not the same as the preferred term (need to fix this in the CV)
                                if term == item and term != key:
                                    # Replace nonpreferred term with preferred term in annotation (if a list)
                                    updated_column_value = list(
                                        map(
                                            lambda x: x.replace(term, key), column_value
                                        )
                                    )
                                    # Update term in annotations data frame
                                    annots_df.at[i, v] = updated_column_value
                                    print(
                                        f'\n\nNonpreferred term caught: "{term}" and updated to preferred term: "{key}"\nAttribute: {k}\nColumn name: {v}\nOriginal full annotation: {column_value}\nUpdated full annotation: {updated_column_value}'
                                    )

                # If column type is not a list, replace annotation term with preferred term
                else:
                    for key, value in column_dict.items():
                        for item in value:
                            if column_value == item and item != key:
                                annots_df.at[i, v] = key
                                print(
                                    f'\n\nNonpreferred term caught: "{item}" and updated to preferred term: "{key}"\nAttribute: {k}\nColumn name: {v}\nOriginal full annotation: "{column_value}"\nUpdated full annotation: "{key}"'
                                )
    return annots_df


def store_edited_annotations(syn, table_id, annots_df):

    syn.store(Table(table_id, annots_df))

    print("\n\nAnnotations have been updated!")


def main():

    syn = login()
    args = get_args()
    annots_df = annotation_df(args.annots_table_id, syn)
    vocab_df = cv_df(args.cv_table_id, syn)
    cv_dict = cv_dictionary(vocab_df)
    edited_annotations = edit_annotations(ATTRIBUTE_DICT, annots_df, cv_dict)

    # Store updated annotations, uncomment when ready.
    # store_edited_annotations(syn, args.annots_table_id, edited_annotations)


if __name__ == "__main__":
    main()
