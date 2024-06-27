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
        "-u",
        "--union_table_id",
        type=str,
        help="Table synID with annotations to update.",
    )
    parser.add_argument(
        "-cv",
        "--cv_list",
        type=str,
        default="https://raw.githubusercontent.com/mc2-center/data-models/main/all_valid_values.csv",
        help="CSV of controlled terms and their non-preferred terms",
    )
    parser.add_argument("--dryrun", action="store_true")
    return parser.parse_args()


def map_current_terms_to_legacy(vocab_csv: str) -> dict:
    """Generate a dictionary of standard terms to their legacy terms.

    Standard terms that do not have legacy terms will NOT be added
    to the dictionary.
    """
    current_cv = pd.read_csv(vocab_csv)

    # Only consider terms with legacy terms, then explode the list.
    filtered_cv = current_cv[current_cv["nonpreferred_values"].notna()]
    filtered_cv.loc[:, "nonpreferred_values"] = (
        filtered_cv["nonpreferred_values"].str.replace(", ", ",").str.split(",")
    )
    filtered_cv = filtered_cv.explode("nonpreferred_values")

    # Create a nested dictionary of
    #   { category -> { non-preferred-term -> standard term } }
    cv_dict = filtered_cv.groupby("category").apply(
        lambda x: dict(zip(x["nonpreferred_values"], x["valid_value"])),
        include_groups=False,
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

#     return annots_df


def main():
    """Main function."""
    syn = login()
    args = get_args()

    annots_df = (
        syn.tableQuery(f"SELECT * FROM {args.annots_table_id}")
        .asDataFrame()
        .fillna("")
    )
    cv_dict = map_current_terms_to_legacy(args.cv_list)

    if not args.dryrun:
        syn.store(synapseclient.Table(args.annots_table_id, edited_annotations))
        print("\n\nAnnotations have been updated!")


if __name__ == "__main__":
    main()
