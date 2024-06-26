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


def get_standard_terms(dictionary: dict, terms: list) -> set:
    """Map list of terms to their standard term, removing all duplicates."""
    return {dictionary.get(term, term) for term in terms}


def map_legacy_terms_to_standard(vocab_csv: str) -> dict:
    """Generate a dictionary of legacy terms to their standard term.

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


def update_nonpreferred_terms(
        table: pd.DataFrame,
        cv_dict: dict
) -> pd.DataFrame:
    """Update legacy annotations found to current standard terms."""

    # Manifest may use colnames that are variations of what is listed
    # in the dictionary, e.g. "Publication Assay" instead of "assay".
    # Re-map colnames to match with dictionary, keeping record of the
    # original manifest colnames.
    og_colnames = table.columns
    table = table.rename(columns=ATTRIBUTE_DICT)
    for category in cv_dict.index:
        if category in table.columns:
            print(f"\tChecking {category}...")
            table.loc[:, category] = (
                table[category]
                .str.replace(", ", ",")
                .str.split(",")
                .apply(lambda annots: ", ".join(
                    get_standard_terms(cv_dict[category], annots)
                ))
            )

    # Rename colnames to the original names.
    table.columns = og_colnames
    return table


def update_manifest_tables(syn, scope_ids: list, cv_dict: dict, dryrun: bool):
    """Update each parent table found in union table."""
    for table_id in scope_ids:
        print(f"Updating annotations found in table ID: {table_id}")
        table = syn.tableQuery(f"SELECT * FROM {table_id}")
        updated_table = update_nonpreferred_terms(
            table.asDataFrame().fillna(""),
            cv_dict
        )
        if dryrun:
            updated_table.to_csv(table_id + "-updated.csv", index=False)
        else:
            syn.store(synapseclient.Table(
                table_id,
                updated_table,
                etag=table.etag
            ))


def main():
    """Main function."""
    syn = login()
    args = get_args()

    union_table_scope_ids = (
        syn.tableQuery(f"SELECT entityId FROM {args.union_table_id}")
        .asDataFrame()["entityId"]
        .unique()
    )
    cv_dict = map_legacy_terms_to_standard(args.cv_list)

    if args.dryrun:
        print("\n❗❗❗ WARNING: dryrun is enabled. Results will be "
              "saved to CSV instead.\n" + "=" * 80 + "\n")
    update_manifest_tables(syn, union_table_scope_ids, cv_dict, args.dryrun)


if __name__ == "__main__":
    main()
