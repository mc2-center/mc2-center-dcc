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


def update_nonpreferred_terms(manifest, cv_dict):
    """Update legacy annotations found to current standard terms."""
    for category in cv_dict.index:
        if category in manifest.columns:
            print(f"\tChecking {category}...")
            manifest.loc[:, category] = (
                manifest[category]
                .map(cv_dict[category])
                .fillna(manifest[category])
            )
    return manifest


def update_manifest_tables(syn, scope_ids, cv_dict, dryrun):
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
    cv_dict = map_current_terms_to_legacy(args.cv_list)

    if args.dryrun:
        print("\n❗❗❗ WARNING: dryrun is enabled. Results will be"
              "saved to CSV instead.\n" + "=" * 80 + "\n")
    update_manifest_tables(syn, union_table_scope_ids, cv_dict, args.dryrun)


if __name__ == "__main__":
    main()
