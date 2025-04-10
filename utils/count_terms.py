"""
Check number of rows in a list of tables, to verify metadata uploads.

Inputs:
- list of table synIds to query

Outputs:
- CSV of table Ids and row counts

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
        description="Count the number of times controlled vocab terms have been used"
    )
    parser.add_argument(
        "-d", help="Path to manifest with terms to count"
    )
    parser.add_argument(
        "-t", help="Path to CSV with list of terms to count"
    )
    parser.add_argument(
        "-c", help="Name of value type to count, as named in 'all_valid_values.csv'"
    )
    return parser.parse_args()


def count_terms(database, terms, count:str, formatted_column, use_column):

    database_df = pd.read_csv(database, header=0, na_values=None, keep_default_na=False)
    
    component = str(database_df.iat[1,1])
    data_type_prefix = component[:-4]
    
    if formatted_column is not None and use_column is True:
        count_type = formatted_column
    else:
        count_type = " ".join([data_type_prefix, count.capitalize()])
    
    term_df = pd.read_csv(terms, header=0)
    term_tuples = [(x, y) for x, y in zip(term_df["category"].to_list(), term_df["valid_value"].to_list())]
    term_list = [y for x, y in term_tuples if x == count.lower()]

    value_list_nested = [x.split(sep=", ") for x in database_df[f"{count_type}"].to_list()]
    value_list = [x for xs in value_list_nested for x in xs]
    
    out_tup_list = []
    print(f"Counting {count} terms from column {count_type}")
    for term in term_list:
        value_count = 0
        
        for value in value_list:
            if value == term:
                value_count += 1
        
        out_tup = (str(term), str(count_type), str(value_count))
        out_tup_list.append(out_tup)
        print(f"\nFound {value_count} entries for term {term}")

    count_df = pd.DataFrame.from_records(out_tup_list, columns=[f"{count}", "Column Name", "Value Count"])
    
    return count_df, data_type_prefix


def main():

    #syn = login()
    args = get_args()
    database = args.d
    terms = args.t
    count = args.c

    op_list = ["input", "output"]

    if count in ["tool_language", "tool_operation", "tool_topic", "tool_type", "tool_data", "tool_format"]:
        count_elements = count.split("_")
        count_elements = [x.capitalize() for x in count_elements]
        
        if count in ["tool_data", "tool_format"]:
            count_list = []
            for e in op_list:
                count_entry = " ".join([count_elements[0], e.capitalize(), count_elements[1]])
                count_list.append(count_entry)
            processed_column = count_list
            
        else: 
            processed_column = [" ".join([c for c in count_elements])]
        
    else:
        processed_column = None

    if processed_column is not None:
        use_column = True
    
    else:
        processed_column = [count]
        use_column = False

    for column in processed_column:
        count_df, data_type = count_terms(database, terms, count, column, use_column)
        print(f"\nCounting complete for {count} terms")
        count_type = "_".join(column.split(sep=" "))
        count_path = "_".join([data_type, count_type, datetime.now().strftime("%Y%m%d")])
        output = count_df.to_csv(count_path + ".csv", index=False)
        print(f"\nCount information is available at: {count_path}.csv")

if __name__ == "__main__":
    main()
