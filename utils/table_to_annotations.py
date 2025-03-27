"""table_to_annotations.py

This script will query a Synapse table for metadata and apply it to an entity as annotations.

Usage:

author: orion.banks
"""

import synapseclient
from synapseclient import Annotations
import argparse
import pandas as pd


def get_args():
    """Set up command-line interface and get arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-t",
        type=str,
        help="Synapse Id of an entity to which annotations will be applied",
        required=False
    )
    parser.add_argument(
        "-c",
        type=str,
        help="The Component name of the schema associated with the entity to be annotated",
        required=False
    )
    parser.add_argument(
        "-s",
        type=str,
        help="Synapse Id of a table containing the data that should be converted to annotations.",
        required=False
    )
    parser.add_argument(
        "-m",
        type=str,
        help="Path to a CSV with the source table Synapse Ids and entity Ids to be annotated",
        required=False
    )
    return parser.parse_args()


def build_annotations(syn, component, source_id, target_id, mapping):
    
    entity = syn.get(target_id, downloadFile=False)
    query = f"SELECT * FROM {source_id} WHERE {component}_id = '{target_id}'"
    #additional case: applying annotations from a single table type to a whole bunch of files
    #would want to pull entire table and create + add annotations in a loop
    #could be a separate script
    annotations_to_add = syn.tableQuery(query).asDataFrame()
    print(f"Annotations acquired from Synapse table {source_id} for entity: {target_id}")
    if mapping is not None:
        annotations_to_add = map_annotations()
    annotations_to_add = annotations_to_add.to_dict()
    new_annotations = Annotations(target_id, entity.etag, annotations_to_add)

    new_annotations = syn.set_annotations(new_annotations)
    print(f"Annotations applied to Synapse entity: {target_id}")

def map_annotations(df: pd.DataFrame, column_map: list[tuple]) -> pd.DataFrame:
    """Map table columns names to different schema"""
    #define mapping between table column names and desired schema for entity, if different
    #mostly applicable for datasets or converting between MC2 and CRDC table headers

    for start, end in column_map:

        df[f"{end}"] = [
            x for x in df[f"{start}"]
        ]

        df = df.drop([f"{start}"])

    return df


def main():

    syn = (
        synapseclient.login()
    ) 

    args = get_args()

    target, component, source, sheet = args.t, args.c, args.v, args.s, args.m
    
    if sheet:
        idSet = pd.read_csv(sheet, header=None)
        if idSet.iat[0,0] == "entity" and idSet.iat[0,1] == "component":
            print(f"\nInput sheet read successfully!\n\nApplying annotations now...")
            idSet = idSet.iloc[1:,:]
            count = 0
            for row in idSet.itertuples(index=False):
                target = row[0]
                component = row[1]
                annotations = build_annotations(syn, component, source, target)  
                count += 1
            print(f"\n\nDONE ✅\n{count} entities had annotations applied.")
        else:
            print(f"\n❗❗❗ The table provided does not appear to be formatted for this operation.❗❗❗\nPlease check its contents and try again.")
    
    else: #if no sheet provided, run process for one round of inputs only
        if target and component:
            annotations = build_annotations(syn, component, source, target)
        else:
            print(f"\n❗❗❗ No dataset information provided.❗❗❗\nPlease check your command line inputs and try again.")

if __name__ == "__main__":
    main()
