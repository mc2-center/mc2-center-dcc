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
        help="Path to a CSV file with entity Synapse Ids and Components on each row",
        required=False
    )
    return parser.parse_args()


def map_table_to_schema(syn, component, table_id, dataset_id):

    
    #mapping = mapping_dict #define here or pull from data-models
    entity = syn.get(dataset_id)
    query = f"SELECT * FROM {table_id} WHERE {component}_id = '{dataset_id}'"
    annotations_to_add = syn.tableQuery(query).asDataFrame()
    print("Annotation table acquired from Synapse")
    #get Dataset View annotations from table. Suggest using union table; ensure scope is updated before pulling
    #make DatasetView_id the index
    #replace column names with schema key names and reference in assignment below
    annotations_to_add = annotations_to_add.to_dict()
    print(annotations_to_add)
    new_annotations = Annotations(dataset_id, entity.etag, annotations_to_add) #pull current annotations - should be empty if schema was just bound
    #print("Current target annotations acquired from Synapse")
    print(new_annotations)
    
    #synapse_annotations = syn.to_synapse_annotations(annotations_to_add)
    #for key in current_annotations: #determine correct reference, but want to loop through annotation keys and add values from table
     #   print("Updating target annotations")
      #  current_annotations[key] = annotations_to_add.at([dataset_id, key])
       # print(current_annotations)
    
    new_annotations = syn.set_annotations(new_annotations)
   
def main():

    syn = (
        synapseclient.login()
    ) 

    args = get_args()

    target, component, version, sheet= args.t, args.c, args.v, args.s
    
    if sheet:
        idSet = pd.read_csv(sheet, header=None)
        if idSet.iat[0,0] == "entity" and idSet.iat[0,1] == "component":
            print(f"\nInput sheet read successfully!\n\nBinding schemas now...")
            idSet = idSet.iloc[1:,:]
            count = 0
            for row in idSet.itertuples(index=False):
                target = row[0]
                component = row[1]
                annotations = map_table_to_schema(syn, component, "syn52752399", target)  
                count += 1
            print(f"\n\nDONE ✅\n{count} schemas bound")
        else:
            print(f"\n❗❗❗ The table provided does not appear to be formatted for this operation.❗❗❗\nPlease check its contents and try again.")
    
    else: #if no sheet provided, run process for one round of inputs only
        if target and component:
            annotations = map_table_to_schema(syn, component, "syn52752399", target)
        else:
            print(f"\n❗❗❗ No dataset information provided.❗❗❗\nPlease check your command line inputs and try again.")

if __name__ == "__main__":
    main()
