"""Combine Synapse projects that are separated 
by grant into the CCKP admin project table.

This script assumes the folders for PublicationView,
DatasetView, and ToolView, respectively, remain 
consistent in the CCKP admin project table.

author: victor.baham
"""

# command line arguments: 1. Portal - Grants Merged Synapse ID 2. CCKP Admin Project Synapse ID
# test value for argument 1: syn42801895
# test value for argument 2: syn35629947 

import argparse
import synapseclient
from synapseclient import Project, File, Folder
from synapseclient import Schema, Column, Table, Row, RowSet, as_table_columns
import itertools
import pandas as pd

def get_args():
    #Retrieve Synapse table IDs from command line
    parser = argparse.ArgumentParser(
        description="Merge publications, datasets, and tools from Grants Merged table into CCKP admin table")
    parser.add_argument("-p1", "--project1",
                        type=str, required=True,
                        help="Portal - Grants Merged Synapse ID.")
    parser.add_argument("-p2", "--project2",
                        type=str, required=True,
                        help=("CCKP - MC2 Admin Synapse ID"))
    parser.add_argument("--dryrun", action="store_true")
    return parser.parse_args()

def getManifestsFromGrant(syn,theParent):
    """This function examines the GrantView table
    and returns a list of all Synapse table IDs
    separated by manifest type (publications, datasets,
    tools) for each grant"""

    # Access each grant project by accessing 'grantId'
    # column in grantView manifest
    # synId for grantView is hardcoded in and
    # will need to be changed for future use
    df = syn.tableQuery(f"select {'grantId'} from {theParent}").asDataFrame()
    gIds = df['grantId'].tolist()

    # for each grant, get publications,
    # datasets, and tools 

    # value inside dictionary will reset upon each
    # loop iteration, so appending to this list 
    # maintains each one
    mTypes = []
    mIds = []
    for gId in gIds:
        grantX = syn.get(gId)
        # By accessing the grants, it is possible
        # to access their 'children'

        # extract manifest type and table id from generator
        mTypes.append([m['name'] for m in syn.getChildren(grantX, includeTypes=['table'])])
        mIds.append([m['id'] for m in syn.getChildren(grantX, includeTypes=['table'])])

    # with each synapse table id mapped to a type of manifest
    # it is possible to begin adding them to the staging table
    # but should flatten list first for easier key-val mapping
    flat_mTypes = list(itertools.chain(*mTypes))
    flat_mIds = list(itertools.chain(*mIds))

    idToManType = dict(zip(flat_mIds, flat_mTypes))
    print(idToManType)

    # use dictionary of synapse id to manifest type
    # to prepare to separate data into rows
    pubsId = []
    datasetsId = []
    toolsId = []

    for key,val in idToManType.items():
        if val == 'publications':
            pubsId.append(key)
        elif val == 'datasets':
            datasetsId.append(key)
        else:
            toolsId.append(key)
    return pubsId, datasetsId, toolsId

def getEachManifestFromCCKP(syn, theNewParent):
    """this function reads in the Synapse ID of the
    CCKP Admin table and finds the table IDs of the
    PublicationView, DatasetView, and ToolView manifests"""

    pMan = syn.findEntityId('PublicationView', theNewParent)
    dMan = syn.findEntityId('DatasetView', theNewParent)
    tMan = syn.findEntityId('ToolView', theNewParent)

    return pMan, dMan, tMan

def main():
    syn = synapseclient.Synapse()
    syn.login()
    
    args = get_args() 
    originTable = args.project1
    destinationTable = args.project2

    # these are the lists of each kind of manifest pulled from
    # all individual grants
    [allPub, allData, allTool] = getManifestsFromGrant(syn, originTable)
    # these are the table IDs of the PublicationView, DataView, and ToolView
    # manifests in the CCKP Admin Project table
    [synPub, synData, synTool] = getEachManifestFromCCKP(syn, destinationTable)
    
    # create merged DataFrame and write to
    # PublicationView manifest in CCKP
    pub_dfs = []
    for pub in allPub:
        pub_df = syn.tableQuery(f"select * from {pub}").asDataFrame()
        pub_dfs.append(pub_df) 
    final_pubs = pd.concat(pub_dfs, ignore_index=True)
    syn.store(Table(synPub, final_pubs))

    # create merged DataFrame and write to
    # DatasetView manifest in CCKP
    dataset_dfs = []
    for dataset in allData:
        dataset_df = syn.tableQuery(f"select * from {dataset}").asDataFrame()
        dataset_dfs.append(dataset_df)
    final_datasets = pd.concat(dataset_dfs, ignore_index=True)   
    syn.store(Table(synData, final_datasets))

    # create merged DataFrame and write to
    # ToolView manifest in CCKP
    tool_dfs = []
    for tool in allTool:
        tool_df = syn.tableQuery(f"select * from {tool}").asDataFrame()
        tool_dfs.append(tool_df)
    final_tools = pd.concat(tool_dfs, ignore_index=True)
    syn.store(Table(synTool, final_tools))

    print('The tables have been successfully merged into the CCKP Admin project.')

if __name__ == "__main__":
    main()