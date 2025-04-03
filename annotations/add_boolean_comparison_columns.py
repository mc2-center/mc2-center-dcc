import pandas as pd
import synapseclient
import argparse
import pandas as pd
from pathlib import Path
import subprocess
import re

# Login to Synapse
def login():

    syn = synapseclient.login()

    return syn

def get_tables(syn, tableIdList, mergeFlag):

    tables = []  # set up lists to store info
    names = []

    for tableId in tableIdList:
        # pull table from Synapse
        table = syn.tableQuery(f"SELECT * FROM {tableId}").asDataFrame().fillna("")
        # grab name of data type from table
        # assumes "Component" is first column in table
        name = table.iat[1, 0]
        # build path to store table as CSV
        manifestPath = Path(f"output/{name}/{name}.csv")
        # create folder to store CSVs
        manifestPath.parent.mkdir(parents=True, exist_ok=True)

        # convert df to CSV
        table.to_csv(manifestPath, index=False, lineterminator="\n")
        # if merging store the table for the next function
        if mergeFlag:
            tables.append(table)
        # if not merging, store the file path for the next function
        else:
            tables.append(manifestPath)
        # store the name for next functions
        names.append(name)

    return list(zip(tables, names))

def add_boolean_comparison_columns():

    syn_login = login()
    grants = get_tables(syn_login, ['syn64590399'], True)[0][0]
    pubs = get_tables(syn_login, ['syn52752398'], True)[0][0]
    datasets = get_tables(syn_login, ['syn52752399'], True)[0][0]
    tools = get_tables(syn_login, ['syn52820451'], True)[0][0]
    educ = get_tables(syn_login, ['syn52963530'], True)[0][0]
    
    grant_pubs = []
    grant_ds = []
    grant_tools = []
    grant_educ = []
    for i in range(len(grants)):
        grant_pubs.append(grants['GrantView_id'][i] in pubs['GrantView Key'].values)
        grant_ds.append(grants['GrantView_id'][i] in datasets['GrantView Key'].values)
        grant_tools.append(grants['GrantView_id'][i] in tools['GrantView Key'].values)
        grant_educ.append(grants['GrantView_id'][i] in educ['GrantView Key'].values)

    grants['Publication_boolean'] = grant_pubs
    grants['Dataset_boolean'] = grant_ds
    grants['Tool_boolean'] = grant_tools
    grants['Education_boolean'] = grant_educ


    pubs_ds = []
    pubs_tools = []
    pubs_educ = []
    for i in range(len(pubs)):
        pubs_ds.append(pubs['PublicationView_id'][i] in datasets['PublicationView Key'].values)
        pubs_tools.append(pubs['PublicationView_id'][i] in tools['PublicationView Key'].values)
        pubs_educ.append(pubs['PublicationView_id'][i] in educ['PublicationView Key'].values)

    pubs['Dataset_boolean'] = pubs_ds
    pubs['Tool_boolean'] = pubs_tools
    pubs['Education_boolean'] = pubs_educ

    ds_tools = []
    ds_educ = []
    for i in range(len(datasets)):
        ds_tools.append(datasets['DatasetView_id'][i] in tools['DatasetView Key'].values)
        ds_educ.append(datasets['DatasetView_id'][i] in educ['DatasetView Key'].values)

    datasets['Tool_boolean'] = ds_tools
    datasets['Education_boolean'] = ds_educ

    tools_educ = []
    for i in range(len(tools)):
        tools_educ.append(tools['ToolView_id'][i] in educ['ToolView Key'].values)

    tools['Education_boolean'] = tools_educ

    return [grants, pubs, datasets, tools, educ]
