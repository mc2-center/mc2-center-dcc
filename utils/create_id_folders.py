"""create_id_folders.py


author: orion.banks
"""

import synapseclient
from synapseclient import Folder
import synapseutils
import argparse
import pandas as pd
import numpy as np

def get_args():
    """Set up command-line interface and get arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("-m",
						type=str,
                        help="path to manifest listing file paths and target folders in csv format")
    parser.add_argument("-t",
						type=str,
						choices=["DatasetView", "EducationalResource", "PublicationView", "ToolView"],
                        help="Type of manifest being submitted")
    return parser.parse_args()


def get_names(manifest, name_column):

	path_name_target = []
	
	paths_sheet = pd.read_csv(manifest)

	paths = paths_sheet['file_path'].tolist()

	targets = paths_sheet['target_id'].tolist()

	for path, target in zip(paths, targets):
		
		df = pd.read_csv(path)
		names = df[f'{name_column}'].tolist()

		for name in names:
			
			folder_info = (path, name, target)
			path_name_target.append(folder_info)
	
	return path_name_target
		

def add_folders(syn, path_name_target):
	
	paths, names, targets = zip(*path_name_target)

	path_name_id = []
	
	for p, n, t in zip(paths, names, targets):
		
		n = n.translate(str.maketrans('', '', '[]:/!@#$<>'))

		folder = Folder(n, parent = t)
		folder = syn.store(folder)
		id = syn.findEntityId(name = n, parent = t)
		info = (p, n, id)
		path_name_id.append(info)
		syn.delete(id)
		
	return path_name_id

def add_ids_to_manifests(path_name_id, name_column, primary_key):
	
	df_to_merge = pd.DataFrame.from_records(path_name_id, columns = ['file_path', f'{name_column}', f'{primary_key}'])

	path_groups = df_to_merge.groupby(['file_path'], sort=False)

	for name, group in path_groups:

		base_df = pd.read_csv(name, index_col = False, dtype = str)
		info_df = group[[f'{name_column}', f'{primary_key}']]
		info_df = info_df.set_index(keys = np.arange(stop = len(info_df)))
		base_df[f'{primary_key}'] = info_df[f'{primary_key}']
		new_manifest = base_df.to_csv(path_or_buf = name, index = False)

def main():
	
	syn = synapseclient.login() #you can pass your username and password directly to this function

	args = get_args()

	manifest, data_type = args.m, args.t

	if data_type == "DatasetView":

		name_column = "Dataset Name"
		primary_key = "DatasetView_id"

	elif data_type == "ToolView":

		name_column = "Tool Name"
		primary_key = "ToolView_id"

	elif data_type == "EducationalResource":

		name_column = "Resource Title"
		primary_key = "EducationalResource_id"
	
	print("Capturing information from " + data_type + " manifests...")
	pnt = get_names(manifest, name_column)

	print("Generating Synapse IDs for each set of " + data_type + " entries...")
	pni = add_folders(syn, pnt)

	print("Adding Synapse IDs to " + primary_key + " column of " + data_type + " manifests")
	filled_manifests = add_ids_to_manifests(pni, name_column, primary_key)

	print("Manifests have been populated with Synapse IDs!")


if __name__ == "__main__":
    main()