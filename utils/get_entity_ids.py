"""
Get synIDs for entities matching input name
"""

import synapseclient
import argparse
import pandas as pd
from pathlib import Path

### Login to Synapse ###
def login():

    syn = synapseclient.Synapse()
    syn.login()

    return syn


def get_args():
	
	parser = argparse.ArgumentParser(
        description='Get synapse project folder ids')
	parser.add_argument('-l',
						nargs='+',
                        help='Synapse project IDs to query.')
	parser.add_argument('-n',
                        type=str,
                        help='Entity name to search for in Synapse project.')
	return parser.parse_args()


def get_list(syn, name, projects):
	
	df = pd.DataFrame(columns=['projectId', 'entityId'])

	for project in projects:
		entityId = syn.findEntityId(name, project)
		newRow = pd.DataFrame([[project, entityId]], columns=['projectId', 'entityId'])
		df = pd.concat([df, newRow])
	
	df = df.reset_index(drop=True)
	return df

def main():
	
	syn = login()
	args = get_args()
	out = get_list(syn, args.n, args.l)

	outPath = Path('output/project_entity.csv')
	outPath.parent.mkdir(parents=True, exist_ok=True)
	out.to_csv(outPath, index=True)

if __name__ == "__main__":
    main()