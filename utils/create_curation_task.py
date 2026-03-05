"""
Script to create a curation task in Synapse

This script allows users to create either a record-based or file-based curation task in Synapse.

usage: create_curation_task.py [-h] [-p PROJECT] [-f FOLDER] [-r RECORD_VIEW_NAME] [-d RECORD_DESCRIPTION]
                               [-t TASK_NAME] [-k PRIMARY_KEYS [PRIMARY_KEYS ...]] [-i INSTRUCTIONS]
                               [-uri SCHEMA_URI] [-path SCHEMA_PATH] [-y {Record,File}] [-input INPUT_PATH]

options:
  -h, --help            show this help message and exit
  -p, --project PROJECT
                        Synapse Id associated with a project to store the task and metadata
  -f, --folder FOLDER   Synapse Id associated with a folder to store the FileView or RecordSet
  -r, --record_view_name RECORD_VIEW_NAME
                        Name to be applied to the FileView or RecordSet
  -d, --record_description RECORD_DESCRIPTION
                        Description to be applied to a RecordSet
  -t, --task_name TASK_NAME
                        Name to be applied to the curation task.
  -k, --primary_keys PRIMARY_KEYS [PRIMARY_KEYS ...]
                        List of primary key(s) associated with the curation task.
  -i, --instructions INSTRUCTIONS
                        Directions to be associated with the curation task.
  -uri, --schema_uri SCHEMA_URI
                        URI of schema to be associated with the RecordSet or FileView and files
  -path, --schema_path SCHEMA_PATH
                        Path of schema to be associated with the RecordSet or FileView and files
  -y, --task_type {Record,File}
                        Type of curation task to be created; one of 'Record' or 'File'.
  -input, --input_path INPUT_PATH
                        Path to a CSV listing inputs for this script. 
"""

import argparse
import pandas as pd
from pprint import pprint
from synapseclient.extensions.curator import (
	create_record_based_metadata_task,
	create_file_based_metadata_task,
	query_schema_registry
)
from synapseclient import Synapse
import synapse_json_schema_bind

def get_args():
	"""Set up command-line interface and get arguments."""
	parser = argparse.ArgumentParser()
	parser.add_argument(
		"-p",
		"--project",
		type=str,
		help="Synapse Id associated with a project to store the task and metadata",
		required=False,
		default=None,
	),
	parser.add_argument(
		"-f",
		"--folder",
		type=str,
		help="Synapse Id associated with a folder to store the FileView or RecordSet",
		required=False,
		default=None,
	),
	parser.add_argument(
		"-t",
		"--data_type",
		type=str,
		help="Type of metadata associated with the FileView or RecordSet",
		required=False,
		default=None,
	),
	parser.add_argument(
		"-d",
		"--record_description",
		type=str,
		help="Description to be applied to a RecordSet",
		required=False,
		default="Example description.",
	),
	parser.add_argument(
		"-k",
		"--primary_keys",
		nargs="+",
		help="List of primary key(s) associated with the curation task.",
		required=False,
		default=None,
	),
	parser.add_argument(
		"-i",
		"--instructions",
		type=str,
		help="Directions to be associated with the curation task.",
		required=False,
		default="Example instructions.",
	),
	parser.add_argument(
		"-uri",
		"--schema_uri",
		type=str,
		help="URI of schema to be associated with the RecordSet or FileView and files",
		required=False,
		default=None,
	),
	parser.add_argument(
		"-path",
		"--schema_path",
		type=str,
		help="Path of schema to be associated with the RecordSet or FileView and files",
		required=False,
		default=None,
	),
	parser.add_argument(
		"-y",
		"--task_type",
		type=str,
		choices=["Record", "File"],
		help="Type of curation task to be created; one of 'Record' or 'File'.",
		required=False,
		default=None,
	),
	parser.add_argument(
		"-input",
		"--input_path",
		type=str,
		help="Path to a CSV listing inputs for this script.",
		required=False,
		default=None,
	),
	parser.add_argument(
		"-v",
		"--version",
		type=str,
		help="Schema version",
		required=False,
		default="1.0.0",
	),
	return parser.parse_args()

def main():
	syn = Synapse()
	syn.login()

	args = get_args()

	project, folder, data_type, record_desc, primary_keys, instructions, schema_uri, schema_path, task_type, sheet, version = args.project, args.folder, args.data_type, args.record_description, args.primary_keys, args.instructions, args.schema_uri, args.schema_path, args.task_type, args.input_path, args.version
	org = "MC2Center"
	record_view_name = "_".join([org, data_type, "RecordSet"])
	task_name = "_".join([org, data_type, "CurationTask"])

	if sheet is not None:
		input_sheet = pd.read_csv(sheet, header=0)
		# assign content as libary of tuples, per row

	if schema_path is not None:
		schema_uri = synapse_json_schema_bind.synapse_json_schema_bind(target=None, url=None, path=schema_path, org_name="MC2Center", includes_ar=None, no_bind=True, version=f"v{version}")

	if task_type.capitalize() == "Record":
		record_set, curation_task, data_grid = create_record_based_metadata_task(
			synapse_client=syn,
			project_id=project,
			folder_id=folder,
			record_set_name=record_view_name,
			record_set_description=record_desc,
			curation_task_name=task_name,
			upsert_keys=primary_keys,
			instructions=instructions,
			schema_uri=schema_uri,
			bind_schema_to_record_set=True
		)

		print(f"Record-based workflow created:")
		print(f"  RecordSet: {record_set.id}")
		print(f"  CurationTask: {curation_task.task_id}")

	elif task_type.capitalize() == "File":
		entity_view_id, task_id = create_file_based_metadata_task(
			synapse_client=syn,
			folder_id=folder,
			curation_task_name=task_name,
			instructions=instructions,
			attach_wiki=True,
			entity_view_name=record_view_name,
			schema_uri=schema_uri
		)

		print(f"File-based workflow created:")
		print(f"  EntityView: {entity_view_id}")
		print(f"  CurationTask: {task_id}")

	else:
		print(f"Task type {task_type} not recognized. Exiting.")
		quit()

if __name__ == "__main__":
    main()
