from pprint import pprint
from synapseclient.extensions.curator import (
    create_record_based_metadata_task,
    create_file_based_metadata_task,
    query_schema_registry
)
from synapseclient import Synapse

syn = Synapse()
syn.login()

task_type = "record"

project = "syn71723047"
folder = "syn72664210"
record_view_name = "ProposedSchema2v2"
record_desc = "Testing schema"
task_name = "ProposedSchema2_Testv2"
primary_keys = ["PatientID"]
instructions = "Enter testing information"
uri = "ADA.PSI-ProposedCuratorSchema2-2.0.0"

if task_type == "record":
	record_set, curation_task, data_grid = create_record_based_metadata_task(
    	synapse_client=syn,
    	project_id=project,
    	folder_id=folder,
    	record_set_name=record_view_name,
    	record_set_description=record_desc,
    	curation_task_name=task_name,
    	upsert_keys=primary_keys,
    	instructions=instructions,
    	schema_uri=uri,
    	bind_schema_to_record_set=True
	)

	print(f"Record-based workflow created:")
	print(f"  RecordSet: {record_set.id}")
	print(f"  CurationTask: {curation_task.task_id}")

elif task_type == "file":	
	entity_view_id, task_id = create_file_based_metadata_task(
    	synapse_client=syn,
    	folder_id=folder,
    	curation_task_name=task_name,
    	instructions=instructions,
    	attach_wiki=True,
    	entity_view_name=record_view_name,
    	schema_uri=uri
	)

	print(f"File-based workflow created:")
	print(f"  EntityView: {entity_view_id}")
	print(f"  CurationTask: {task_id}")

else:
	print(f"Task type {task_type} not recognized. Exiting.")
	quit()
