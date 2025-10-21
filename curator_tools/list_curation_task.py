from pprint import pprint
from synapseclient import Synapse
from synapseclient.models.curation import CurationTask

PROJECT_ID = ""  # The Synapse ID of the project to list tasks from

syn = Synapse()
syn.login()


for curation_task in CurationTask.list(
    project_id=PROJECT_ID
):
    pprint(curation_task)