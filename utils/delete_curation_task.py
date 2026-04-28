"""
delete_curation_task.py

This script uses the synapse client to delete curation tasks based on their CurationTask ID.
To get a list of the Curation Tasks for a Synapse Project, use list_curation_tasks.py

Usage:

python delete_curation_task.py [CURATION_TASK_ID_LIST]

Note: Provide multiple CurationTask IDs as a space separated list.

Return:
None
"""

from synapseclient import Synapse
from synapseclient.models import CurationTask
import sys

syn = Synapse()
syn.login()

task_ids = sys.argv[1:]

for id in task_ids:
	task = CurationTask(task_id=id)
	task.delete(delete_source=True)