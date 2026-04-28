from synapseclient import Synapse
from synapseclient.models import CurationTask
import sys

syn = Synapse()
syn.login()

task_ids = sys.argv[1:]

for id in task_ids:
	task = CurationTask(task_id=id)
	task.delete(delete_source=True)