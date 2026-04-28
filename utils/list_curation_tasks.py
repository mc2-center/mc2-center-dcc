from synapseclient import Synapse
from synapseclient.models import CurationTask
import sys

syn = Synapse()
syn.login()

for task in CurationTask.list(project_id=sys.argv[1]):
    print(f"Task ID: {task.task_id}")
    print(f"Data Type: {task.data_type}")
    print(f"Instructions: {task.instructions}")
    print("---")