"""
list_curation_tasks.py

This script uses the synapse client to list curation tasks for a given Synapse Project.

Usage:

python list_curation_tasks.py [PROJECT_SYNAPSE_ID]

Prints Task ID, Data Type, and Instructions for the task to stdout.
"""

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