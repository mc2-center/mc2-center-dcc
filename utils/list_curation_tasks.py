"""
list_curation_tasks.py

This script uses the synapse client to list curation tasks for a given Synapse Project.

Usage:

python list_curation_tasks.py [PROJECT_SYNAPSE_ID]

Prints Task ID, Data Type, and Instructions for the task to stdout.
"""

from synapseclient.models import CurationTask
import sys

import importlib.util
import os
# removed when the mc2dcc package (CLI consolidation plan) lands
# portal_tables/utils.py, loaded by path: "utils" would collide with this package
_spec = importlib.util.spec_from_file_location(
    "portal_tables_utils",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "portal_tables", "utils.py"),
)
_portal_utils = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_portal_utils)
syn_login = _portal_utils.syn_login

syn = syn_login()

for task in CurationTask.list(project_id=sys.argv[1]):
    print(f"Task ID: {task.task_id}")
    print(f"Data Type: {task.data_type}")
    print(f"Instructions: {task.instructions}")
    print("---")
