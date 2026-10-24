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

task_ids = sys.argv[1:]

for id in task_ids:
	task = CurationTask(task_id=id)
	task.delete(delete_source=True)
