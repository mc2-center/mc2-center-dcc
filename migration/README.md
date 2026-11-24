# Data model 14.0 migration scripts

This folder holds two scripts. They moved the MC2 Center tables to data model 14.0.

Each script ran one time for MC2. We keep them for reuse.
Other programs can use a script as it is. You can also reuse parts of a script.
These scripts are not part of the regular pipeline.

Their tests are in `tests/migration/`.

## migrate_grant_tables_14.py

Target: data model 14.0.

This script moves the grant-level manifest tables to shared RecordSets.
It does one component each time: PublicationView, DatasetView, ToolView or EducationalResource.
For each grant table, it renames the 13.1.0 headers and normalizes the values.
It then checks the keys and validates the rows against the 14.0 JSON Schema.
It reads the curated merged manifest to get the entity IDs and to list differences.

A dry run is the default. It writes only reports.
Use `--execute` to write to Synapse. This option needs `--write-log-table`.

Example, from the repository root:

    python migration/migrate_grant_tables_14.py --component PublicationView

## migrate_admin_manifests_14.py

Target: data model 14.0.

This script moves one admin manifest to a shared RecordSet.
The manifests are Grant, Project and PersonView.
It uses the rules and functions of `migrate_grant_tables_14.py`.
It imports that script, so keep the two files in the same folder.

A dry run is the default. It writes only reports.
Use `--execute` to write to Synapse. This option needs `--write-log-table`.

Example, from the repository root:

    python migration/migrate_admin_manifests_14.py --component GrantView

## Shared modules

Both scripts use these modules from `portal_tables/`:
`crosswalk`, `merge_tables`, `model_contract`, `pubmed`, `recordsets`,
`schema_validation`, `utils` and `value_maps`.
The admin script uses `model_contract`, `recordsets`, `utils` and `value_maps` directly.
Do not move these modules. Other code uses them too.

To run a script, use the repository root or this folder as the working directory.
