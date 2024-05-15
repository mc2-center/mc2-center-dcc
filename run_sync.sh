#!/usr/bin/env bash

python portal_tables/sync_publications.py --noprint
python portal_tables/sync_datasets.py --noprint
python portal_tables/sync_tools.py --noprint
