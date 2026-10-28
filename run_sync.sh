#!/usr/bin/env bash
# Sync the manifests to the portal tables. The single list of sync steps:
# .github/workflows/sync-to-portal.yml calls this script.
# Extra arguments (e.g. --dryrun) are passed through to every script.
set -euo pipefail
cd "$(dirname "$0")"

python portal_tables/sync_publications.py --noprint "$@"
python portal_tables/sync_datasets.py --noprint "$@"
python portal_tables/sync_tools.py --noprint "$@"
