# Implementation report: mc2-center-dcc alignment with data model 14.0.0

Executed against `plans/impl_dcc_14_alignment.md`, 2026-10-23, on branch `model-alignment` (47 commits after `56a2ee7`; nothing pushed). Sonnet subagents implemented each task in its own worktree; Opus reviewed every diff, re-ran the tests, re-checked dry-run diffs, and merged. Two independent reviews covered the whole branch; their should-fix items are fixed below. `pytest tests/`: 185 passed.

## Status by task
| Task | Status | Evidence |
|---|---|---|
| D1 `model_contract` | Done | Pinned to `v14.0.0`. All 33 template header rows round-trip. Label map restricted to attribute nodes (536 in the real model), so enum values such as `Breast` are rejected as headers. |
| D2 `crosswalk` + consortium lookup | Done | `check_crosswalk` passes on all 7 live portal tables. |
| D3 publications | Done | Live dry run: 4773 rows identical to the pre-change run. |
| D4 datasets + DUO | Done | 1141 rows; one cell differs: a row whose manifest lists two synIDs now keeps the first instead of a random one. DUO IDs and short codes give the same text; unknown codes keep the row. |
| D5 tools, education | Done | Identical apart from list order inside cells (old code used `set()`) and `"['']"` → `""` in education `publicationId`. |
| D6 grants | Done; safety fix in progress | 160 rows identical. Per-view scope update bug fixed. See "Open" for the creation-order fix. |
| D7 projects, people | Done | Projects identical apart from theme order; people identical when the consortium step is bypassed (see I4b). |
| D8 hand-off | Done | Output headers equal the templates for legacy and P1 input; P1's real output passes with no renames or drops. Cap skips ID, key and alias columns. |
| D9 other scripts | Done | `header_maps.py` replaces `attribute_dictionary.py` (now unreferenced). |
| D10 cron `--dryrun` | Done | Both cron scripts run read-only. |
| D11 RecordSet upload | Blocked on I3 | — |
| D12 `merge_tables` | Bug fixes done; RecordSet mode blocked on I3 | Per-grant mirror tables kept: the MaterializedView union reads them. |
| D13 `csv_to_ttl` | Done | Runs on the real 14.0.0 model: 5942 triples, 1198 subjects. |
| D14 Makefile | Done | `make -n register-json` lists 38 commands; bare `make` prints help. |
| D15 upload driver | Blocked on D11/I3 | — |
| D16 `read_manifest` | Done | Dry runs identical for every sync script (datasets/tools/education modulo list order). |
| D17a deletions | **Not done** | `git rm` blocked by the permission classifier; needs the owner. |
| D17b consolidation | Done | 24 scripts use `syn_login()`; `tally_themes` uses the shared `update_table`; `reporter_grants.py` replaces the two RePORTER scripts; sync workflow runs `run_sync.sh`. |

## Fixes beyond the spec
- `gen-mp-csv.py`: the `grants` type returned no folder for every grant; an unmatched grant crashed (`a2ff0f7`).
- `sync_grants.py`: a grant already in the first view's scope was never added to the second view.
- `update_table` refuses an empty DataFrame (it would otherwise truncate a live table with no error).
- Deterministic list cells in the syncs (`set()` → ordered dedupe).
- `reporter_grants.py` fails on any failed RePORTER chunk instead of writing a partial report.
- One shared consortium resolver in `utils.py`; raw-string regexes in four scripts.

## Behaviour changes to know about
- `tally_themes.py` now snapshots its three count tables before each monthly write and restores them on failure.
- `edit_legacy_annotations.py` starts standardizing `Resource Media Accessibility` (the old map had a typo, `ed_accessbility`).
- `table_to_annotations.py` writes 14.0.0 keys (`Assay`, `DatasetDataUseCodes`) as dataset annotations.
- 24 scripts' bare `syn.login()` gained the PAT-prompt fallback (`syn_login()`); CI's `SYNAPSE_AUTH_TOKEN` still works.
- `run_sync.sh` stops on the first failed sync and passes extra flags (e.g. `--dryrun`) through.

## Open
- **Grant creation order (review finding, fix in progress on `dcc-grants-safety`):** validate the full frame before creating projects, never write `"None"` rows, reuse existing projects by name.
- **D17a deletions** and the newly unreferenced files (`attribute_dictionary.py`, `utils/reporter_project_query.py`, `utils/clean_reporter_results.py`) need the owner.
- **People sync** fails until I4b adds `HTAN`, `NCI` and `Sage Bionetworks` to the Consortium table.
- **DUO text:** 17 of 24 DUO codes in 14.0.0 have no portal access text; they would show "Access information not mapped: …" once manifests carry them.
- **Live data oddities seen in review (not caused here):** education `dataType` holds synIDs live; one dataset title has a corrupted thin space; tools `portalDisplay` is written as `"true"` (as before).
- Blocked on I3: D11, D12 RecordSet mode, D15.
