# Migrate `synapseclient` usage to the object-oriented models API

## Context

This repo drives the Cancer Complexity Knowledge Portal's Synapse tables. Roughly 40 Python
files call `synapseclient`, and almost all of them use the legacy `Synapse`-handle API
(`syn.tableQuery`, `syn.store`, `syn.get`, `syn.delete`, …).

Checking the upstream client turned up the real deadline: **every legacy method this repo
depends on is already decorated `@deprecated(... "To be removed in 5.0.0")`.**

| Legacy call in this repo | Deprecated since | Replacement |
|---|---|---|
| `syn.tableQuery` | 4.9.0 | `Table.query()` / `models.query()` |
| `syn.create_snapshot_version` | 4.9.0 | `.snapshot()` on Table/EntityView/Dataset |
| `syn.getColumns`, `syn.getTableColumns` | 4.9.0 | `.columns` attribute |
| `syn.get_annotations`, `syn.set_annotations` | 4.9.0 | `.annotations` on any model |
| `syn.getChildren` | 4.9.0 | `Project`/`Folder` models |
| `syn.setPermissions` | 4.9.0 | `.set_permissions()` |
| `syn.getTeam`, `syn.getTeamMembers` | 4.9.0 | `Team.from_id()`, `Team.members()` |
| `syn.is_certified` | 4.9.0 | `UserProfile.is_certified()` |
| `syn.get`, `syn.store`, `syn.delete` | 4.11.0 | `synapseclient.operations` or model methods |
| `syn.findEntityId` | 4.11.0 | `operations.find_entity_id` |
| `syn.service`, `syn.get_available_services` | 4.11.0 | (no named replacement) |

**This is not hypothetical.** All three GitHub Actions workflows run
`pip install synapseclient` *unpinned*, so production already pulls whatever PyPI serves —
currently **4.13.0** (released 2026-06-09; local env is 4.12.0). When 5.0.0 ships, the nightly
portal syncs break with no warning and no test suite to catch it.

Intended outcome: move the code that matters onto the supported API, and close the
unpinned-CI hole so a major client release can't silently break the portal.

### Decisions taken (confirmed with user)

- **Phased**, core first. Phase 2 is a follow-up, not this branch.
- **Preserve truncate-and-reload semantics exactly** in `update_table` — no switch to `upsert_rows`.
- Switch CI to `pip install -r requirements.txt`.
- **Do not touch `curator_tools/`** at all.

### Constraints

- **No test suite exists** (no `tests/`, no `conftest.py`, no mocks — verified). Verification is
  manual against live Synapse, so `--dryrun` and snapshot-rollback are the only safety nets.
- No linter/formatter config; match each file's existing style.
- No `asyncio` anywhere in the repo. **Use the synchronous wrappers only** (`.get()`, `.store()`,
  `.query()`) — not `*_async()`.

---

## Phase 1 — scope

**`portal_tables/utils.py`** (the shared core, imported by 9 files) plus its 7 sync callers, plus
the 2 CI-run scripts in `utils/`.

| File | Why in Phase 1 |
|---|---|
| `portal_tables/utils.py` | `syn_login`, `update_table`, `identify_download_type` — used by everything |
| `portal_tables/sync_{datasets,education,grants,people,projects,publications,tools}.py` | 3 of these run in CI |
| `utils/tally_themes.py` | runs monthly in CI; has its **own** copy of `update_table` |
| `utils/check_publications_status.py` | runs monthly in CI |
| `.github/workflows/*.yml` (3 files) | close the unpinned-install hole |

---

## The core change: `portal_tables/utils.py:139-167` [APPROVED]

Today's `update_table` is snapshot → delete-all-rows → re-insert:

```python
syn.create_snapshot_version(table_id, label=today)
current_rows = syn.tableQuery(f"SELECT * FROM {table_id}")
syn.delete(current_rows)
df = df.map(lambda v: re.sub(r"[\r\n]+", " ", v) if isinstance(v, str) else v)
syn.store(synapseclient.Table(table_id, df.values.tolist()))
```

Becomes a 1:1 mapping onto `synapseclient.models.Table`:

```python
table = Table(id=table_id).get()
table.snapshot(label=today)
current_rows = table.query(query=f"SELECT * FROM {table_id}")   # -> DataFrame
# ... same print, len(current_rows) still works on a DataFrame ...
table.delete_rows(query=f"SELECT * FROM {table_id}")
df = df.map(...)                     # newline strip — KEEP VERBATIM
table.store_rows(values=df)
```

Three things to get right here:

1. **`df.values.tolist()` → `store_rows(values=df)` changes column binding from *positional* to
   *by name*.** Today the row lists line up with the live schema by position; `store_rows` matches
   on column name. Each sync script ends with `return df[col_order]` (e.g. `sync_people.py:51-71`),
   so names are well-defined — but they must match the live Synapse schema exactly.
   **Pre-flight check before any write:** for each of the 7 portal tables, compare
   `Table(id=...).get(include_columns=True).columns.keys()` against that script's `col_order`
   and report mismatches. `sync_publications.py` is the one to watch — its columns carry spaces
   (`"Pubmed Id"`) because, unlike its siblings, it never strips spaces from `manifest.columns`. [APPROVED]

2. **The newline-stripping block at `utils.py:156-165` must survive verbatim.** It fixes the
   blank-row corruption from commit `4f213a5` (row-versions 2230/2231 in `syn21897968`).
   `store_rows` still serializes through a CSV upload, so the same server-side bug applies. [APPROVED]

3. **Do not substitute `upsert_rows`.** It does not delete rows dropped from the manifest, and is
   explicitly non-transactional between its update and insert phases. Truncate-and-reload is the
   required semantics. [APPROVED]

### Also in `utils.py`

- `syn_login()` (L67-79) → `Synapse()` + `syn.login(silent=True)`, keeping the
  `SynapseNoCredentialsError` → `getpass` PAT fallback. `login` is *not* deprecated; this is a
  style change only, matching the house pattern in `curator_tools/`.
- `identify_download_type()` (L224-259) → `synapseclient.operations.get(...)`. **Keep the untyped
  get.** `sync_datasets.py:65-67` documents why: `models.Dataset.get()` errors on
  `DatasetView_id`s that point at non-Dataset entities. Compare the returned `concreteType`
  as it does now. [APPROVED]

## Per-file work in the sync scripts [APPROVED]

Mostly mechanical, one pattern repeated:

- `syn.tableQuery(q).asDataFrame()` → `Table(id=...).query(query=q)` (already returns a DataFrame;
  drop `.asDataFrame()`). ~12 call sites across the 7 scripts.
- `syn.get(manifest_id).path` → `operations.get(manifest_id).path`.
- `sync_grants.py:78-84` is the one non-mechanical spot — it mutates an entity view's scope via the
  legacy `.scopeIds` / `.add_scope()`:
  ```python
  view = EntityView(id=table).get()
  view.scope_ids = set(view.scope_ids) | set(updated_scope)
  view.store()
  ```
  Note `models.EntityView.scope_ids` is a `Set[str]` of bare IDs, while the legacy `.scopeIds`
  returned IDs *without* the `syn` prefix — hence the `"syn" + scope` juggling on L81. Normalize
  once and drop that workaround.

## CI workflows

In all three of `.github/workflows/{sync-to-portal,update-theme-graphs,publications-status-check}.yml`,
replace `pip install synapseclient pandas [bs4 lxml]` with `pip install -r requirements.txt`.
`bs4`/`lxml` are used by `check_publications_status.py` but are **not** in `requirements.txt` —
add them there, or that workflow breaks on the switch.

---

## Flagged for your call (not doing these unless you say so)

1. **`requirements.txt` floor is `>=4.10.0`.** Once CI installs from it, that floor becomes
   load-bearing. pip will still resolve to 4.13.0 today, but the floor no longer reflects what the
   migrated code needs. **Recommend raising to `>=4.13.0`.** You didn't select this earlier, so
   flagging rather than assuming. [APPROVED]
2. **Local env is 4.12.0, CI runs 4.13.0.** Verification would happen against a different version
   than production. Recommend `pip install -U synapseclient` before verifying. [REVIEW: just use 4.13.0 everywhere]
3. **Bugs found in Phase 1 files** — all pre-existing, none caused by this migration: [APPROVED: add fixes]
   - `create_grant_projects.py:183` calls `get_args()` unconditionally, so when
     `sync_grants.py:61` uses it as a library it re-parses the *caller's* CLI flags.
   - `sync_grants.py` logs in twice per run (`utils.syn_login()` at L50, then
     `process_new_grants` does its own `Synapse().login()` at L181-182).
   - `utils/tally_themes.py:119-123` is a second, divergent copy of `update_table` that
     **lacks the newline-stripping fix** — so it is still exposed to the blank-row corruption
     that `4f213a5` fixed in `portal_tables/utils.py`.

   Say the word and I'll fix these in-branch; otherwise I'll migrate them as-is and leave the
   behavior untouched.

---

## Phase 2 (follow-up branch, not now) [APPROVED]

~25 remaining files. Notable ones, for sizing only:
- `annotations/schema_update.py` — the hardest. Hand-builds a `TableUpdateTransactionRequest` and
  submits it through the **private** `syn._waitForAsync`. Collapses to
  `Table.get(include_columns=True)` → mutate `.columns` → `.store()`.
- `utils/synapse_json_schema_bind.py` — built entirely on `syn.service("json_schema")` and
  `syn.restPUT`, both removed in 5.0.0. Replacement is `models.schema_organization` +
  `Folder.bind_schema()`.
- `portal_tables/{build_table_view,merge_tables,unify_grant_tables}.py` — `EntityViewSchema` →
  `EntityView`, `MaterializedViewSchema` → `MaterializedView`.
- `utils/reset_teams.py` — `getTeamMembers`/`restDELETE` → `Team` model.

**Stays on the legacy handle either way** (not deprecated, no models equivalent):
`syn.sendMessage`, `syn.getWiki`, `synapseutils.walk`, `synapseutils.syncToSynapse`.

---

## Verification

No automated tests exist, so this is a manual gate. In order:

1. **Static:** `python -m compileall portal_tables utils` and import each changed module.
2. **Deprecation sweep** — the objective signal that the migration worked:
   ```bash
   python -W error::DeprecationWarning portal_tables/sync_education.py --dryrun
   ```
   Any surviving legacy call raises instead of warning.
3. **Schema pre-flight** (before any write): for each of the 7 portal tables, diff live column
   names against the script's `col_order`. Must be empty.
4. **Dry runs:** every sync script with `--dryrun`, which writes a CSV instead of calling
   `update_table`. Diff each output against the CSV produced by the current `main` code — these
   should be **byte-identical**, since Phase 1 changes only the transport, not the data.
5. **One live write**, smallest table first (`sync_education`). Confirm afterwards:
   row count matches the manifest, a new snapshot version exists with today's label, and no blank
   rows appear. The snapshot from step 5 is the rollback.
6. Then the remaining tables, then trigger `sync-to-portal.yml` via `workflow_dispatch`.

Rollback for any table is its pre-write snapshot version.
