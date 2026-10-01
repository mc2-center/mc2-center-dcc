# synapseclient OOP migration — Phase 2

## Context

Phase 1 (merged via `synapse-client-oop-refresh`) migrated the shared portal-sync core: `portal_tables/utils.py`, all 7 `sync_*.py` scripts, `tally_themes.py`, `check_publications_status.py`, and CI. That left ~25 files still on the legacy `Synapse`-handle API, deliberately deferred because they're lower-traffic (manual/one-off scripts, not CI-scheduled) but individually harder: hand-built REST transactions, JSON-Schema service calls, Team member management, and two genuine ETag/partial-column-update patterns with real data-loss risk if migrated carelessly.

This plan covers that remaining ~25-file surface, on `synapse-client-oop-refresh-2` (branched from Phase 1). `curator_tools/` remains untouched per your standing decision.

### Constraints (same as Phase 1)

- No test suite. Verification is manual: static compile, a residual-legacy-call grep sweep, and live `--dryrun`/read-only checks against production Synapse where a script supports it.
- No `asyncio` in the repo today. **One exception, decided for this phase:** `syn.getChildren()`'s only OOP replacement (`synapseclient.api.get_children`) is async-only with no sync wrapper. You asked me to introduce a small `asyncio.run()` wrapper for it rather than keep the legacy call — this will be the first asyncio usage in the repo, isolated to a single local helper.
- Match each file's existing style; no repo-wide linting.

## Scope, grouped by risk (execution order)

### Group A — mechanical, low risk (same patterns as Phase 1)

Read-only or simple create-and-store scripts. Same substitutions Phase 1 already validated live:

| File | Legacy → OOP |
|---|---|
| `utils/get_entity_ids.py` | `syn.findEntityId` → `operations.find_entity_id` |
| `utils/make_folders.py` | `Folder(name, parent=)` + `syn.store()` → `Folder(name=, parent_id=).store()`; drops the redundant `syn.findEntityId` call after (the stored folder's `.id` is already available) |
| `utils/create_id_folders.py`, `annotations/create_id_folders.py` (near-duplicates) | same `Folder`/`.store()`/`syn.delete()` → `.delete()` pattern |
| `utils/check_cert.py` | `syn.is_certified(id)` → `UserProfile(id=id).is_certified()` |
| `utils/get_abstracts.py` | `syn.tableQuery().asDataFrame()` → `Table.query()`; `syn.store(Table(id, df))` → `Table(id=id).store_rows(values=df)` (query already selects all needed columns, so this is a safe direct swap — see Group C for why that check matters) |
| `annotations/gen-mp-csv.py` | `syn.tableQuery().asDataFrame()` → `Table.query()` (read-only) |
| `portal_tables/union_qc.py` | same tableQuery swap; `syn.get(ref, downloadLocation=...)` → `operations.get(ref, file_options=FileOptions(download_location=...))` (read-only, no writes) |
| `utils/table_to_annotations.py` | `syn.get_annotations`/`syn.set_annotations` → `operations.get(entity_id)`, mutate `.annotations`, `.store()`. The `RecordSet(source_id).get()` branch is already OOP from Phase 1-adjacent work — just switch to keyword form `RecordSet(id=source_id)` for consistency |
| `utils/create_curation_task.py` | one remaining legacy call: `Folder(name=f"{data_type}", parent=project)` + `syn.store()` → `Folder(name=..., parent_id=project).store()` |
| `annotations/upload-manifests.py` | dead code cleanup only: unused `login()` helper and its `synapseclient` import are never called (`# syn = login()` is commented out) — delete both |
| `utils/upload_files.py` | login style only (`synapseclient.login()` → `Synapse()+.login()`); **keep `synapseutils.syncToSynapse`** — see "Deliberately staying on legacy" below |
| `utils/build_datasets.py` | `syn.get(entity_id, downloadFile=False)` → `operations.get(...)`; legacy `Dataset(name=, parent=, dataset_items=)` + `syn.store()` → OOP `Dataset(name=, parent_id=).store()`. **`add_items(dataset_items=[...], force=True)` has no bulk equivalent** — the OOP `Dataset.add_item()` takes one `EntityRef`/`File`/`Folder` at a time, so this becomes a loop. Verify duplicate-item handling during testing since `force=True`'s behavior isn't replicated 1:1. **Keep `synapseutils.walk`** — no OOP equivalent for pure enumeration without a full recursive download |
| `utils/trim_datasets.py` | `syn.get(dataset, downloadFile=False)` → `operations.get(...)`; `.properties.datasetItems` (legacy dict access) → OOP `Dataset(id=...).get()` populates items directly; `.remove_item()` name is unchanged, already matches the OOP method; `syn.store()` → `.store()` |

### Group B — schema/view/team model changes

Same shape as Phase 1's `EntityView` scope-mutation work, applied to the entity-management scripts:

| File | Design |
|---|---|
| `portal_tables/build_table_view.py` | `EntityViewSchema(name=, parent=, scopes=, includeEntityTypes=[...], addDefaultViewColumns=)` + `syn.store()` → `EntityView(name=, parent_id=, scope_ids=set(...), view_type_mask=ViewTypeMask.<TYPE>)` + `.store()`. `EntityViewType.TABLE/FILE/PROJECT` → `ViewTypeMask.TABLE/FILE/PROJECT` (confirmed present in `synapseclient.models`) |
| `portal_tables/merge_tables.py` | `MaterializedViewSchema(name=, parent=, definingSQL=)` + `syn.store()` → `MaterializedView(name=, parent_id=, defining_sql=).store()`. Also: dedupe the identical duplicate `get_table_ids`/`get_record_sets` definitions (the file defines `get_record_sets` twice, verbatim — the second silently shadows the first); fix `get_table_ids`'s `pd.DataFrame(table_id_sheet)` (wraps the raw query object instead of calling `.query()`, then indexes column `0` positionally) to use `Table(id=source_id).query(query=...)` directly, which already returns a properly-columned DataFrame |
| `utils/reset_teams.py` | `syn.getTeamMembers(team_id)` → `Team(id=team_id).members()`, iterating `TeamMember.member.owner_id` instead of the legacy dict's `"ownerId"`; `syn.getTeam(team_id)` → `Team(id=team_id).get()`, reading `.name` directly. **`syn.restDELETE(f"/team/{team_id}/member/{user_id}")` has no OOP equivalent — the `Team` model has no member-removal method at all** — keep the raw REST call (it isn't deprecated) |

**`portal_tables/unify_grant_tables.py` is out of scope** — confirmed unused, so it's left entirely on the legacy API rather than migrated.

**The `getChildren` asyncio wrapper** (used by `schema_update.py`, its only remaining caller now that `unify_grant_tables.py` is out of scope):
```python
import asyncio
from synapseclient.api import get_children

def _get_children(parent, include_types=None, *, synapse_client=None):
    """Sync wrapper around the async-only get_children() API."""
    async def _collect():
        return [c async for c in get_children(parent, include_types=include_types, synapse_client=synapse_client)]
    return asyncio.run(_collect())
```
Small, local, and isolated to that one call site — not a repo-wide asyncio adoption.

### Group C — highest risk: hand-built REST transactions and column-nulling hazards

**`annotations/schema_update.py`** — hand-builds a `TableUpdateTransactionRequest`/`TableSchemaChangeRequest` dict and submits it via the *private* `syn._waitForAsync`. This collapses entirely:
```python
table = Table(id=my_table_synid).get(include_columns=True, synapse_client=syn)
for column_name in columns_to_modify:
    if column_name in table.columns:
        col = table.columns[column_name]
        col.column_type = "LARGETEXT"   # (or MEDIUMTEXT / STRING+maximum_size, per the existing branch logic)
table.store(synapse_client=syn)
```
Verified against the installed 4.13.0 source: mutating `table.columns[name].column_type` and calling `.store()` makes the framework detect the change, mint a replacement column (Synapse columns are immutable — a type change always creates a new column ID), and submit exactly the same `TableSchemaChangeRequest` shape through the proper async-job API. This removes the private-API dependency and the hand-rolled request-building entirely. Also migrate its `syn.getChildren()` call via the same wrapper as Group B, and `syn.get`/`syn.store(Column(...))` accordingly.

**`utils/synapse_json_schema_bind.py`** — the other full rewrite. Verified replacements for every call:
- `syn.get_available_services()` + `syn.service("json_schema")` → gone entirely, no service handle needed.
- Organization create-or-get: `SchemaOrganization(name=org_name).store()`, catching `SynapseHTTPError` and falling back to `SchemaOrganization(name=org_name).get()` — same try/except shape as today, just on the model.
- Schema registration: `JSONSchema(organization_name=org_name, name=schema_type).store(schema_body=schema_json, version=num_version)` replaces `org.create_json_schema(...)`.
- Binding — **this is the one call that most needed the OOP model**: `folder.bind_schema(uri, enable_derived_annotations=True)` replaces the manual `syn.restPUT(f"/entity/{id}/schema/binding", body=json.dumps({..., "enableDerivedAnnotations": True}))` for the AR case, and the same call with `enable_derived_annotations=False` (or the parameter omitted) replaces the non-AR `service.bind_json_schema(...)` path — both collapse into one call. Since the target entity's type varies (Project, EntityView, Folder, etc. depending on caller), fetch it generically first: `entity = operations.get(entity_id, synapse_client=syn)` — `File`, `Folder`, `Project`, `EntityView`, and `Table` (confirmed) all support `.bind_schema()`, so no type-specific branching is needed.

**`portal_tables/add_datasets_to_pub.py` and `annotations/update_pending_annotations.py`** — both do an ETag-guarded partial update, `syn.store(synapseclient.Table(table_id, df, etag=query.etag))`, where the **query only selects a subset of columns** (`SELECT pubMedId, dataset FROM ...` / `SELECT pubMedId, assay, tumorType, ... FROM ...`). This matters: `Table.store_rows()`'s documented full-row-replacement semantics say "the data for the columns not provided will be set to null." Migrating the query as-is and calling `store_rows()` on a partial-column DataFrame risks **nulling out every other column in the table for every updated row** — a serious regression, not a mechanical one.

The fix: **change both queries to `SELECT *`** before applying the same cell-level mutation logic, so the full row is always present when `store_rows()` sends the update. This is a deliberate, safety-driven change to these two files, not a straight port — flagging it explicitly rather than doing a "faithful" migration that would carry this risk silently. (`annotations/edit_legacy_annotations.py` already queries `SELECT *`, so it needs no such adjustment — straightforward `Table.query()` / `.store_rows()` swap.)

**`annotations/create_entity_links.py`** — the two direct `File(path=, name=, parent=, synapseStore=False)` + `syn.store()` calls migrate to `File(path=, name=, parent_id=, synapse_store=False).store()`. The file also calls into the external `geo_synapse` package (`geo-synapse @ git+...`), which expects a legacy-style `Synapse` client — since `Synapse()` instances work identically whether obtained via `synapseclient.login()` or `Synapse()+.login()`, this is not a blocker, just a note that the `geo_synapse.*` calls themselves are out of this repo's control and stay as-is.

## Deliberately staying on the legacy handle (no OOP equivalent exists)

Confirmed in the installed 4.13.0 source, not just assumed:
- `syn.sendMessage` (`utils/check_publications_status.py`, already noted in Phase 1)
- `syn.getWiki` / legacy `Wiki` construction (`portal_tables/create_grant_projects.py` — Phase 1 already deferred its Wiki/Folder/Team/Project creation functions; still out of scope here)
- `synapseutils.walk` (`utils/build_datasets.py`) — no enumeration-only OOP equivalent
- `synapseutils.syncToSynapse` (`utils/upload_files.py`) — 4.13.0 *does* add an OOP `sync_to_synapse()`, but it's a method on a `Project`/`Folder` instance and this script's manifest carries per-row `parentId`s with no single natural container to anchor the call on; the function-style call is also not deprecated, so there's no pressure to force it
- `syn.restDELETE` for team-member removal (`utils/reset_teams.py`) — the `Team` model has no member-removal method at all

## Verification

Same manual gate as Phase 1, since no test suite exists:

1. `python -m py_compile` on every changed file.
2. Residual-legacy-call grep sweep (`tableQuery`, `asDataFrame`, `getChildren`, `getColumns`, `getTeamMembers`, `getTeam`, `_waitForAsync`, `syn.service`, `get_available_services`, `restPUT`) — zero hits outside the explicitly-noted "stays on legacy" list above.
3. Read-only scripts (`union_qc.py`, `gen-mp-csv.py`) and any script with a `--dryrun` flag: run live against production Synapse, same as Phase 1's validation approach.
4. **`add_datasets_to_pub.py` / `update_pending_annotations.py` specifically:** before any live write, run the migrated `SELECT *`-based read side-by-side with the current production query and confirm the row set and non-`dataset`/non-`assay`-etc. column values match — this is the one place migration risk concentrates, so it gets an extra check beyond the standard sweep.
5. `schema_update.py`: dry-run the column-type change against one non-production table first (or use `Table.store(dry_run=True)`, which the OOP API supports natively and the legacy hand-rolled request did not) to confirm the generated `TableSchemaChangeRequest` matches expectations before touching a real portal table.
6. `synapse_json_schema_bind.py`: verify against a throwaway/test organization name first, not `MC2Center`, given schema registration is not easily reversible.

Granular commits, one per file/logical unit, matching Phase 1's convention. Implementation report saved to `plans/synapseclient_oop_migration_phase2_report.md` alongside this plan, same as Phase 1.
