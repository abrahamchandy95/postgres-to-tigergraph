# Temporal mule detection loader

Loads PhantomLedger's `mule-temporal` corpus into `Mule_Pattern_Learner`,
MulePatternLearner's TigerGraph 4.2.5 graph. To replace a loaded corpus, see
[A fresh push](#a-fresh-push).

```sh
tf-gnn-load --use-case mule-temporal schema-path   # run this file once in the Savanna GSQL editor
tf-gnn-load --use-case mule-temporal inspect
tf-gnn-load --use-case mule-temporal push
```

| Variable | Meaning |
|---|---|
| `HOST`, `GRAPHNAME`, `SECRET` | Savanna host, `Mule_Pattern_Learner`, that graph's REST++ secret |
| `MULE_PG_DSN` | Source database; falls back to `PG_DSN` |
| `MULE_EXPORT_DIR` | Default `artifacts/mule_temporal`; a new, empty one per export |
| `SHARD_BYTES` | Shard size, shared with card fraud |
| `MULE_UPLOAD_BYTES` | Request size, default 10,000,000 (avoids gateway timeouts) |
| `MULE_UPLOAD_WORKERS` | Parallel uploads within a dataset, default 1, maximum 8; vertices still load first |

The source is 27 text tables (`mt_Party`, `mt_Account`, ...) in schema
`mule_temporal` of the `phantomledger` database. `public.transactions` is not
used. Ids are already opaque, so no PII salt applies.

## Schema and data contract

[schema.gsql](../gsql/mule_temporal/schema.gsql) is MulePatternLearner's
`gsql/schema/schema.gsql` byte for byte: 8 vertex types, 7 discriminated
associations and 12 point-event relationships, each with a named reverse.
Recopy it when MulePatternLearner's changes; a test compares the two, reading
`../MulePatternLearner` or `MULE_PATTERN_LEARNER_DIR` (skipped if absent).

- Before installing or uploading, the push checks the live schema: attribute
  types and order, endpoints, reverse edges and discriminator flags.
  The only extra types allowed are those `mule install` adds,
  `Temporal_Training_Scope` and `Entity_In_Training_Scope` with its reverse.
- The push never drops or migrates types. [The migration](../gsql/mule_temporal/migrations/temporal_valid_time.gsql)
  is only for the old empty six-vertex schema; never run it on data.

### The Account table

`mt_Account` has MulePatternLearner's fifteen Account label columns, in this
order:

| # | Column | Type | # | Column | Type |
|---|---|---|---|---|---|
| 1 | `id` | STRING | 9 | `pu_label` | INT |
| 2 | `account_type` | STRING | 10 | `mule_label_effective_seq` | UINT |
| 3 | `is_external` | BOOL | 11 | `mule_label_effective_ts_ms` | UINT |
| 4 | `first_seen_seq` | UINT | 12 | `mule_label_available_seq` | UINT |
| 5 | `first_seen_ts_ms` | UINT | 13 | `mule_label_available_ts_ms` | UINT |
| 6 | `is_mule` | INT | 14 | `mule_ring_id` | INT |
| 7 | `mule_label_known` | BOOL | 15 | `mule_label_source` | STRING |
| 8 | `is_mule_masked` | BOOL | | | |

- All fifteen load exactly as given. PhantomLedger marks no label known and
  masks every account; MulePatternLearner's label reveal, not the loader,
  changes that, and never touches `mule_ring_id`.
- Booleans may be any case. Only `mule_label_source` may be NULL (loaded as
  the empty string).
- Ring ids must be at least -1; `verify` requires the graph's ring
  distribution to match the source's.

### Every table

- All 27 tables must have their exact columns. Empty tables are allowed and
  reported; at least one payment is required.
- One row per vertex id. Entity ids use PhantomLedger's 128-bit pseudonym
  format (another format needs a source adapter); event ids are the source's.
- Zelle payments are only in `Zelle_Transfer`, other rails in
  `Payment_Transaction`.
- Sequence numbers are kept, gaps included, and each tenure keeps its
  `valid_from_seq`, even for a repeated endpoint pair.

Cutoffs (the tenure rule applies in both directions):

```text
tenure:  valid_from_seq <= seed_seq AND (valid_to_seq == 0 OR seed_seq < valid_to_seq)
event:   event_seq < seed_seq
entity:  first_seen_seq <= seed_seq
```

Point edges carry their event's clocks. Ties keep the source's order. Sequence
gaps are not elapsed time; use `event_ts_ms`.

**The audit checks** required columns, nulls, PSV safety, unsigned ranges,
opaque ids, unique identities and roles, positive clocks, timestamp agreement,
finite amounts and presence flags, endpoint existence and visibility,
nonoverlapping tenures, disjoint event ids, shared chronological clocks, label
availability, Account mule flags and ring ids, sender and recipient presence,
exclusive token bindings, and bindings visible at each Zelle payment. The
schema has no association end times or known-time history to check.

## Export, resume, and verification

- `prepare` and `audit` change no tables; all reads share one read-only
  snapshot.
- `export` writes a manifest (source database, audit results, row and byte
  counts, SHA-256 per shard) and never overwrites one. A changed source needs
  a new export directory and an empty graph; an export from before the
  fifteen-column Account table is refused.
- `install-jobs` installs the [27 loading jobs](../gsql/mule_temporal/loading_jobs.gsql)
  and the read-only `mt_validate_graph`; no feature query or model.
- `load` checks every shard hash, refuses a graph with any vertex (naming the
  types), and resumes on rerun, skipping completed shards. A lock stops
  concurrent uploads; the load state is tied to one host and graph.
- `verify` compares all 8 vertex and 38 edge counts (both directions) with the
  manifest, and the Account `is_mule` and `mule_ring_id` distributions with
  PostgreSQL. It also checks clocks, first observations, intervals, Zelle label
  fields, Account ranges and participation clock parity. It writes
  `tigergraph_verification.json` and must pass before training.

On HTTP 403 `Exceeds the license limit`, raise the licensed capacity and rerun
`load` and `verify` with the same export directory. Do not clear the graph or
delete its checkpoint file. A partial load is not ready for training.

Single stages: `tf-gnn-load --use-case mule-temporal <stage>` with `audit`,
`export`, `install-jobs`, `load` or `verify`. After editing the Python
contract:

```sh
python scripts/generate_mule_gsql.py
python scripts/check_loader_contracts.py
python -m unittest discover -s tests
# MulePatternLearner elsewhere than ../MulePatternLearner:
MULE_PATTERN_LEARNER_DIR=/path/to/MulePatternLearner python -m unittest discover -s tests
# Real PostgreSQL fixtures (session-local objects only):
MULE_TEST_DSN='dbname=phantomledger' python -m unittest discover -s tests
```

## A fresh push

1. **Regenerate** with PhantomLedger `--usecase mule-temporal` (commit
   `525708f` or later), point `MULE_PG_DSN` at it, and run `inspect`. A bad
   table stops with `Source contract mismatch` and the columns found.
2. **Delete every vertex** in `Mule_Pattern_Learner`, `Temporal_Training_Scope`
   included; the schema, jobs and queries stay. A request may outlast the
   gateway, so repeat until every count is 0:

   ```sh
   python - <<'EOF'
   from tf_gnn_loader.tigergraph.client import Client
   from tf_gnn_loader.tigergraph.settings import Settings

   settings = Settings()
   assert settings.graphname == "Mule_Pattern_Learner", settings.graphname
   conn = Client(settings).conn
   for vertex_type in conn.getVertexTypes():
       print(vertex_type, conn.delVertices(vertex_type, timeout=3600))
   print(conn.getVertexCount("*", realtime=True))
   EOF
   ```

   **Never use `CLEAR GRAPH STORE` when another graph shares the instance (such
   as `TransactionFraud_GNN`): it empties every graph.** If the graph does not
   exist, run the `schema-path` file in the Savanna GSQL editor and put its new
   REST++ secret in `.env` here and in MulePatternLearner.
3. **Set a new, empty `MULE_EXPORT_DIR`**, such as
   `artifacts/mule_temporal_<date>`; the push resumes any export it finds.
4. **Run `tf-gnn-load --use-case mule-temporal push`.** It reinstalls the 27
   `mt_load_*` jobs and `mt_validate_graph`. Success prints `"pushed": true`,
   and `tigergraph_verification.json` has `"passed": true`. If it stops, rerun
   with the same `MULE_EXPORT_DIR`; do not clear again.
5. **In MulePatternLearner**, set a new `scope.id` for the built-in run (its
   "Set up a graph" guide, "After the data changes"), then run `mule install`
   and `mule train`. Before the label reveal, `validate_label_contract`
   reports one `invalid_unknown` per mule; after it, zero.

## Supervision and source limitations

- Zelle assignment and registration are synthetic in the 2024 research corpus.
  A Zelle label is available at the sequence after its payment, possibly in
  the same millisecond. Unknown labels stay `-1`, `false`, `0`, `0`.
- No label is a feature. Risk scores, fraud channels, splits and stored time
  encodings are never loaded, and first observation is never label discovery
  time.
- `mt_validate_graph` checks ingestion, not MulePatternLearner's label
  contract.
- A fraudulent payment does not make either endpoint a mule. Account training
  needs its own target: account, cutoff, horizon, label and label availability.
- Valid time cannot exclude backdated corrections, and the schema has no
  known-time fields. Keep source snapshots and arrival history before claiming
  knowledge-safe backtests on corrected production feeds.
