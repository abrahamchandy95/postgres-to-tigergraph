# Temporal mule detection loader

This additional use case loads the supplied TigerGraph 4.2.5 schema into
`Mule_Pattern_Learner`. The original card-fraud pipeline remains the default.

```sh
tf-gnn-load --use-case mule-temporal schema-path
tf-gnn-load --use-case mule-temporal inspect
tf-gnn-load --use-case mule-temporal push
```

Configure `HOST`, `GRAPHNAME=Mule_Pattern_Learner`, and `SECRET` in `.env`.
`MULE_PG_DSN` selects the temporal PostgreSQL database, falling back to `PG_DSN`.
`MULE_EXPORT_DIR` defaults to `artifacts/mule_temporal`, independently of the
card-fraud export. `SHARD_BYTES` applies to both use cases. The temporal feed
already has opaque IDs; the loader does not apply the card-fraud PII salt again.
`MULE_UPLOAD_BYTES` defaults to 10,000,000: large immutable shards are sent in
smaller, complete-row requests to avoid Savanna gateway timeouts. A retry of an
incomplete original shard replays identical rows safely; partitioning does not
change its manifest digest, sequence values, or edge identity.
`MULE_UPLOAD_WORKERS` defaults to 1 (maximum 8). Higher values upload independent
shards of one dataset concurrently, each with its own HTTP session. Every
dataset finishes before the next begins, so vertices still precede edges.
Successful shards are checkpointed even when another request in the batch fails.

Generate this use case into the shared `phantomledger` database, schema
`mule_temporal`. The loader reads 27 text staging tables named `mt_Party`,
`mt_Account`, etc. The shared `public.transactions` table is simulator audit
output, not this graph feed; it is not joined into temporal features.

## Schema and data contract

[Fresh-graph DDL](../gsql/mule_temporal/schema.gsql) mirrors the updated live schema:
eight vertex types, seven discriminated associations, and twelve point-event
relationships, each with its named reverse. The loader checks the live schema's
attribute types and order, endpoints, reverse edges, and discriminator flags
before installing jobs or uploading. It never automatically drops or migrates
types. The [historical migration](../gsql/mule_temporal/migrations/temporal_valid_time.gsql)
is retained only for the exact prior empty six-vertex schema; it is not a
general migration and must not be run against populated data. Account supervision
and payment pair-cache attributes are preserved. The six-column Account source
ends with `is_mule`; loading jobs map it to the final INT attribute in the graph.
Absent graph fields use `_` to retain schema defaults. Payment caches start empty.

All 27 PostgreSQL tables are required with their exact declared columns.
Zero-row tables are allowed and reported: the supplied feed has no non-Zelle
token participation. At least one payment is required. Vertex metadata must
have one row per ID. Persistent entities use PhantomLedger's domain-separated
128-bit pseudonym format; event IDs remain stable source IDs. Supporting a
different production token format requires an explicit source adapter.

Every Zelle payment stays exclusively in `Zelle_Transfer`; all other rails use
`Payment_Transaction`. Existing sequence numbers are preserved across all
tables, including their gaps. No per-table sequence is generated. Every tenure
keeps its `valid_from_seq` discriminator, including repeated endpoint pairs.
The cutoff rule in either traversal direction is:

```text
valid_from_seq <= seed_seq AND (valid_to_seq == 0 OR seed_seq < valid_to_seq)
```

Historical event context uses `event_seq < seed_seq`. Entity visibility uses
`first_seen_seq <= seed_seq`. Point edges repeat their parent event's clocks.
The loader preserves the source's ordering within equal timestamps; it does
not infer causality from an arbitrary identifier sort. Sequence differences
are not elapsed time; use `event_ts_ms` for durations.

Audits check required columns, nulls, PSV safety, unsigned ranges, opaque IDs,
unique identities and roles, positive clocks, timestamp agreement, finite
amounts and presence flags, endpoint existence/visibility, nonoverlapping
tenures, disjoint event identities, shared chronological clock witnesses,
label availability, sender/recipient presence, exclusive token bindings, and
bindings visible at each Zelle payment. Association end timestamps and
known-time histories are absent from this schema, so their chronology cannot
be independently reconstructed from these columns.

## Export, resume, and verification

`prepare` and `audit` validate the source without modifying stored tables.
Normalization uses session-local temporary views; all validation and export
reads run in one repeatable-read, read-only snapshot. The exporter writes one
dataset per graph type, with all vertices before all edges. It normalizes
booleans to `true`/`false` and datetimes to UTC-compatible GSQL values.

The export manifest records the source database, audit results, row/byte counts,
and SHA-256 for every shard. `export` refuses to replace an existing manifest.
`push` resumes that immutable export; source changes require a new export
directory and an empty target, rather than overwriting first-observation
metadata. A directory lock prevents concurrent uploads to the same artifacts.
Load state binds the manifest and loading contract to the TigerGraph host and
graph. Failed or interrupted shards are retried; completed shards are skipped.

`install-jobs` installs the [27 loading jobs](../gsql/mule_temporal/loading_jobs.gsql)
and the read-only `mt_validate_graph` query. `load` checks all shard hashes
before upload and rejects a fresh load into a populated graph. `verify`
compares all eight vertex counts and all 38 forward/reverse edge counts with
the manifest, using traversals rather than delayed degree statistics. Account
`is_mule` distributions must also match PostgreSQL exactly. It also
checks graph clocks, first observations, interval validity, label fields, and
participation clock parity. Results are written to `tigergraph_verification.json`.
Verification must pass before downstream training. No feature query or model
is installed.

If TigerGraph returns HTTP 403 with `Exceeds the license limit`, reads can still
work while loading is blocked. Increase licensed capacity before resuming
`load` and `verify` with the same export directory. Do not clear the graph again
or remove its checkpoint file: incomplete shards replay identically, and all
completed shards remain reusable. A partial load is not ready for training.

```sh
# Individual stages and recovery after an interrupted upload:
tf-gnn-load --use-case mule-temporal audit
tf-gnn-load --use-case mule-temporal export
tf-gnn-load --use-case mule-temporal install-jobs
tf-gnn-load --use-case mule-temporal load
tf-gnn-load --use-case mule-temporal verify

# After editing the Python contract:
python scripts/generate_mule_gsql.py
python -m unittest discover -s tests
# Optional real PostgreSQL fixtures; only session-local objects are created:
MULE_TEST_DSN='dbname=phantomledger' python -m unittest discover -s tests
python scripts/check_loader_contracts.py
```

## Supervision and source limitations

The 2024 research corpus models Zelle assignment and registration synthetically.
Its Zelle labels use an explicit synthetic oracle: availability is the next
sequence after the payment, possibly at the same millisecond. Unknown labels
must remain `-1`, `false`, `0`, `0`. No label field is a feature, and this
loader never copies risk scores, fraud channel names, train/test splits, or
persisted time encodings from PostgreSQL into the graph.

The source now supplies `Account.is_mule` (`1` mule, `0` non-mule, `-1` unknown).
This synthetic role flag is copied exactly as supervision. It supplies no label
effective or availability clocks. The loader leaves `mule_label_known=false`,
`is_mule_masked=true`, `pu_label=0`, and all label clocks at zero; it does not
substitute first-observation time for label discovery. These raw flags alone
are not a complete temporal training-label contract. In particular, the separate
strict account-supervision query can reject positive flags until real label
timing is supplied; `mt_validate_graph` validates raw ingestion and flag parity.

Payment fraud supervision does not define account-level mule supervision.
Account training still needs a separate target with account ID, decision
cutoff, target horizon, mule label, and label availability. A fraudulent
transfer alone does not establish that either endpoint is a mule.

Valid time cannot exclude corrections that were backdated but learned after a
historical seed. The schema intentionally defers known-time fields. Preserve
source snapshots and obtain arrival history before claiming knowledge-safe
backtests on corrected production feeds.
