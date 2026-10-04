# Temporal mule detection loader

This additional use case loads PhantomLedger's `mule-temporal` corpus into
`Mule_Pattern_Learner`, the TigerGraph 4.2.5 graph of MulePatternLearner (MPL).
The original card-fraud pipeline remains the default. To replace a loaded
corpus with a regenerated one, follow [A fresh push](#a-fresh-push).

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

The [fresh-graph DDL](../gsql/mule_temporal/schema.gsql) is MPL's
`gsql/schema/schema.gsql`, copied byte for byte (its comments are MPL's):
eight vertex types, seven discriminated associations, and twelve point-event
relationships, each with its named reverse, including the attributes only
MPL's own queries write (the Account label fields its label reveal sets and
the payment pair caches its pair queries fill). The test
`test_schema_is_mpl_fresh_graph_ddl` fails when the copy differs from MPL's
file; it reads the MulePatternLearner checkout next to this repository, or
the one `MULE_PATTERN_LEARNER_DIR` names, and is skipped when there is none.
After changing MPL's schema, copy the file again.

The push does not create the graph: the REST++ secret is minted per graph, so
the schema is run once by hand (`schema-path`). Before installing jobs or
uploading, the push checks the live schema against the contract: attribute
types and order, endpoints, reverse edges, and discriminator flags. A graph
created from the DDL passes (`test_graph_from_the_ddl_passes_the_push_schema_check`).
The only other types it accepts are MPL's experiment scope,
`Temporal_Training_Scope` and `Entity_In_Training_Scope` with its reverse,
which MPL's `mule install` adds to a loaded graph; the loader neither loads nor
checks them. It never automatically drops or migrates types.

The [historical migration](../gsql/mule_temporal/migrations/temporal_valid_time.gsql)
is retained only for existing graphs, and only for the exact prior empty
six-vertex schema; it is not a general migration and must not be run against
populated data. No migration applies to the fifteen-column Account table: the
Account vertex already stores the fifteen attributes, so only the source
contract and the loading job changed.

### The Account table

`mt_Account` carries the fifteen columns of MPL's Account label contract (its
`docs/reference/labels.md`, "Loading accounts"), in this order, with the types
of the graph:

| Column | Type | Column | Type |
|---|---|---|---|
| `id` | STRING | `pu_label` | INT |
| `account_type` | STRING | `mule_label_effective_seq` | UINT |
| `is_external` | BOOL | `mule_label_effective_ts_ms` | UINT |
| `first_seen_seq` | UINT | `mule_label_available_seq` | UINT |
| `first_seen_ts_ms` | UINT | `mule_label_available_ts_ms` | UINT |
| `is_mule` | INT | `mule_ring_id` | INT |
| `mule_label_known` | BOOL | `mule_label_source` | STRING |
| `is_mule_masked` | BOOL | | |

The table keeps `is_mule` sixth, in the contract's load order; MPL's schema
stores it last. `mt_load_account` therefore reads
`$0` to `$4`, `$6` to `$14`, then `$5`, the mapping of MPL's `load_accounts`;
a test compares the two. Every column is loaded and none keeps a schema
default. The flags load the way `is_external` always has: the audit accepts
`true` and `false` in any case (PhantomLedger renders `True` and `False`), and
the export writes them lowercase. `mule_label_source` is the one column that
may be NULL (PhantomLedger's CSV `COPY` stores an external account's empty
source as NULL); it loads as the empty string, the attribute's default. Every
other NULL fails the audit.

The label columns are supervision, copied exactly; the loader infers,
reveals or repairs none of them. PhantomLedger marks no label known and masks
every account, so MPL's label reveal runs on the loaded graph as on any fresh
PhantomLedger load; the reveal never writes `mule_ring_id`, so the rings stay.
The audit rejects a ring id below -1 and records the ring distribution, and
`verify` requires the graph's to equal it, so a load that left the rings at
their default fails.

### Every table

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
label availability, Account mule flags and ring ids, sender/recipient
presence, exclusive token bindings, and
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

The manifest has format version 5, the fifteen-column Account contract.
`load` and `push` refuse an export directory of another version (the
six-column contract wrote 4) and ask for a new, empty `MULE_EXPORT_DIR`.

`install-jobs` installs the [27 loading jobs](../gsql/mule_temporal/loading_jobs.gsql)
and the read-only `mt_validate_graph` query. `load` checks all shard hashes
before upload and rejects a fresh load into a graph with any vertex, of any
type, naming the types that still have some. `verify`
compares all eight vertex counts and all 38 forward/reverse edge counts with
the manifest, using traversals rather than delayed degree statistics. The
Account `is_mule` and `mule_ring_id` distributions must also match PostgreSQL
exactly. It also checks graph clocks, first observations, interval validity,
Zelle label fields, Account flag and ring ranges, and participation clock
parity. Results are written to `tigergraph_verification.json`.
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
# The schema identity tests read MPL's checkout, by default ../MulePatternLearner:
MULE_PATTERN_LEARNER_DIR=/path/to/MulePatternLearner python -m unittest discover -s tests
# Optional real PostgreSQL fixtures; only session-local objects are created:
MULE_TEST_DSN='dbname=phantomledger' python -m unittest discover -s tests
python scripts/check_loader_contracts.py
```

## A fresh push

To replace the loaded corpus with a regenerated one, in this order:

1. **Regenerate the corpus** into PostgreSQL with PhantomLedger
   (`--usecase mule-temporal`, PhantomLedger commit `afbb83a` or later, whose
   `mt_Account` has the fifteen columns). Point `MULE_PG_DSN` (or `PG_DSN`) at
   that database and check its contract without exporting:

   ```sh
   tf-gnn-load --use-case mule-temporal inspect
   ```

   It stops with `Source contract mismatch` and names the columns it found
   when a table differs from the contract.

2. **Clear the graph's data and keep its schema.** Delete every vertex of every
   type in `Mule_Pattern_Learner`, MPL's `Temporal_Training_Scope` included;
   deleting a vertex deletes its edges. The schema, the loading jobs and MPL's
   installed queries stay. The load refuses a fresh load while any vertex type
   has vertices, and names those types. One way, with this repository's client
   and `.env` (not run here; on the full corpus a single request may outlast
   the gateway, so read the counts afterwards and repeat until every one is 0):

   ```sh
   python - <<'EOF'
   from tf_gnn_loader.tigergraph.client import Client
   from tf_gnn_loader.tigergraph.settings import Settings

   settings = Settings()
   assert settings.graphname == "Mule_Pattern_Learner", settings.graphname
   conn = Client(settings).conn
   for vertex_type in conn.getVertexTypes():
       print(vertex_type, conn.delVertices(vertex_type))
   print(conn.getVertexCount("*", realtime=True))
   EOF
   ```

   Do not use `CLEAR GRAPH STORE` while another graph on the instance, such
   as `TransactionFraud_GNN`, must keep its data: it empties every graph. If
   `Mule_Pattern_Learner` does not exist, create it instead: run the file
   `tf-gnn-load --use-case mule-temporal schema-path` prints once in the
   Savanna GSQL editor, then mint the graph's REST++ secret and put it in
   `.env`, here and in MPL.

3. **Choose a new, empty export directory**, for example
   `MULE_EXPORT_DIR=artifacts/mule_temporal_<date>`. The push resumes any export
   it finds in its directory; one written before the fifteen-column contract
   is refused.

4. **Push:**

   ```sh
   tf-gnn-load --use-case mule-temporal push
   ```

   It checks the schema, audits and exports the 27 tables in one snapshot,
   drops and reinstalls the 27 `mt_load_*` jobs (so `mt_load_account` loads all
   fifteen columns) and `mt_validate_graph`, uploads, and verifies. It ends
   with `"pushed": true`, and `tigergraph_verification.json` in the export
   directory has `"passed": true`. If it stops part way, run the same command
   with the same `MULE_EXPORT_DIR`: completed shards are skipped. Do not clear
   the graph again.

5. **Install MPL's queries and train**, in the MulePatternLearner repository:

   ```sh
   mule install
   mule train
   ```

   `mule install` adds the `Temporal_Training_Scope` vertex type when it is
   missing and installs the stale queries. MPL's "Set up a graph" guide,
   section "After the data changes", says a dataset and scope prepared before
   the reload describe data that is gone: give the built-in run a new
   `scope.id` there, so `mule train` creates a scope on the new data, reveals
   the labels and prepares a new dataset. Straight after the load, before the
   reveal, MPL's `validate_label_contract` counts one `invalid_unknown` per
   mule, because PhantomLedger marks no label known; after the reveal every
   count is zero.

## Supervision and source limitations

The 2024 research corpus models Zelle assignment and registration synthetically.
Its Zelle labels use an explicit synthetic oracle: availability is the next
sequence after the payment, possibly at the same millisecond. Unknown labels
must remain `-1`, `false`, `0`, `0`. No label field is a feature, and this
loader never copies risk scores, fraud channel names, train/test splits, or
persisted time encodings from PostgreSQL into the graph.

The source supplies the whole Account label contract (see
[The Account table](#the-account-table)): the synthetic role flag `is_mule`,
the mask, the PU label, the effective and availability clocks, the ring and
the source. The loader copies them exactly as supervision and never
substitutes first-observation time for label discovery. MPL's label reveal,
not this loader, decides which labels training sees; `mt_validate_graph`
validates raw ingestion and the flag and ring parity, not MPL's label contract.

Payment fraud supervision does not define account-level mule supervision.
Account training still needs a separate target with account ID, decision
cutoff, target horizon, mule label, and label availability. A fraudulent
transfer alone does not establish that either endpoint is a mule.

Valid time cannot exclude corrections that were backdated but learned after a
historical seed. The schema intentionally defers known-time fields. Preserve
source snapshots and obtain arrival history before claiming knowledge-safe
backtests on corrected production feeds.
