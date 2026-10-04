# PhantomLedger PostgreSQL to TigerGraph

Two use cases:

- **Card fraud** (default): `card_fraud` into `TransactionFraud_GNN`.
- **[Temporal mule detection](docs/mule_temporal.md)**: the 27
  `mule_temporal.mt_*` tables into MulePatternLearner's `Mule_Pattern_Learner`,
  with its schema copied byte for byte. All fifteen
  [Account columns](docs/mule_temporal.md#the-account-table) load, ring ids
  included. To replace a loaded corpus, follow
  [A fresh push](docs/mule_temporal.md#a-fresh-push).

```bash
tf-gnn-load --use-case mule-temporal push
```

The mule push reads `MULE_PG_DSN`, checks the contract, exports immutable
shards to `MULE_EXPORT_DIR`, installs loading jobs, uploads resumably, and
verifies edge counts (both directions) and clocks.

## Card-fraud use case

Schema: [gsql/schema/schema.gsql](gsql/schema/schema.gsql). The supervised
event vertex is `Payment_Transaction`; the clock is `event_seq`.

- **The target is one authorization, scored when the request arrives**, so
  its response (`error` decline code, approval status) is not loaded. Features
  over *prior* rows' responses are fine.
- **No split**: no `split_id`, `causal_fold` or calendar. Cut on `event_seq`
  when training.

## What transfers

Source: PhantomLedger's `card_fraud` export, 43 tables
(`include/phantomledger/exporter/card_fraud/schema.hpp`), enforced by
[001_validate_sources.sql](sql/postgres/001_validate_sources.sql) and explained
in [contract.py](src/tf_gnn_loader/postgres/contract.py).

| Graph type | Source |
|---|---|
| `Party` | `cf_Party` (id, type, `created_at` only) |
| `Account` | derived from card ids, see below |
| `Card`, `Payment_Transaction` | `cf_Card`, `cf_Payment_Transaction` |
| `Merchant` | `cf_Merchant` + `cf_Merchant_Assigned` |
| `Merchant_Location` | `cf_Merchant_Location` + `cf_Has_City` / `_State` / `_Zip` |
| `Device`, `IP_Address` | `cf_Device`, `cf_IP`, tokenized |
| `Email`, `Phone`, `Address`, `Identity_Document` | `cf_Email`, `cf_Phone`, `cf_Address`, `cf_ID`, tokenized |
| `Transaction_Used_Device`, `Transaction_From_IP` | `cf_Transaction_Uses_Device`, `cf_Transaction_Uses_IP` |

24 PSV datasets, 12 vertex types, 16 forward edge types.

### `Account` is derived from card ids

A card id is `[C|D] <funding key> [-G<generation>]` (`D` deposit account, `C`
credit card, `-G` reissue generation). `account_id` drops the generation
([020_create_policies.sql](sql/postgres/020_create_policies.sql)).

- `Account.account_type` and `Card.card_type` come from the `C`/`D` prefix.
- `Account_Has_Card` is the only relation with a real tenure: generation *n*
  holds until *n+1* is first seen.
- Each authorization has one Account, the funding side; the merchant receives.
  Account-to-account transfers would need source and destination relations; a
  split tender should arrive as separate authorizations.

### PII is tokenized in PostgreSQL

- Email, phone, address, identity-document, device and IP ids become
  `HMAC-SHA256(key = sha256(salt), message = kind || ':' || value)`, cut to 128
  bits and prefixed `eml_`, `phn_`, `adr_`, `doc_`, `dev_` or `ip_`. The kind
  is hashed too, so one string as two kinds gives two unrelated tokens.
- `TFGNN_PII_SALT` is required (16 characters or more) and never written into
  SQL. **Keep it secret and never change it:** a new salt re-keys every token
  vertex and orphans every loaded edge. A mismatch with the stored digest is
  refused.
- The export fails if any `load_*` view has a raw-PII column.
- `Email.domain` and `Identity_Document.document_type` are kept.

### Not loaded

| Not loaded | Why |
|---|---|
| `cf_Party.name` / `.gender` / `.dob`, `cf_Full_Name`, `cf_DOB` | Protected attributes |
| `cf_Payment_Transaction.error` | An authorization response |
| `cf_IP.is_blocked`, `cf_Device.is_blocked`, `cf_Ground_Truth_Label` | Label leaks |
| `cf_Email_Minhash`, `cf_Has_Email_Minhash` | Entity resolution is out of scope |
| `cf_Merchant_Category`, `cf_City`, `cf_State`, `cf_Zipcode` as vertices | Read only for attributes (category, `region_code`, `postal_code_prefix`, coordinates) |

### Defaults where the source is silent

- Country codes are empty, so `is_cross_border` is always false: unknowable,
  not domestic.
- `currency` is empty, not `"USD"`.
- `device_risk_score` and `ip_risk_score` are `-1` (unavailable), never `0`.
- `Account.opened_ts_ms` and `Card.issued_ts_ms` are `0`: first use is not an
  open date.
- `use_chip`: `Online` gives `channel=ecommerce`, `card_present=false`;
  `Chip`/`Swipe` give `channel=pos`, `entry_mode` chip or magstripe.

## Two silent failures

- **A zero stamp opens every filter.** TigerGraph creates a missing endpoint
  with `first_seen_seq = 0`, which passes every cutoff. So no vertex loads with
  a zero stamp (checked in prep, audit and `tfgnn_validate_graph`), and
  datasets load in order: the 11 entity vertex types, relations, the fact
  table, optional endpoints.
- **A lower-bound stamp is not an attachment time.** `valid_from_seq` on
  ownership and PII relations is the later endpoint's `first_seen_seq`.
  **Never build "*x* at onset" features on them**: they return the final
  count. `Account_Has_Card` is the exception.

## Running

`.env` (see [.env.example](.env.example)):

```
HOST=<savanna host>
GRAPHNAME=TransactionFraud_GNN
SECRET=<REST++ secret, minted per graph in Savanna>
PG_DSN=dbname=phantomledger
EXPORT_DIR=./artifacts/tfgnn_load
SHARD_BYTES=90000000
TFGNN_PII_SALT=<at least 16 characters, kept with your other secrets>
```

1. Run the schema once in the Savanna GSQL editor; the loader cannot create a
   graph. `tf-gnn-load schema-path` prints the file.
2. Check the schema, jobs, load views and verifier agree, offline:
   `python scripts/check_loader_contracts.py`
3. `tf-gnn-load push`, or the steps `inspect`, `prepare`, `audit`, `export`,
   `install-jobs`, `load`, `verify`, or `scripts/run_pipeline.sh full`.

`verify` installs and runs `tfgnn_validate_graph`, which must pass before
exporting or training.
