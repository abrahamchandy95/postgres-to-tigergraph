# PhantomLedger PostgreSQL → TigerGraph `TransactionFraud_GNN`

Loads the PhantomLedger card-fraud corpus into the `TransactionFraud_GNN`
graph: the schema in [gsql/schema/schema.gsql](gsql/schema/schema.gsql),
whose supervised event vertex is `Payment_Transaction` and whose clock is
`event_seq`.

**The target is one payment authorization, scored at the moment the
authorization request arrives.** That is not a framing preference; it is
what the schema is built around, and it is why the authorization
*response* is absent from it. There is no `error` decline code on the
fact vertex and no approval status, because neither exists yet at the
moment being scored. A decline-count-in-the-last-hour feature is still
perfectly legal — it reads *prior* rows' responses at query time — but the
row's own response is not an input to its own decision.

The benchmark framing this repo used to carry is gone with it. There is no
train/validation/test calendar, no `split_id` and no `causal_fold`: the
schema declares no split attribute at all, so separation is a cutoff on
`event_seq` applied by whoever is training. A stored split column would be
a calendar proxy resident on the fact vertex.

## What transfers

The source is PhantomLedger's `card_fraud` export, 43 tables
(`include/phantomledger/exporter/card_fraud/schema.hpp`). The enforced
contract is [sql/postgres/001_validate_sources.sql](sql/postgres/001_validate_sources.sql);
[src/tf_gnn_loader/postgres/contract.py](src/tf_gnn_loader/postgres/contract.py)
mirrors it with the reasoning.

| Graph type | Source |
|---|---|
| `Party` | `cf_Party` — id, type and `created_at` only |
| `Account` | **derived**, see below |
| `Card` | `cf_Card` |
| `Merchant` | `cf_Merchant` + `cf_Merchant_Assigned` |
| `Merchant_Location` | `cf_Merchant_Location` + `cf_Has_City` / `_State` / `_Zip` |
| `Device`, `IP_Address` | `cf_Device`, `cf_IP`, tokenized |
| `Email`, `Phone`, `Address`, `Identity_Document` | `cf_Email`, `cf_Phone`, `cf_Address`, `cf_ID`, tokenized |
| `Payment_Transaction` | `cf_Payment_Transaction` |
| `Transaction_Used_Device` | `cf_Transaction_Uses_Device` |
| `Transaction_From_IP` | `cf_Transaction_Uses_IP` |

Twenty-four PSV datasets, 12 vertex types, 16 forward edge types.

### `Account` is derived, and from the source's own identifiers

The corpus exports no account table, but PhantomLedger's card ids encode
one. `derive.hpp` renders a card as

    [C|D] <rendered funding key> [ -G<generation> ]

where a `D` card renders the **deposit-account** key, a `C` card renders
the **credit-card** key (“cards and accounts are distinct id spaces”), and
the `-G` suffix is the reissue generation — a cardholder receives
replacement plastic over a three-year window.

So `account_id` is the card id with the generation suffix stripped:
[`tf_gnn_prep.funding_account_id`](sql/postgres/020_create_policies.sql).
That recovers the funding instrument the authorization actually drew on,
rather than inventing one, and it buys two things a
one-account-per-customer shortcut would not:

- `Account_Has_Card` becomes a real one-to-many over reissue generations
  instead of a synonym for “all this customer's cards”;
- it becomes the **one relation in this schema with a genuine tenure**.
  Generation *n* holds until generation *n+1* is first seen, so “which
  card was in force at this authorization” is answerable.

`Account.account_type` (credit/debit) and `Card.card_type` come from the
same `C`/`D` prefix.

**One Account per authorization means one *initiating/funding* Account.**
The merchant is the receiving side and is the mandatory `Merchant`
endpoint; there is no second Account. That invariant holds for merchant
payments, which is what this corpus is. Account-to-account transfers,
bank transfers or genuine split funding would need role-specific source
and destination account relations — and a split tender should arrive as
separate authorization records unless the source explicitly models it as
one transaction.

### PII is tokenized before it leaves PostgreSQL

The schema requires it: *“Email, phone, address, identity-document,
device and IP primary IDs must be salted/tokenized before loading. Raw
PII is not stored.”*

Every one of those ids is `HMAC-SHA256(key = sha256(salt), message =
kind || ':' || value)`, truncated to 128 bits and prefixed by type
(`eml_`, `phn_`, `adr_`, `doc_`, `dev_`, `ip_`). The `kind` is inside the
HMAC, so the same string held as a device id and as a document number
produces two unrelated tokens.

`TFGNN_PII_SALT` supplies the salt. It is required, it must be at least
16 characters, and it reaches PostgreSQL as a session GUC bound as a
parameter — never as a literal in a SQL file. **Keep it with your other
secrets: changing it changes every `Device`, `IP_Address`, `Email`,
`Phone`, `Address` and `Identity_Document` primary id, which silently
re-keys the graph and orphans every edge already loaded.** The prep
schema records the salt's digest and refuses a mismatch.

The enforcement is structural rather than editorial:
`tf_gnn_prep.audit_forbidden_columns` reads the PostgreSQL **catalogue**
and fails the export if any `load_*` view carries a raw-PII column name —
so a view that started selecting `cf_Email.email` fails even if every row
in it happened to look tokenized.

Two attributes survive tokenization because they are not personal:
`Email.domain` (the mail provider — “this group all signs up at the same
throwaway provider” is exactly what shared-value topology is for) and
`Identity_Document.document_type` (passport vs licence, not which one).

### What is deliberately not loaded

| Not loaded | Reason |
|---|---|
| `cf_Party.name` / `.gender` / `.dob`, `cf_Full_Name`, `cf_DOB` | Protected attributes. The schema declares no `Full_Name` or `Birthdate` vertex, and 001 does not even require the columns — a column nothing validates is a column nothing can accidentally load. |
| `cf_Payment_Transaction.error` | An authorization *response*. The scoring moment is the request. |
| `cf_IP.is_blocked`, `cf_Device.is_blocked` | A blocklist entry is written *after* fraud is confirmed, so a timeless flag is a function of the label and will look like your best feature. |
| `cf_Ground_Truth_Label` | Whole-window entity labels (“this card ever carried a flag-1 row”). Loading it reintroduces exactly the leak the table exists to quarantine. |
| `cf_Email_Minhash`, `cf_Has_Email_Minhash` | Entity-resolution blocking keys. The schema puts ER outside its scope explicitly, “as an optional, separately versioned module”. |
| `cf_Merchant_Category`, `cf_City`, `cf_State`, `cf_Zipcode` as vertices | Deleted types. The category is an attribute on `Merchant` and on `Payment_Transaction`; geography is `region_code` / `postal_code_prefix` / coordinates. The three tables are still *read* to supply those attributes. |

### Where the source is silent, the schema's own default loads

Not every schema attribute has a source, and the ones that do not take the
declared “not loaded” value rather than a plausible-looking guess:

- **country codes are empty everywhere.** `cf_State` is a bare subdivision
  code and PhantomLedger's catalogue mixes US states with foreign
  subdivisions (`LND`, `ON`, `CMX`, …) that carry no country. Guessing
  “US if it looks like a US state code” would turn `home_country` into a
  US-residency flag with a respectable name. The consequence is recorded
  rather than hidden: `is_cross_border` is false on every row because it
  is *unknowable*, not because every authorization is domestic.
- **`currency` is empty**, not `"USD"`. A constant column teaches a model
  nothing either way, and a wrong constant is worse than an honest blank.
- **`device_risk_score` and `ip_risk_score` are `-1`**, the schema's
  explicit unavailable sentinel — never `0`, which is a real value meaning
  “the vendor says this is clean”.
- **`Account.opened_ts_ms` and `Card.issued_ts_ms` are `0`.** A card's
  first transaction is emphatically not its open date: “issued, then sat
  unused” is the bust-out shape, and stamping first-use as issuance would
  hand a bust-out detector a covariate that is really its own label.

`use_chip` *is* causal in the source, so it does carry information:
`Online` → `channel=ecommerce`, `card_present=false`; `Chip`/`Swipe` →
`channel=pos` with `entry_mode` chip or magstripe.

## Two things that are silent when they go wrong

Most of the audit machinery exists for these two.

**A zero stamp opens every filter.** TigerGraph upserts a missing edge
endpoint with schema defaults rather than failing, and a vertex created
that way carries `first_seen_seq = 0` — which passes every
`first_seen_seq <= cutoff` predicate *including cutoffs from before the
entity existed*. No error, no null, a plausible number, and every
admission filter quietly opens. So no vertex ever loads with a zero stamp
(030 guarantees it, 090 and `tfgnn_validate_graph` both gate it), and the
dataset numbering is a load order: all 11 vertex types, then the
relations, then the fact table, then the optional endpoints. By the time
the fact table loads it upserts nothing.

**A lower-bound stamp is not an attachment time.** The source carries no
per-attachment observation time for ownership or PII, so
`valid_from_seq` on those relations is `max(first_seen_seq of the two
endpoints)` — the tightest defensible lower bound. **Do not build an
“*x* at onset” feature on them.** Under a lower-bound stamp, “how many
cards did this holder have when this one was attached” collapses to the
holder's *final* card count: a whole-window leak wearing a point-in-time
name, and the join succeeds and returns a plausible number.
`Account_Has_Card` is the exception and the only real tenure here.

## Running

Set in `.env` (see [.env.example](.env.example)):

```
HOST=<savanna host>
GRAPHNAME=TransactionFraud_GNN
SECRET=<REST++ secret, minted per graph in Savanna>
PG_DSN=dbname=phantomledger
EXPORT_DIR=./artifacts/tfgnn_load
SHARD_BYTES=90000000
TFGNN_PII_SALT=<at least 16 characters, kept with your other secrets>
```

Create the schema once in the Savanna GSQL editor — the loader cannot do
it, because the REST++ secret is minted per graph and a graph that does
not exist yet cannot be reached with these credentials:

```bash
tf-gnn-load schema-path
```

Check the static contracts before touching a server. This compares the
schema, the loading jobs, the load views and the Python verifier against
each other, and catches the class of failure that otherwise surfaces
minutes into a run with an error that does not name the file:

```bash
python scripts/check_loader_contracts.py
```

Then:

```bash
tf-gnn-load push
```

or step by step — `inspect`, `prepare`, `audit`, `export`, `install-jobs`,
`load`, `verify` — equivalently `scripts/run_pipeline.sh full`.

After the load, `tfgnn_validate_graph` is installed and run by
`tf-gnn-load verify`. The schema header names it as the thing to run
before exporting or training.
