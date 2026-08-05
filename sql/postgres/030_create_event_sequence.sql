-- =====================================================================
-- THE TEMPORAL CONTRACT
--
-- The schema states it exactly:
--
--   event_seq is unique, deterministic, dense from 1 through the
--   transaction count, strictly positive, and ordered by
--   (event_ts_ms, transaction_id).
--
-- transaction_event_seq materialises that, and everything downstream ---
-- first-seen stamps, label availability ranks, slowly-changing edge
-- validity --- derives from this one table. The CASCADE drops clear every
-- dependent view from previous revisions of this pipeline.
--
-- event_ts_ms is unix_time * 1000, so ordering by (unix_time, id) and by
-- (event_ts_ms, transaction_id) are the same ordering. 001 has already
-- asserted that unix_time is positive and unique-per-id, so the
-- row_number() below is a dense permutation by construction.
--
-- ---------------------------------------------------------------------
-- WHY first_seen_* NEVER LOADS AS ZERO
-- ---------------------------------------------------------------------
-- TigerGraph upserts an unseen edge endpoint with schema defaults. A
-- vertex created that way carries first_seen_seq = 0, and 0 passes every
--     neighbour.first_seen_seq <= seed.event_seq
-- predicate --- INCLUDING cutoffs from before the entity existed. That is
-- a silent, metric-improving failure: no error, no null, a plausible
-- number, and every admission filter in the project quietly opens.
--
-- So every dimension vertex gets a stamp from a real transaction where
-- one exists, and a declared fallback where it does not:
--
--   1. the minimum event_seq of the entity's own transactions;
--   2. failing that, the minimum over the parties that hold it --- an
--      observability lower bound, not an attachment time;
--   3. failing that, the first observed event in the corpus, which reads
--      as "already present when observation began". That is the truthful
--      reading of a registry row that never transacts: it existed, it was
--      simply never used.
--
-- Case 3 is COUNTED by 090 (audit_first_seen_fallback) rather than
-- assumed to be empty. Zero is never emitted, and the GSQL validation
-- fails the load if it ever appears.
-- =====================================================================

DROP VIEW IF EXISTS tf_gnn_prep.transaction_manifest CASCADE;

DROP TABLE IF EXISTS tf_gnn_prep.transaction_event_seq CASCADE;
DROP TABLE IF EXISTS tf_gnn_prep.event_seq_by_time CASCADE;
DROP TABLE IF EXISTS tf_gnn_prep.observation_bounds CASCADE;

DROP TABLE IF EXISTS tf_gnn_prep.card_first_seen CASCADE;
DROP TABLE IF EXISTS tf_gnn_prep.account_first_seen CASCADE;
DROP TABLE IF EXISTS tf_gnn_prep.merchant_first_seen CASCADE;
DROP TABLE IF EXISTS tf_gnn_prep.party_first_seen CASCADE;
DROP TABLE IF EXISTS tf_gnn_prep.device_first_seen CASCADE;
DROP TABLE IF EXISTS tf_gnn_prep.ip_first_seen CASCADE;

-- Retired with Merchant_Category, which the new schema deletes.
DROP TABLE IF EXISTS tf_gnn_prep.category_first_seen CASCADE;


-- ---------------------------------------------------------------
-- event_seq: rank 1..N over (unix_time, id)
-- ---------------------------------------------------------------

CREATE UNLOGGED TABLE
tf_gnn_prep.transaction_event_seq AS
SELECT
    txn.id AS transaction_id,
    txn.unix_time::bigint AS unix_time,

    row_number() OVER (
        ORDER BY
            txn.unix_time::bigint,
            txn.id
    ) AS event_seq

FROM card_fraud."cf_Payment_Transaction" AS txn;


ALTER TABLE tf_gnn_prep.transaction_event_seq
ADD PRIMARY KEY (transaction_id);

CREATE UNIQUE INDEX
    transaction_event_seq_seq_uq
ON tf_gnn_prep.transaction_event_seq (event_seq);

CREATE INDEX
    transaction_event_seq_time_idx
ON tf_gnn_prep.transaction_event_seq (unix_time);


-- ---------------------------------------------------------------
-- Cumulative rank by second. Because event_seq is ordered by
-- (unix_time, id), the highest event_seq at or before epoch X is the
-- count of transactions with unix_time <= X, and the first transaction
-- strictly after X carries that count + 1. This is what turns a
-- label-availability timestamp into label_available_seq without a
-- per-row sort.
-- ---------------------------------------------------------------

CREATE UNLOGGED TABLE
tf_gnn_prep.event_seq_by_time AS
SELECT
    unix_time,
    max(event_seq) AS latest_event_seq

FROM tf_gnn_prep.transaction_event_seq

GROUP BY unix_time;


ALTER TABLE tf_gnn_prep.event_seq_by_time
ADD PRIMARY KEY (unix_time);


-- The rank of the first transaction STRICTLY after an external fact
-- landed. When the fact lands after the last observed transaction the
-- result is max(event_seq) + 1, which no seed can satisfy --- correctly
-- meaning "never consumable inside this dataset".
CREATE OR REPLACE FUNCTION
tf_gnn_prep.event_seq_after(epoch bigint)
RETURNS bigint
LANGUAGE sql
STABLE
PARALLEL SAFE
AS $$
    SELECT coalesce(
        (
            SELECT cumulative.latest_event_seq
            FROM tf_gnn_prep.event_seq_by_time AS cumulative
            WHERE cumulative.unix_time <= epoch
            ORDER BY cumulative.unix_time DESC
            LIMIT 1
        ),
        0
    ) + 1;
$$;


-- ---------------------------------------------------------------
-- Observation window. maximum_epoch bounds label maturation: a clean row
-- whose maturation deadline exceeds it is UNRESOLVED, not a known
-- negative. Right-censoring, handled rather than ignored.
--
-- minimum_event_seq is 1 by construction and is materialised anyway,
-- because it is the fallback stamp for a registry row that never
-- transacts and reading it from here keeps that fallback in one place.
-- ---------------------------------------------------------------

CREATE UNLOGGED TABLE
tf_gnn_prep.observation_bounds AS
SELECT
    min(unix_time) AS minimum_epoch,
    max(unix_time) AS maximum_epoch,
    min(event_seq) AS minimum_event_seq,
    max(event_seq) AS maximum_event_seq,
    count(*) AS transaction_count

FROM tf_gnn_prep.transaction_event_seq;


-- ---------------------------------------------------------------
-- First-seen stamps, in dependency order:
--   card     <- its own transactions
--   account  <- its cards
--   merchant <- its own transactions
--   party    <- its cards and the merchants it operates
--   device   <- the transactions that used it, else its parties
--   ip       <- the transactions that came from it, else its parties
--
-- Email / Phone / Address / Identity_Document have no transaction of
-- their own at all, so they take their parties' minimum; 070 resolves
-- them where the value set is built.
-- ---------------------------------------------------------------

CREATE UNLOGGED TABLE
tf_gnn_prep.card_first_seen AS
SELECT
    edge.card_number,
    min(sequence.unix_time) AS first_seen_unix_time,
    min(sequence.event_seq) AS first_seen_event_seq

FROM card_fraud."cf_Card_Send_Transaction" AS edge

JOIN tf_gnn_prep.transaction_event_seq AS sequence
  ON sequence.transaction_id = edge.txn_id

GROUP BY edge.card_number;


ALTER TABLE tf_gnn_prep.card_first_seen
ADD PRIMARY KEY (card_number);


-- An Account is first seen when the first of its card generations is.
-- Reissues share one account, so this is earlier than any individual
-- card's stamp whenever a replacement exists --- which is the point: the
-- funding relationship predates the plastic.
CREATE UNLOGGED TABLE
tf_gnn_prep.account_first_seen AS
SELECT
    tf_gnn_prep.funding_account_id(card.card_number) AS account_id,
    min(card.first_seen_unix_time) AS first_seen_unix_time,
    min(card.first_seen_event_seq) AS first_seen_event_seq

FROM tf_gnn_prep.card_first_seen AS card

GROUP BY tf_gnn_prep.funding_account_id(card.card_number);


ALTER TABLE tf_gnn_prep.account_first_seen
ADD PRIMARY KEY (account_id);


CREATE UNLOGGED TABLE
tf_gnn_prep.merchant_first_seen AS
SELECT
    edge.merchant_id,
    min(sequence.unix_time) AS first_seen_unix_time,
    min(sequence.event_seq) AS first_seen_event_seq

FROM card_fraud."cf_Merchant_Receive_Transaction" AS edge

JOIN tf_gnn_prep.transaction_event_seq AS sequence
  ON sequence.transaction_id = edge.txn_id

GROUP BY edge.merchant_id;


ALTER TABLE tf_gnn_prep.merchant_first_seen
ADD PRIMARY KEY (merchant_id);


-- A party is first seen at the earliest transaction of any card it holds
-- or any merchant it operates.
CREATE UNLOGGED TABLE
tf_gnn_prep.party_first_seen AS
SELECT
    linked.party_id,
    min(linked.first_seen_unix_time) AS first_seen_unix_time,
    min(linked.first_seen_event_seq) AS first_seen_event_seq

FROM (
    SELECT
        ownership.party_id,
        card.first_seen_unix_time,
        card.first_seen_event_seq

    FROM card_fraud."cf_Party_Has_Card" AS ownership

    JOIN tf_gnn_prep.card_first_seen AS card
      ON card.card_number = ownership.card_number

    UNION ALL

    SELECT
        ownership.party_id,
        merchant.first_seen_unix_time,
        merchant.first_seen_event_seq

    FROM card_fraud."cf_Is_Merchant" AS ownership

    JOIN tf_gnn_prep.merchant_first_seen AS merchant
      ON merchant.merchant_id = ownership.merchant_id
) AS linked

GROUP BY linked.party_id;


ALTER TABLE tf_gnn_prep.party_first_seen
ADD PRIMARY KEY (party_id);


-- Device and IP now have a REAL event-time first sighting, because
-- cf_Transaction_Uses_Device / cf_Transaction_Uses_IP say which endpoint
-- each authorization actually came from. The party fallback below is the
-- old whole-window association and is only reached for an endpoint the
-- institution has on file but never saw transact.
CREATE UNLOGGED TABLE
tf_gnn_prep.device_first_seen AS
SELECT
    observed.device_id,
    min(observed.first_seen_unix_time) AS first_seen_unix_time,
    min(observed.first_seen_event_seq) AS first_seen_event_seq

FROM (
    SELECT
        edge.device_id,
        sequence.unix_time AS first_seen_unix_time,
        sequence.event_seq AS first_seen_event_seq

    FROM card_fraud."cf_Transaction_Uses_Device" AS edge

    JOIN tf_gnn_prep.transaction_event_seq AS sequence
      ON sequence.transaction_id = edge.txn_id

    UNION ALL

    SELECT
        ownership.device_id,
        party.first_seen_unix_time,
        party.first_seen_event_seq

    FROM card_fraud."cf_Has_Device" AS ownership

    JOIN tf_gnn_prep.party_first_seen AS party
      ON party.party_id = ownership.party_id
) AS observed

GROUP BY observed.device_id;


ALTER TABLE tf_gnn_prep.device_first_seen
ADD PRIMARY KEY (device_id);


CREATE UNLOGGED TABLE
tf_gnn_prep.ip_first_seen AS
SELECT
    observed.ip_id,
    min(observed.first_seen_unix_time) AS first_seen_unix_time,
    min(observed.first_seen_event_seq) AS first_seen_event_seq

FROM (
    SELECT
        edge.ip_id,
        sequence.unix_time AS first_seen_unix_time,
        sequence.event_seq AS first_seen_event_seq

    FROM card_fraud."cf_Transaction_Uses_IP" AS edge

    JOIN tf_gnn_prep.transaction_event_seq AS sequence
      ON sequence.transaction_id = edge.txn_id

    UNION ALL

    SELECT
        ownership.ip_id,
        party.first_seen_unix_time,
        party.first_seen_event_seq

    FROM card_fraud."cf_Has_IP" AS ownership

    JOIN tf_gnn_prep.party_first_seen AS party
      ON party.party_id = ownership.party_id
) AS observed

GROUP BY observed.ip_id;


ALTER TABLE tf_gnn_prep.ip_first_seen
ADD PRIMARY KEY (ip_id);


-- ---------------------------------------------------------------
-- The declared fallback, in one place.
--
-- Returns the corpus's first observed event when an entity has no
-- transaction and no transacting party. Never returns 0: see the header.
-- ---------------------------------------------------------------

CREATE OR REPLACE VIEW
tf_gnn_prep.first_seen_floor AS
SELECT
    bounds.minimum_epoch AS first_seen_unix_time,
    bounds.minimum_event_seq AS first_seen_event_seq
FROM tf_gnn_prep.observation_bounds AS bounds;


ANALYZE tf_gnn_prep.transaction_event_seq;
ANALYZE tf_gnn_prep.event_seq_by_time;
ANALYZE tf_gnn_prep.observation_bounds;
ANALYZE tf_gnn_prep.card_first_seen;
ANALYZE tf_gnn_prep.account_first_seen;
ANALYZE tf_gnn_prep.merchant_first_seen;
ANALYZE tf_gnn_prep.party_first_seen;
ANALYZE tf_gnn_prep.device_first_seen;
ANALYZE tf_gnn_prep.ip_first_seen;
