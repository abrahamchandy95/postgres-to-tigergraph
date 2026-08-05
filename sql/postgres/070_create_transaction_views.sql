-- =====================================================================
-- THE TRANSACTION MANIFEST
--
-- One row per source transaction --- every source transaction. There is
-- no split filter and no split column: the new schema declares neither,
-- and separation between fitting and scoring windows is a cutoff on
-- event_seq applied by the consumer.
--
-- This view is the whole authorization-time picture, and the endpoint
-- keys travel with it. 080 turns one row into a Payment_Transaction
-- vertex plus four edges --- Transaction_From_Account,
-- Transaction_Used_Card, Transaction_At_Merchant and
-- Transaction_At_Location --- in a single loading-job line, which is what
-- makes "exactly one Account and Merchant, at most one Card and
-- Location" true by construction rather than by assertion.
--
-- ---------------------------------------------------------------------
-- THE SCORING MOMENT IS THE AUTHORIZATION REQUEST
-- ---------------------------------------------------------------------
-- The schema is explicit that "current authorization response/status is
-- intentionally absent: the scoring moment is the authorization request,
-- before that response exists". Two consequences here, and both are
-- deletions relative to the TF_GNN loader:
--
--   cf_Payment_Transaction.error IS NOT LOADED. It is a decline code ---
--   a property of the response, produced after the decision this model
--   is making. The old schema carried it as a feature; the new one has
--   nowhere to put it, and that is correct rather than an oversight. A
--   decline-count-in-the-last-hour feature is still perfectly legal, but
--   it is built from PRIOR rows' responses at query time, not from this
--   row's own.
--
--   is_online is not loaded as a transaction attribute either. channel,
--   entry_mode and card_present carry the acceptance environment, which
--   is what it meant.
--
-- ---------------------------------------------------------------------
-- WHAT use_chip BECOMES
-- ---------------------------------------------------------------------
-- use_chip is the source's only acceptance-environment signal and it is
-- CAUSAL, not a hash (PhantomLedger use-chip-causal-2026-07): Online iff
-- the destination is a geography-free acceptance endpoint, Chip or Swipe
-- on physical outlets by the dated US EMV terminal mix. So it splits
-- cleanly into the three schema attributes:
--
--   use_chip              channel      entry_mode    card_present
--   Online Transaction    ecommerce    ecommerce     false
--   Chip Transaction      pos          chip          true
--   Swipe Transaction     pos          magstripe     true
--   anything else         unknown      unknown       false
--
-- The unknown branch is not defensive decoration --- 090 counts it, so a
-- source that grows a fourth value shows up as a number rather than as a
-- silently miscoded channel.
--
-- ---------------------------------------------------------------------
-- ATTRIBUTES WITH NO SOURCE
-- ---------------------------------------------------------------------
--   currency          ""    the source models no currency. NOT "USD":
--                           asserting a currency the corpus never states
--                           is worse than the schema's own "not loaded"
--                           default, and a constant column teaches a
--                           model nothing either way.
--   is_cross_border   false unknowable without countries (040).
--   is_recurring      false no source. PhantomLedger models per-pair
--                           repeat rates but exports no subscription
--                           flag, and deriving one from repeat behaviour
--                           would be a feature computed from the whole
--                           window and stamped on a single event.
--   device_risk_score -1    explicit unavailable sentinel, per the
--   ip_risk_score     -1    schema. NOT 0: a zero risk score is a real
--                           value meaning "vendor says this is clean".
--   transaction_type  "purchase"  true of every row: the card view is
--                           credit-card purchases and account-paid POS
--                           purchases, and nothing else.
--
-- ---------------------------------------------------------------------
-- EVENT LOCATION
-- ---------------------------------------------------------------------
-- has_event_location / event_latitude / event_longitude are populated
-- ONLY for card-present rows at a merchant location with coordinates.
-- For a card-present authorization the terminal IS the merchant's
-- outlet, so this is the same fact Transaction_At_Location reaches,
-- denormalised onto the event so an impossible-travel velocity is one
-- read instead of a traversal.
--
-- It is NOT a cardholder position, and must never become one. For a
-- card-not-present row the cardholder's location is unmodelled, and in
-- generated data any such coordinate would be drawn conditional on the
-- fraud flag --- label leakage through a covariate. Online rows carry
-- has_event_location = false and 0,0, and 0,0 is a real place, so the
-- flag is the only valid test.
--
-- ---------------------------------------------------------------------
-- LABEL MATURITY --- WHERE VICTIM-REPORTED SCAM FRAUD FOLDS IN
-- ---------------------------------------------------------------------
--   confirmed fraud   fraud_label = 1. Available at the event plus the
--                     policy confirmation delay. Covers chargebacks AND
--                     coached "bail fee" payments: both are verdicts
--                     that land after the event, and neither may enter a
--                     feature before its availability rank.
--   matured clean     fraud_label = 0 and the maturation deadline falls
--                     inside the observed window. Available at the
--                     deadline.
--   unresolved        fraud_label = 0 but the deadline exceeds the
--                     observed window. label_known is FALSE: treating it
--                     as a clean negative is right-censoring.
--
-- label_known is the mask, and it is what makes the difference visible.
-- fraud_label stays 0/1 on every row because the source states it; what
-- an unresolved row lacks is not the value but the right to use it.
-- =====================================================================

DROP VIEW IF EXISTS tf_gnn_prep.transaction_manifest CASCADE;


CREATE VIEW
tf_gnn_prep.transaction_manifest AS
SELECT
    txn.id AS transaction_id,

    sequence.unix_time,
    sequence.event_seq,

    txn.amount::double precision AS amount,

    -- amount_present separates a genuine zero authorization (a card
    -- verification, a $0.00 pre-auth) from an amount that never loaded.
    -- The schema declares amount DOUBLE DEFAULT 0, so without this flag
    -- the two are the same value.
    (txn.amount IS NOT NULL) AS amount_present,

    '' AS currency,
    'purchase' AS transaction_type,

    CASE
        WHEN txn.use_chip = 'Online Transaction' THEN 'ecommerce'
        WHEN txn.use_chip = 'Chip Transaction' THEN 'pos'
        WHEN txn.use_chip = 'Swipe Transaction' THEN 'pos'
        ELSE 'unknown'
    END AS channel,

    CASE
        WHEN txn.use_chip = 'Online Transaction' THEN 'ecommerce'
        WHEN txn.use_chip = 'Chip Transaction' THEN 'chip'
        WHEN txn.use_chip = 'Swipe Transaction' THEN 'magstripe'
        ELSE 'unknown'
    END AS entry_mode,

    (txn.use_chip IN ('Chip Transaction', 'Swipe Transaction'))
        AS card_present,

    false AS is_cross_border,
    false AS is_recurring,

    tf_gnn_prep.clean_text(txn.mer_cat) AS merchant_category_code,

    -- Event location: card-present at a located outlet only.
    (
        txn.use_chip IN ('Chip Transaction', 'Swipe Transaction')
        AND location.has_coordinates
    ) AS has_event_location,

    CASE
        WHEN txn.use_chip IN ('Chip Transaction', 'Swipe Transaction')
         AND location.has_coordinates
        THEN location.latitude
        ELSE 0
    END AS event_latitude,

    CASE
        WHEN txn.use_chip IN ('Chip Transaction', 'Swipe Transaction')
         AND location.has_coordinates
        THEN location.longitude
        ELSE 0
    END AS event_longitude,

    -1::double precision AS device_risk_score,
    -1::double precision AS ip_risk_score,

    txn.is_fraud::integer AS fraud_label,

    (
        txn.is_fraud::integer = 1
        OR sequence.unix_time + label.maturation_window_seconds
           <= bounds.maximum_epoch
    ) AS label_known,

    CASE
        WHEN txn.is_fraud::integer = 1
        THEN sequence.unix_time + label.fraud_confirmation_delay_seconds

        WHEN sequence.unix_time + label.maturation_window_seconds
             <= bounds.maximum_epoch
        THEN sequence.unix_time + label.maturation_window_seconds

        ELSE 0
    END AS label_available_unix_time,

    CASE
        WHEN txn.is_fraud::integer = 1
        THEN tf_gnn_prep.event_seq_after(
            sequence.unix_time + label.fraud_confirmation_delay_seconds
        )

        WHEN sequence.unix_time + label.maturation_window_seconds
             <= bounds.maximum_epoch
        THEN tf_gnn_prep.event_seq_after(
            sequence.unix_time + label.maturation_window_seconds
        )

        ELSE 0
    END AS label_available_seq,

    CASE
        WHEN txn.is_fraud::integer = 1
          OR sequence.unix_time + label.maturation_window_seconds
             <= bounds.maximum_epoch
        THEN label.policy_id
        ELSE ''
    END AS label_source,

    -- Endpoint keys. Account and Card come from the SAME source edge, so
    -- exactly-one-Account and at-most-one-Card are the same fact; 001
    -- already asserted one cf_Card_Send_Transaction row per transaction.
    card_edge.card_number AS card_id,

    tf_gnn_prep.funding_account_id(card_edge.card_number) AS account_id,

    merchant_edge.merchant_id,

    -- NULL for an online merchant, which has no Merchant_Location at all
    -- (040). 080 emits an empty column there and the loading job's
    -- Transaction_At_Location clause skips the edge.
    location.location_id

FROM card_fraud."cf_Payment_Transaction" AS txn

JOIN tf_gnn_prep.transaction_event_seq AS sequence
  ON sequence.transaction_id = txn.id

JOIN card_fraud."cf_Card_Send_Transaction" AS card_edge
  ON card_edge.txn_id = txn.id

JOIN card_fraud."cf_Merchant_Receive_Transaction" AS merchant_edge
  ON merchant_edge.txn_id = txn.id

LEFT JOIN tf_gnn_prep.merchant_locations AS location
  ON location.merchant_id = merchant_edge.merchant_id

CROSS JOIN tf_gnn_prep.label_policy AS label

CROSS JOIN tf_gnn_prep.observation_bounds AS bounds

WHERE label.policy_id = 'phantomledger_synthetic_v1';


-- ---------------------------------------------------------------
-- EVENT-TIME ENDPOINTS
--
-- The two relations the TF_GNN loader could not build. cf_Has_Device and
-- cf_Has_IP are whole-window associations the institution has ON FILE;
-- these two tables say which endpoint the AUTHORIZATION actually came
-- from, which is what Transaction_Used_Device and Transaction_From_IP
-- are.
--
-- Both carry the transaction's own event_seq and event_ts_ms, per the
-- schema's "every transaction-participation edge repeats event_seq and
-- event_ts_ms". They are taken from the transaction's sequence row, NOT
-- from the source's edge_unix_time column: a participation edge that
-- disagreed with its own transaction's clock would break every
-- cutoff-safe query, and there is exactly one clock.
-- ---------------------------------------------------------------

DROP VIEW IF EXISTS tf_gnn_prep.loaded_transaction_used_device CASCADE;
DROP VIEW IF EXISTS tf_gnn_prep.loaded_transaction_from_ip CASCADE;


CREATE VIEW
tf_gnn_prep.loaded_transaction_used_device AS
SELECT
    edge.txn_id AS transaction_id,
    value.device_id,
    sequence.unix_time,
    sequence.event_seq

FROM card_fraud."cf_Transaction_Uses_Device" AS edge

JOIN tf_gnn_prep.transaction_event_seq AS sequence
  ON sequence.transaction_id = edge.txn_id

JOIN tf_gnn_prep.device_values AS value
  ON value.device_raw = edge.device_id;


CREATE VIEW
tf_gnn_prep.loaded_transaction_from_ip AS
SELECT
    edge.txn_id AS transaction_id,
    value.ip_id,
    sequence.unix_time,
    sequence.event_seq

FROM card_fraud."cf_Transaction_Uses_IP" AS edge

JOIN tf_gnn_prep.transaction_event_seq AS sequence
  ON sequence.transaction_id = edge.txn_id

JOIN tf_gnn_prep.ip_values AS value
  ON value.ip_raw = edge.ip_id;
