-- =====================================================================
-- AUDITS
--
-- Two kinds of view live here and the difference is the whole design:
--
--   GATES     surface in tf_gnn_prep.audit_failures with a non-zero
--             failure_count and stop the export. Reserved for defects
--             that would silently corrupt the graph.
--   REPORTS   are read and printed by `tf-gnn-load audit` and never
--             block. Reserved for measurements whose right value is not
--             known in advance --- coverage, fanout, distributions.
--
-- A number nobody looked at and a number that is fine look identical, so
-- the reports exist to make the difference visible. A gate that fires on
-- a legitimate corpus is worse than no gate, so nothing becomes a gate
-- because it seems tidy.
-- =====================================================================


-- =====================================================================
-- SOURCE COUNTS                                                  REPORT
-- =====================================================================
CREATE OR REPLACE VIEW
tf_gnn_prep.audit_source_counts AS
SELECT
    (SELECT count(*) FROM card_fraud."cf_Payment_Transaction")
        AS transactions,
    (SELECT count(*) FROM card_fraud."cf_Card_Send_Transaction")
        AS card_edges,
    (SELECT count(*) FROM card_fraud."cf_Merchant_Receive_Transaction")
        AS merchant_edges,
    (SELECT count(*) FROM card_fraud."cf_Transaction_Uses_Device")
        AS device_edges,
    (SELECT count(*) FROM card_fraud."cf_Transaction_Uses_IP")
        AS ip_edges,
    (SELECT count(*) FROM card_fraud."cf_Card") AS cards,
    (SELECT count(*) FROM tf_gnn_prep.account_first_seen) AS accounts,
    (SELECT count(*) FROM card_fraud."cf_Merchant") AS merchants,
    (SELECT count(*) FROM card_fraud."cf_Party") AS parties;


-- =====================================================================
-- THE TEMPORAL CONTRACT                                            GATE
--
-- "event_seq is unique, deterministic, dense from 1 through the
--  transaction count, strictly positive, and ordered by
--  (event_ts_ms, transaction_id)."
--
-- out_of_order_rows is the one check that cannot be replaced by a
-- min/max/sum triple: a permutation that scrambles two rows within a
-- second still has min 1, max N and the right sum. It compares the rank
-- against a re-derivation of the same ordering.
-- =====================================================================
CREATE OR REPLACE VIEW
tf_gnn_prep.audit_event_sequence AS
SELECT
    bounds.transaction_count,
    bounds.minimum_event_seq,
    bounds.maximum_event_seq,
    bounds.minimum_epoch,
    bounds.maximum_epoch,

    (SELECT count(DISTINCT event_seq)
       FROM tf_gnn_prep.transaction_event_seq) AS distinct_event_seq,

    (SELECT count(*)
       FROM tf_gnn_prep.transaction_event_seq
      WHERE event_seq <= 0) AS zero_event_seq_rows,

    (SELECT count(*)
       FROM (
           SELECT
               event_seq,
               row_number() OVER (
                   ORDER BY unix_time, transaction_id
               ) AS expected_seq
           FROM tf_gnn_prep.transaction_event_seq
       ) AS ranked
      WHERE ranked.event_seq <> ranked.expected_seq)
        AS out_of_order_rows

FROM tf_gnn_prep.observation_bounds AS bounds;


-- =====================================================================
-- LABEL MATURITY                                                   GATE
--
-- label_known is the mask and must partition the rows; a known label
-- must carry both an availability rank and an availability timestamp;
-- confirmed fraud must become knowable strictly AFTER the event it
-- describes, never at it.
-- =====================================================================
CREATE OR REPLACE VIEW
tf_gnn_prep.audit_label_maturity AS
SELECT
    count(*) AS transactions,

    count(*) FILTER (WHERE txn.fraud_label = 1) AS fraud_rows,
    count(*) FILTER (WHERE txn.fraud_label = 0) AS clean_rows,

    count(*) FILTER (WHERE txn.label_known) AS known_rows,
    count(*) FILTER (WHERE NOT txn.label_known) AS unknown_rows,

    count(*) FILTER (
        WHERE txn.label_known
          AND (txn.label_available_unix_time = 0
               OR txn.label_available_seq = 0)
    ) AS known_without_availability,

    count(*) FILTER (
        WHERE NOT txn.label_known
          AND (txn.label_available_unix_time <> 0
               OR txn.label_available_seq <> 0
               OR txn.label_source <> '')
    ) AS unknown_with_availability,

    -- A fraud verdict lands after the event. Equality would mean the
    -- label was knowable at the authorization request, which is the one
    -- thing it is not.
    count(*) FILTER (
        WHERE txn.fraud_label = 1
          AND txn.label_available_unix_time <= txn.unix_time
    ) AS fraud_available_not_after_event,

    -- Every fraud row is confirmable by construction, so an unknown
    -- fraud label would mean the policy branch was not taken.
    count(*) FILTER (
        WHERE txn.fraud_label = 1 AND NOT txn.label_known
    ) AS fraud_rows_unknown,

    count(*) FILTER (
        WHERE txn.fraud_label NOT IN (0, 1)
    ) AS label_out_of_domain

FROM tf_gnn_prep.transaction_manifest AS txn;


-- =====================================================================
-- ENDPOINT INTEGRITY AND CARDINALITY                               GATE
--
-- The schema's cardinality contract, asserted before 27M rows are
-- exported rather than only after they reach the graph:
--
--   exactly one initiating/funding Account and one receiving Merchant;
--   at most one Card, Merchant_Location, Device and IP_Address;
--   each Merchant has exactly one cutoff-visible operating Party;
--   each Account has at least one owning Party.
--
-- Account and Card come off the same source edge, so exactly-one-Account
-- and at-most-one-Card are one fact; 001 already asserted the row count.
-- What this view adds is REFERENTIAL closure: every endpoint key in the
-- manifest resolves to a vertex the loader actually exports. A key that
-- does not is the worst failure mode available, because TigerGraph
-- upserts the missing endpoint with schema defaults --- first_seen_seq 0,
-- which passes every cutoff filter including cutoffs from before the
-- entity existed.
-- =====================================================================
CREATE OR REPLACE VIEW
tf_gnn_prep.audit_endpoint_integrity AS
SELECT
    (SELECT count(*)
       FROM tf_gnn_prep.transaction_manifest AS txn
       LEFT JOIN tf_gnn_prep.loaded_accounts AS account
         ON account.account_id = txn.account_id
      WHERE account.account_id IS NULL) AS transactions_without_account,

    (SELECT count(*)
       FROM tf_gnn_prep.transaction_manifest AS txn
       LEFT JOIN tf_gnn_prep.loaded_cards AS card
         ON card.card_id = txn.card_id
      WHERE card.card_id IS NULL) AS transactions_without_card,

    (SELECT count(*)
       FROM tf_gnn_prep.transaction_manifest AS txn
       LEFT JOIN tf_gnn_prep.loaded_merchants AS merchant
         ON merchant.merchant_id = txn.merchant_id
      WHERE merchant.merchant_id IS NULL) AS transactions_without_merchant,

    (SELECT count(*)
       FROM tf_gnn_prep.transaction_manifest AS txn
       LEFT JOIN tf_gnn_prep.merchant_locations AS location
         ON location.location_id = txn.location_id
      WHERE txn.location_id IS NOT NULL
        AND location.location_id IS NULL) AS transactions_without_location,

    (SELECT count(*)
       FROM tf_gnn_prep.loaded_transaction_used_device AS edge
       LEFT JOIN tf_gnn_prep.device_values AS value
         ON value.device_id = edge.device_id
      WHERE value.device_id IS NULL) AS device_edges_without_device,

    (SELECT count(*)
       FROM tf_gnn_prep.loaded_transaction_from_ip AS edge
       LEFT JOIN tf_gnn_prep.ip_values AS value
         ON value.ip_id = edge.ip_id
      WHERE value.ip_id IS NULL) AS ip_edges_without_ip,

    -- Each Account has at least one owning Party.
    (SELECT count(*)
       FROM tf_gnn_prep.loaded_accounts AS account
       LEFT JOIN tf_gnn_prep.loaded_party_owns_account AS ownership
         ON ownership.account_id = account.account_id
      WHERE ownership.account_id IS NULL) AS accounts_without_owner,

    -- Each Merchant has exactly one operating Party. More than one is a
    -- contract violation; none is not, because PhantomLedger's merchant
    -- register covers proprietors only for catalogue merchants and the
    -- schema's rule is about merchants that HAVE an operator.
    (SELECT count(*)
       FROM (
           SELECT merchant_id
           FROM tf_gnn_prep.loaded_party_operates_merchant
           GROUP BY merchant_id
           HAVING count(*) > 1
       ) AS multi) AS merchants_with_multiple_operators,

    (SELECT count(*)
       FROM tf_gnn_prep.loaded_merchants AS merchant
       LEFT JOIN tf_gnn_prep.loaded_party_operates_merchant AS operator
         ON operator.merchant_id = merchant.merchant_id
      WHERE operator.merchant_id IS NULL) AS merchants_without_operator;


-- =====================================================================
-- FIRST-SEEN STAMPS                                          GATE + REPORT
--
-- GATE: no dimension vertex may carry first_seen_seq = 0. See the header
-- of 030 --- a zero stamp opens every cutoff filter silently.
--
-- REPORT: how many took the declared floor (present at the start of
-- observation) rather than a real transaction. A large number here does
-- not mean the load is wrong; it means a large share of the registry
-- never transacts, which is worth knowing before anyone reads a
-- "first seen" feature.
-- =====================================================================
CREATE OR REPLACE VIEW
tf_gnn_prep.audit_first_seen AS
SELECT
    (SELECT count(*) FROM tf_gnn_prep.load_parties
      WHERE first_seen_seq <= 0)
    + (SELECT count(*) FROM tf_gnn_prep.load_accounts
        WHERE first_seen_seq <= 0)
    + (SELECT count(*) FROM tf_gnn_prep.load_cards
        WHERE first_seen_seq <= 0)
    + (SELECT count(*) FROM tf_gnn_prep.load_merchants
        WHERE first_seen_seq <= 0)
    + (SELECT count(*) FROM tf_gnn_prep.load_merchant_locations
        WHERE first_seen_seq <= 0)
    + (SELECT count(*) FROM tf_gnn_prep.load_devices
        WHERE first_seen_seq <= 0)
    + (SELECT count(*) FROM tf_gnn_prep.load_ip_addresses
        WHERE first_seen_seq <= 0)
    + (SELECT count(*) FROM tf_gnn_prep.load_emails
        WHERE first_seen_seq <= 0)
    + (SELECT count(*) FROM tf_gnn_prep.load_phones
        WHERE first_seen_seq <= 0)
    + (SELECT count(*) FROM tf_gnn_prep.load_addresses
        WHERE first_seen_seq <= 0)
    + (SELECT count(*) FROM tf_gnn_prep.load_identity_documents
        WHERE first_seen_seq <= 0)
        AS vertices_with_zero_first_seen,

    (SELECT count(*) FROM tf_gnn_prep.loaded_parties
      WHERE first_seen_is_floor) AS parties_at_floor,

    (SELECT count(*) FROM tf_gnn_prep.loaded_cards
      WHERE first_seen_is_floor) AS cards_at_floor,

    (SELECT count(*) FROM tf_gnn_prep.loaded_merchants
      WHERE first_seen_is_floor) AS merchants_at_floor;


-- =====================================================================
-- SLOWLY CHANGING INTERVALS                                  GATE + REPORT
--
-- The schema's visibility rule is
--     valid_from_seq <= s.event_seq
--     AND (valid_to_seq == 0 OR s.event_seq < valid_to_seq)
--
-- Two ways to write an interval that is silently empty, and both are
-- gates because the symptom is a MISSING edge rather than an error:
--   valid_from_seq = 0    visible from before either endpoint existed;
--   valid_to_seq <= valid_from_seq while non-zero    visible to nobody.
--
-- closed_card_tenures is a REPORT and is the interesting number in this
-- view: it counts Account_Has_Card rows whose tenure actually ends,
-- i.e. reissues. Zero means the corpus has no card churn, which makes
-- every reissue-based feature dead weight.
-- =====================================================================
CREATE OR REPLACE VIEW
tf_gnn_prep.audit_scd_intervals AS
WITH every_relation AS (
    SELECT valid_from_seq, valid_to_seq
      FROM tf_gnn_prep.load_party_owns_account
    UNION ALL
    SELECT valid_from_seq, valid_to_seq
      FROM tf_gnn_prep.load_account_has_card
    UNION ALL
    SELECT valid_from_seq, valid_to_seq
      FROM tf_gnn_prep.load_party_has_email
    UNION ALL
    SELECT valid_from_seq, valid_to_seq
      FROM tf_gnn_prep.load_party_has_phone
    UNION ALL
    SELECT valid_from_seq, valid_to_seq
      FROM tf_gnn_prep.load_party_has_address
    UNION ALL
    SELECT valid_from_seq, valid_to_seq
      FROM tf_gnn_prep.load_party_has_identity_document
    UNION ALL
    SELECT valid_from_seq, valid_to_seq
      FROM tf_gnn_prep.load_party_has_device
    UNION ALL
    SELECT valid_from_seq, valid_to_seq
      FROM tf_gnn_prep.load_party_has_ip
    UNION ALL
    SELECT valid_from_seq, valid_to_seq
      FROM tf_gnn_prep.load_party_operates_merchant
    UNION ALL
    SELECT valid_from_seq, valid_to_seq
      FROM tf_gnn_prep.load_merchant_has_location
)
SELECT
    count(*) FILTER (WHERE valid_from_seq <= 0)
        AS relations_with_zero_valid_from,

    count(*) FILTER (
        WHERE valid_to_seq <> 0 AND valid_to_seq <= valid_from_seq
    ) AS relations_with_empty_interval,

    (SELECT count(*)
       FROM tf_gnn_prep.load_account_has_card
      WHERE valid_to_seq <> 0) AS closed_card_tenures,

    (SELECT count(*)
       FROM (
           SELECT account_id
           FROM tf_gnn_prep.load_account_has_card
           GROUP BY account_id
           HAVING count(*) > 1
       ) AS reissued) AS accounts_with_multiple_cards

FROM every_relation;


-- =====================================================================
-- MERCHANT GEOGRAPHY                                         GATE + REPORT
--
-- PARTIAL geography is a gate: a merchant with a city but no zip would
-- fall through 040's all-three test into is_online = true, which would
-- silently reclassify a physical outlet as card-not-present.
--
-- MULTIPLE cities / states / zips is also a gate: 040 takes min() and
-- documents it as "the value, not a pick", which is only true while the
-- source emits at most one.
-- =====================================================================
CREATE OR REPLACE VIEW
tf_gnn_prep.audit_geography AS
SELECT
    count(*) AS merchants,

    count(*) FILTER (
        WHERE (city_id IS NOT NULL)::int
            + (state_id IS NOT NULL)::int
            + (zipcode_id IS NOT NULL)::int
            BETWEEN 1 AND 2
    ) AS merchants_with_partial_geography,

    count(*) FILTER (WHERE city_count > 1) AS merchants_with_many_cities,
    count(*) FILTER (WHERE state_count > 1) AS merchants_with_many_states,
    count(*) FILTER (WHERE zipcode_count > 1) AS merchants_with_many_zips,

    count(*) FILTER (
        WHERE city_id IS NOT NULL
          AND state_id IS NOT NULL
          AND zipcode_id IS NOT NULL
    ) AS physical_merchants,

    count(*) FILTER (
        WHERE city_id IS NULL
          AND state_id IS NULL
          AND zipcode_id IS NULL
    ) AS online_merchants

FROM tf_gnn_prep.merchant_geography;


CREATE OR REPLACE VIEW
tf_gnn_prep.audit_coordinates AS
SELECT
    (SELECT count(*) FROM tf_gnn_prep.merchant_locations)
        AS merchant_locations,

    (SELECT count(*) FROM tf_gnn_prep.merchant_locations
      WHERE has_coordinates) AS locations_with_coordinates,

    (SELECT count(*) FROM tf_gnn_prep.merchant_locations
      WHERE NOT has_coordinates) AS locations_masked,

    (SELECT count(*) FROM tf_gnn_prep.transaction_manifest
      WHERE has_event_location) AS transactions_with_event_location,

    -- A card-not-present authorization has no terminal, so an event
    -- location on one would be a fabricated cardholder position.
    (SELECT count(*) FROM tf_gnn_prep.transaction_manifest
      WHERE has_event_location AND NOT card_present)
        AS card_not_present_with_event_location,

    (SELECT count(*) FROM tf_gnn_prep.address_values
      WHERE distinct_area_count > 1) AS addresses_masked_ambiguous,

    (SELECT count(*) FROM tf_gnn_prep.address_values
      WHERE region_code <> '' OR postal_code_prefix <> '')
        AS addresses_with_area;


-- =====================================================================
-- CATEGORY CONSISTENCY                                             GATE
--
-- Merchant.merchant_category_code is a single value, so a merchant
-- carrying two source categories would silently take min().
-- =====================================================================
CREATE OR REPLACE VIEW
tf_gnn_prep.audit_category_consistency AS
SELECT
    count(*) AS merchants_with_category,
    count(*) FILTER (WHERE category_count > 1)
        AS merchants_with_many_categories,
    (SELECT count(*)
       FROM tf_gnn_prep.loaded_merchants
      WHERE merchant_category_code = '')
        AS merchants_without_category
FROM tf_gnn_prep.merchant_category;


-- =====================================================================
-- CHANNEL AND PARTY-TYPE VOCABULARIES                             REPORT
--
-- Both attributes map a free source string onto a closed vocabulary with
-- an "unknown" fallback. The fallback is where a source that grows a new
-- value would land, silently, so the counts are reported rather than
-- assumed to be zero.
-- =====================================================================
CREATE OR REPLACE VIEW
tf_gnn_prep.audit_vocabularies AS
SELECT
    (SELECT count(*) FROM tf_gnn_prep.transaction_manifest
      WHERE channel = 'unknown') AS transactions_unknown_channel,

    (SELECT count(*) FROM tf_gnn_prep.transaction_manifest
      WHERE entry_mode = 'unknown') AS transactions_unknown_entry_mode,

    (SELECT count(*) FROM tf_gnn_prep.transaction_manifest
      WHERE channel = 'ecommerce') AS transactions_ecommerce,

    (SELECT count(*) FROM tf_gnn_prep.transaction_manifest
      WHERE card_present) AS transactions_card_present,

    (SELECT count(*) FROM tf_gnn_prep.loaded_parties
      WHERE party_type = 'unknown') AS parties_unknown_type,

    (SELECT count(*) FROM tf_gnn_prep.loaded_parties
      WHERE party_type = 'individual') AS parties_individual,

    (SELECT count(*) FROM tf_gnn_prep.loaded_parties
      WHERE party_type = 'company') AS parties_company,

    (SELECT count(*) FROM tf_gnn_prep.loaded_parties
      WHERE party_type = 'merchant_entity') AS parties_merchant_entity,

    (SELECT count(*) FROM tf_gnn_prep.load_cards
      WHERE card_type = 'credit') AS cards_credit,

    (SELECT count(*) FROM tf_gnn_prep.load_cards
      WHERE card_type = 'debit') AS cards_debit,

    (SELECT count(*) FROM tf_gnn_prep.load_cards
      WHERE card_type = 'unknown') AS cards_unknown_type;


-- =====================================================================
-- PII TOKENISATION                                           GATE + REPORT
--
-- GATE 1, COLLISIONS. A truncated HMAC is a hash, and a hash can
-- collide. At 128 bits and order 10^6 values the probability is ~10^-27,
-- so a non-zero count here means something other than chance --- most
-- likely that the token function returned a constant because the policy
-- row was missing.
--
-- GATE 2, RAW-VALUE LEAKAGE, and this is the one that matters. It reads
-- the CATALOGUE, not the data: every column of every tf_gnn_prep.load_*
-- view is checked against the set of names a raw PII column would have.
-- A view that starts selecting cf_Email.email fails here even if every
-- row in it happens to look like a token.
-- =====================================================================
CREATE OR REPLACE VIEW
tf_gnn_prep.audit_pii_tokens AS
SELECT
    (SELECT count(*) FROM tf_gnn_prep.email_values) AS emails,
    (SELECT count(*) FROM tf_gnn_prep.phone_values) AS phones,
    (SELECT count(*) FROM tf_gnn_prep.address_values) AS addresses,
    (SELECT count(*) FROM tf_gnn_prep.document_values) AS documents,
    (SELECT count(*) FROM tf_gnn_prep.device_values) AS devices,
    (SELECT count(*) FROM tf_gnn_prep.ip_values) AS ip_addresses,

    (SELECT count(*) FROM tf_gnn_prep.email_values)
    - (SELECT count(DISTINCT email_id) FROM tf_gnn_prep.email_values)
    + (SELECT count(*) FROM tf_gnn_prep.phone_values)
    - (SELECT count(DISTINCT phone_id) FROM tf_gnn_prep.phone_values)
    + (SELECT count(*) FROM tf_gnn_prep.address_values)
    - (SELECT count(DISTINCT address_id) FROM tf_gnn_prep.address_values)
    + (SELECT count(*) FROM tf_gnn_prep.document_values)
    - (SELECT count(DISTINCT document_id) FROM tf_gnn_prep.document_values)
    + (SELECT count(*) FROM tf_gnn_prep.device_values)
    - (SELECT count(DISTINCT device_id) FROM tf_gnn_prep.device_values)
    + (SELECT count(*) FROM tf_gnn_prep.ip_values)
    - (SELECT count(DISTINCT ip_id) FROM tf_gnn_prep.ip_values)
        AS token_collisions,

    -- Endpoints observed transacting but never enrolled to a party. NOT
    -- a defect: PhantomLedger's registry coverage is deliberately
    -- partial, and this residual is where an unenrolled attacker
    -- endpoint lives. Reported because a sudden zero would mean the
    -- union in 060 stopped working.
    (SELECT count(*) FROM tf_gnn_prep.device_values
      WHERE used_by_transaction AND NOT held_by_party)
        AS devices_transacting_without_party,

    (SELECT count(*) FROM tf_gnn_prep.ip_values
      WHERE used_by_transaction AND NOT held_by_party)
        AS ips_transacting_without_party;


-- Column-name gate over the load views. FORBIDDEN_COLUMNS is the union
-- of: raw PII column names, protected attributes, split attributes the
-- schema does not declare, and the authorization RESPONSE.
CREATE OR REPLACE VIEW
tf_gnn_prep.audit_forbidden_columns AS
SELECT
    coalesce(count(*), 0) AS forbidden_columns,
    coalesce(
        string_agg(
            table_name || '.' || column_name,
            ', ' ORDER BY table_name, column_name
        ),
        ''
    ) AS detail
FROM information_schema.columns
WHERE table_schema = 'tf_gnn_prep'
  AND table_name LIKE 'load\_%'
  AND column_name IN (
      -- raw PII
      'email', 'email_raw', 'phone_number', 'phone_raw',
      'address', 'address_raw', 'device_raw', 'ip_raw', 'document_raw',
      -- protected attributes
      'name', 'gender', 'dob', 'birthdate', 'full_name',
      -- attributes the schema does not declare
      'split_id', 'causal_fold', 'error', 'is_blocked', 'is_online_txn',
      'label_resolution_status'
  );


-- =====================================================================
-- IDENTITY FANOUT                                                 REPORT
--
-- Parties per shared value, per type. This is the measurement that says
-- whether shared-PII topology carries signal: a device held by two
-- parties is a ring, the same device held by two hundred is an internet
-- cafe, and only the data can say which end the distribution sits at.
--
-- No gate. The right cap is a modelling decision made downstream from
-- these numbers, not a threshold this loader can assert.
-- =====================================================================
CREATE OR REPLACE VIEW
tf_gnn_prep.audit_identity_fanout AS
WITH shared AS (
    SELECT 'email' AS pii_kind, email_id AS value_id, count(*) AS parties
      FROM tf_gnn_prep.load_party_has_email GROUP BY email_id
    UNION ALL
    SELECT 'phone', phone_id, count(*)
      FROM tf_gnn_prep.load_party_has_phone GROUP BY phone_id
    UNION ALL
    SELECT 'address', address_id, count(*)
      FROM tf_gnn_prep.load_party_has_address GROUP BY address_id
    UNION ALL
    SELECT 'identity_document', document_id, count(*)
      FROM tf_gnn_prep.load_party_has_identity_document
     GROUP BY document_id
    UNION ALL
    SELECT 'device', device_id, count(*)
      FROM tf_gnn_prep.load_party_has_device GROUP BY device_id
    UNION ALL
    SELECT 'ip', ip_id, count(*)
      FROM tf_gnn_prep.load_party_has_ip GROUP BY ip_id
)
SELECT
    pii_kind,
    count(*) AS distinct_values,
    sum(parties) AS relations,
    max(parties) AS max_parties_per_value,
    round(avg(parties)::numeric, 4) AS mean_parties_per_value,
    count(*) FILTER (WHERE parties > 1) AS shared_values
FROM shared
GROUP BY pii_kind
ORDER BY pii_kind;


-- =====================================================================
-- PII ORPHANS                                                     REPORT
--
-- Registry values that no party holds and no transaction used. They are
-- excluded from the load on purpose (an isolated vertex is noise), and
-- counting them keeps the exclusion measured rather than silent.
-- =====================================================================
CREATE OR REPLACE VIEW
tf_gnn_prep.audit_pii_orphans AS
SELECT
    (SELECT count(*) FROM card_fraud."cf_Email" AS registry
      LEFT JOIN tf_gnn_prep.email_values AS loaded
        ON loaded.email_raw = registry.email
     WHERE loaded.email_raw IS NULL) AS unreferenced_emails,

    (SELECT count(*) FROM card_fraud."cf_Phone" AS registry
      LEFT JOIN tf_gnn_prep.phone_values AS loaded
        ON loaded.phone_raw = registry.phone_number
     WHERE loaded.phone_raw IS NULL) AS unreferenced_phones,

    (SELECT count(*) FROM card_fraud."cf_Address" AS registry
      LEFT JOIN tf_gnn_prep.address_values AS loaded
        ON loaded.address_raw = registry.address
     WHERE loaded.address_raw IS NULL) AS unreferenced_addresses,

    (SELECT count(*) FROM card_fraud."cf_ID" AS registry
      LEFT JOIN tf_gnn_prep.document_values AS loaded
        ON loaded.document_raw = registry.id
     WHERE loaded.document_raw IS NULL) AS unreferenced_documents,

    (SELECT count(*) FROM card_fraud."cf_Device" AS registry
      LEFT JOIN tf_gnn_prep.device_values AS loaded
        ON loaded.device_raw = registry.id
     WHERE loaded.device_raw IS NULL) AS unreferenced_devices,

    (SELECT count(*) FROM card_fraud."cf_IP" AS registry
      LEFT JOIN tf_gnn_prep.ip_values AS loaded
        ON loaded.ip_raw = registry.id
     WHERE loaded.ip_raw IS NULL) AS unreferenced_ips;


-- =====================================================================
-- LOAD COUNTS                                                     REPORT
--
-- One row per exported dataset. `tf-gnn-load export` writes the same
-- numbers into export_manifest.json and the TigerGraph verifier compares
-- the loaded graph against them, so this view is the first of three
-- places the same count has to agree.
--
-- The dataset names match src/tf_gnn_loader/postgres/export.py exactly.
-- =====================================================================
CREATE OR REPLACE VIEW
tf_gnn_prep.audit_load_counts AS
SELECT '01_parties' AS dataset,
       (SELECT count(*) FROM tf_gnn_prep.load_parties) AS rows
UNION ALL SELECT '02_accounts',
       (SELECT count(*) FROM tf_gnn_prep.load_accounts)
UNION ALL SELECT '03_cards',
       (SELECT count(*) FROM tf_gnn_prep.load_cards)
UNION ALL SELECT '04_merchants',
       (SELECT count(*) FROM tf_gnn_prep.load_merchants)
UNION ALL SELECT '05_merchant_locations',
       (SELECT count(*) FROM tf_gnn_prep.load_merchant_locations)
UNION ALL SELECT '06_devices',
       (SELECT count(*) FROM tf_gnn_prep.load_devices)
UNION ALL SELECT '07_ip_addresses',
       (SELECT count(*) FROM tf_gnn_prep.load_ip_addresses)
UNION ALL SELECT '08_emails',
       (SELECT count(*) FROM tf_gnn_prep.load_emails)
UNION ALL SELECT '09_phones',
       (SELECT count(*) FROM tf_gnn_prep.load_phones)
UNION ALL SELECT '10_addresses',
       (SELECT count(*) FROM tf_gnn_prep.load_addresses)
UNION ALL SELECT '11_identity_documents',
       (SELECT count(*) FROM tf_gnn_prep.load_identity_documents)
UNION ALL SELECT '12_party_owns_account',
       (SELECT count(*) FROM tf_gnn_prep.load_party_owns_account)
UNION ALL SELECT '13_account_has_card',
       (SELECT count(*) FROM tf_gnn_prep.load_account_has_card)
UNION ALL SELECT '14_party_has_email',
       (SELECT count(*) FROM tf_gnn_prep.load_party_has_email)
UNION ALL SELECT '15_party_has_phone',
       (SELECT count(*) FROM tf_gnn_prep.load_party_has_phone)
UNION ALL SELECT '16_party_has_address',
       (SELECT count(*) FROM tf_gnn_prep.load_party_has_address)
UNION ALL SELECT '17_party_has_identity_document',
       (SELECT count(*) FROM tf_gnn_prep.load_party_has_identity_document)
UNION ALL SELECT '18_party_has_device',
       (SELECT count(*) FROM tf_gnn_prep.load_party_has_device)
UNION ALL SELECT '19_party_has_ip',
       (SELECT count(*) FROM tf_gnn_prep.load_party_has_ip)
UNION ALL SELECT '20_party_operates_merchant',
       (SELECT count(*) FROM tf_gnn_prep.load_party_operates_merchant)
UNION ALL SELECT '21_merchant_has_location',
       (SELECT count(*) FROM tf_gnn_prep.load_merchant_has_location)
UNION ALL SELECT '22_transactions',
       (SELECT count(*) FROM tf_gnn_prep.load_transactions)
UNION ALL SELECT '23_transaction_used_device',
       (SELECT count(*) FROM tf_gnn_prep.load_transaction_used_device)
UNION ALL SELECT '24_transaction_from_ip',
       (SELECT count(*) FROM tf_gnn_prep.load_transaction_from_ip);


-- =====================================================================
-- THE GATE SET
--
-- Every row must be zero. `tf-gnn-load export` refuses to run otherwise,
-- so a defect here never becomes 27 million PSV rows.
-- =====================================================================
CREATE OR REPLACE VIEW
tf_gnn_prep.audit_failures AS

SELECT 'transaction_card_edge_count_mismatch'::text AS check_name,
       abs(transactions - card_edges) AS failure_count
  FROM tf_gnn_prep.audit_source_counts

UNION ALL SELECT 'transaction_merchant_edge_count_mismatch',
       abs(transactions - merchant_edges)
  FROM tf_gnn_prep.audit_source_counts

UNION ALL SELECT 'event_seq_not_starting_at_one',
       CASE WHEN transaction_count > 0 AND minimum_event_seq <> 1
            THEN 1 ELSE 0 END
  FROM tf_gnn_prep.audit_event_sequence

UNION ALL SELECT 'event_seq_not_dense',
       CASE WHEN maximum_event_seq <> transaction_count
                 OR distinct_event_seq <> transaction_count
            THEN 1 ELSE 0 END
  FROM tf_gnn_prep.audit_event_sequence

UNION ALL SELECT 'event_seq_zero_rows', zero_event_seq_rows
  FROM tf_gnn_prep.audit_event_sequence

UNION ALL SELECT 'event_seq_out_of_order_rows', out_of_order_rows
  FROM tf_gnn_prep.audit_event_sequence

UNION ALL SELECT 'label_known_without_availability',
       known_without_availability
  FROM tf_gnn_prep.audit_label_maturity

UNION ALL SELECT 'label_unknown_with_availability',
       unknown_with_availability
  FROM tf_gnn_prep.audit_label_maturity

UNION ALL SELECT 'label_fraud_available_not_after_event',
       fraud_available_not_after_event
  FROM tf_gnn_prep.audit_label_maturity

UNION ALL SELECT 'label_fraud_rows_unknown', fraud_rows_unknown
  FROM tf_gnn_prep.audit_label_maturity

UNION ALL SELECT 'label_out_of_domain', label_out_of_domain
  FROM tf_gnn_prep.audit_label_maturity

UNION ALL SELECT 'transactions_without_account',
       transactions_without_account
  FROM tf_gnn_prep.audit_endpoint_integrity

UNION ALL SELECT 'transactions_without_card', transactions_without_card
  FROM tf_gnn_prep.audit_endpoint_integrity

UNION ALL SELECT 'transactions_without_merchant',
       transactions_without_merchant
  FROM tf_gnn_prep.audit_endpoint_integrity

UNION ALL SELECT 'transactions_without_location',
       transactions_without_location
  FROM tf_gnn_prep.audit_endpoint_integrity

UNION ALL SELECT 'device_edges_without_device',
       device_edges_without_device
  FROM tf_gnn_prep.audit_endpoint_integrity

UNION ALL SELECT 'ip_edges_without_ip', ip_edges_without_ip
  FROM tf_gnn_prep.audit_endpoint_integrity

UNION ALL SELECT 'accounts_without_owner', accounts_without_owner
  FROM tf_gnn_prep.audit_endpoint_integrity

UNION ALL SELECT 'merchants_with_multiple_operators',
       merchants_with_multiple_operators
  FROM tf_gnn_prep.audit_endpoint_integrity

UNION ALL SELECT 'vertices_with_zero_first_seen',
       vertices_with_zero_first_seen
  FROM tf_gnn_prep.audit_first_seen

UNION ALL SELECT 'relations_with_zero_valid_from',
       relations_with_zero_valid_from
  FROM tf_gnn_prep.audit_scd_intervals

UNION ALL SELECT 'relations_with_empty_interval',
       relations_with_empty_interval
  FROM tf_gnn_prep.audit_scd_intervals

UNION ALL SELECT 'merchants_with_partial_geography',
       merchants_with_partial_geography
  FROM tf_gnn_prep.audit_geography

UNION ALL SELECT 'merchants_with_many_cities', merchants_with_many_cities
  FROM tf_gnn_prep.audit_geography

UNION ALL SELECT 'merchants_with_many_states', merchants_with_many_states
  FROM tf_gnn_prep.audit_geography

UNION ALL SELECT 'merchants_with_many_zips', merchants_with_many_zips
  FROM tf_gnn_prep.audit_geography

UNION ALL SELECT 'card_not_present_with_event_location',
       card_not_present_with_event_location
  FROM tf_gnn_prep.audit_coordinates

UNION ALL SELECT 'merchants_with_many_categories',
       merchants_with_many_categories
  FROM tf_gnn_prep.audit_category_consistency

UNION ALL SELECT 'pii_token_collisions', token_collisions
  FROM tf_gnn_prep.audit_pii_tokens

UNION ALL SELECT 'forbidden_columns_in_load_views', forbidden_columns
  FROM tf_gnn_prep.audit_forbidden_columns;
