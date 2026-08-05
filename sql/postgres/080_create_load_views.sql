-- =====================================================================
-- LOAD VIEWS --- THE PSV COLUMN CONTRACT
--
-- One view per exported dataset. COLUMN ORDER HERE IS A POSITIONAL
-- CONTRACT WITH gsql/loading_jobs.gsql: the loading job addresses columns
-- as $0, $1, ..., so reordering a column here silently writes values into
-- the wrong attribute. Change either side and you change both;
-- scripts/check_loader_contracts.py compares the two mechanically.
--
-- Within a vertex dataset the order is the schema's own ADD VERTEX order
-- (primary id first, then attributes as declared), so a reader can diff
-- a view against gsql/schema/schema.gsql by eye.
--
-- CONVENTIONS
--   * every text column passes through tf_gnn_prep.clean_text, which
--     strips the '|' separator and the line-breaking characters. A stray
--     separator inside a value shifts every later column on that row;
--   * booleans export as the literal 'true' / 'false' TigerGraph accepts;
--   * epochs export as MILLISECONDS (tf_gnn_prep.to_ms), because
--     event_ts_ms and every *_ts_ms attribute in the schema are
--     milliseconds. This is the single most likely place to introduce a
--     1000x error, so the conversion happens exactly once, here;
--   * NO RAW PII. Device, IP, email, phone, address and identity-document
--     ids come from the tokenised value tables in 060. 090 asserts that
--     no view below selects a cf_* PII column.
--
-- WHAT IS DELIBERATELY ABSENT
--   split_id, causal_fold      no split attribute exists in the schema
--   error                      an authorization RESPONSE; the scoring
--                              moment is the request (see 070)
--   name, gender, dob          protected attributes
--   lat/lon on Address         the schema declares no coordinate columns
--                              on Address
--   Merchant_Category,         deleted vertex / edge types
--   City, State, Zipcode
-- =====================================================================


-- =====================================================================
-- 01  Party
-- =====================================================================
CREATE OR REPLACE VIEW
tf_gnn_prep.load_parties AS
SELECT
    tf_gnn_prep.clean_text(party.party_id) AS party_id,
    party.first_seen_event_seq AS first_seen_seq,
    tf_gnn_prep.to_ms(party.first_seen_unix_time) AS first_seen_ts_ms,
    tf_gnn_prep.to_ms(party.customer_since_unix_time)
        AS customer_since_ts_ms,
    tf_gnn_prep.clean_text(party.party_type) AS party_type,
    tf_gnn_prep.clean_text(party.home_country) AS home_country
FROM tf_gnn_prep.loaded_parties AS party;


-- =====================================================================
-- 02  Account
-- =====================================================================
CREATE OR REPLACE VIEW
tf_gnn_prep.load_accounts AS
SELECT
    tf_gnn_prep.clean_text(account.account_id) AS account_id,
    account.first_seen_event_seq AS first_seen_seq,
    tf_gnn_prep.to_ms(account.first_seen_unix_time) AS first_seen_ts_ms,
    tf_gnn_prep.to_ms(account.opened_unix_time) AS opened_ts_ms,
    tf_gnn_prep.clean_text(account.account_type) AS account_type,
    tf_gnn_prep.clean_text(account.base_currency) AS base_currency,
    tf_gnn_prep.clean_text(account.country_code) AS country_code
FROM tf_gnn_prep.loaded_accounts AS account;


-- =====================================================================
-- 03  Card
-- =====================================================================
CREATE OR REPLACE VIEW
tf_gnn_prep.load_cards AS
SELECT
    tf_gnn_prep.clean_text(card.card_id) AS card_id,
    card.first_seen_event_seq AS first_seen_seq,
    tf_gnn_prep.to_ms(card.first_seen_unix_time) AS first_seen_ts_ms,
    tf_gnn_prep.to_ms(card.issued_unix_time) AS issued_ts_ms,
    tf_gnn_prep.clean_text(card.card_type) AS card_type,
    tf_gnn_prep.clean_text(card.card_network) AS card_network,
    tf_gnn_prep.clean_text(card.card_product) AS card_product,
    tf_gnn_prep.boolean_text(card.is_virtual) AS is_virtual
FROM tf_gnn_prep.loaded_cards AS card;


-- =====================================================================
-- 04  Merchant
-- =====================================================================
CREATE OR REPLACE VIEW
tf_gnn_prep.load_merchants AS
SELECT
    tf_gnn_prep.clean_text(merchant.merchant_id) AS merchant_id,
    merchant.first_seen_event_seq AS first_seen_seq,
    tf_gnn_prep.to_ms(merchant.first_seen_unix_time) AS first_seen_ts_ms,
    tf_gnn_prep.to_ms(merchant.onboarded_unix_time) AS onboarded_ts_ms,
    tf_gnn_prep.clean_text(merchant.merchant_category_code)
        AS merchant_category_code,
    tf_gnn_prep.clean_text(merchant.merchant_country) AS merchant_country,
    tf_gnn_prep.clean_text(merchant.merchant_type) AS merchant_type,
    tf_gnn_prep.boolean_text(merchant.is_online) AS is_online
FROM tf_gnn_prep.loaded_merchants AS merchant;


-- =====================================================================
-- 05  Merchant_Location
--
-- Physical merchants only. has_coordinates is the mask; the coordinates
-- are zeroed rather than merely masked when it is false, so a reader who
-- forgets to check the flag cannot pick up a stray centroid.
-- =====================================================================
CREATE OR REPLACE VIEW
tf_gnn_prep.load_merchant_locations AS
SELECT
    tf_gnn_prep.clean_text(location.location_id) AS location_id,
    location.first_seen_event_seq AS first_seen_seq,
    tf_gnn_prep.to_ms(location.first_seen_unix_time) AS first_seen_ts_ms,
    tf_gnn_prep.clean_text(location.country_code) AS country_code,
    tf_gnn_prep.clean_text(location.region_code) AS region_code,
    tf_gnn_prep.clean_text(location.postal_code_prefix)
        AS postal_code_prefix,
    CASE WHEN location.has_coordinates THEN location.latitude ELSE 0 END
        AS latitude,
    CASE WHEN location.has_coordinates THEN location.longitude ELSE 0 END
        AS longitude,
    tf_gnn_prep.boolean_text(location.has_coordinates) AS has_coordinates
FROM tf_gnn_prep.merchant_locations AS location;


-- =====================================================================
-- 06  Device
-- =====================================================================
CREATE OR REPLACE VIEW
tf_gnn_prep.load_devices AS
SELECT
    tf_gnn_prep.clean_text(value.device_id) AS device_id,
    value.first_seen_event_seq AS first_seen_seq,
    tf_gnn_prep.to_ms(value.first_seen_unix_time) AS first_seen_ts_ms,
    tf_gnn_prep.clean_text(value.device_type) AS device_type,
    tf_gnn_prep.clean_text(value.os_family) AS os_family,
    tf_gnn_prep.clean_text(value.browser_family) AS browser_family
FROM tf_gnn_prep.device_values AS value;


-- =====================================================================
-- 07  IP_Address
-- =====================================================================
CREATE OR REPLACE VIEW
tf_gnn_prep.load_ip_addresses AS
SELECT
    tf_gnn_prep.clean_text(value.ip_id) AS ip_id,
    value.first_seen_event_seq AS first_seen_seq,
    tf_gnn_prep.to_ms(value.first_seen_unix_time) AS first_seen_ts_ms,
    tf_gnn_prep.clean_text(value.country_code) AS country_code,
    value.asn,
    tf_gnn_prep.clean_text(value.network_type) AS network_type
FROM tf_gnn_prep.ip_values AS value;


-- =====================================================================
-- 08  Email
-- =====================================================================
CREATE OR REPLACE VIEW
tf_gnn_prep.load_emails AS
SELECT
    tf_gnn_prep.clean_text(value.email_id) AS email_id,
    value.first_seen_event_seq AS first_seen_seq,
    tf_gnn_prep.to_ms(value.first_seen_unix_time) AS first_seen_ts_ms,
    tf_gnn_prep.clean_text(value.domain) AS domain
FROM tf_gnn_prep.email_values AS value;


-- =====================================================================
-- 09  Phone
-- =====================================================================
CREATE OR REPLACE VIEW
tf_gnn_prep.load_phones AS
SELECT
    tf_gnn_prep.clean_text(value.phone_id) AS phone_id,
    value.first_seen_event_seq AS first_seen_seq,
    tf_gnn_prep.to_ms(value.first_seen_unix_time) AS first_seen_ts_ms,
    tf_gnn_prep.clean_text(value.country_code) AS country_code,
    tf_gnn_prep.clean_text(value.line_type) AS line_type
FROM tf_gnn_prep.phone_values AS value;


-- =====================================================================
-- 10  Address
-- =====================================================================
CREATE OR REPLACE VIEW
tf_gnn_prep.load_addresses AS
SELECT
    tf_gnn_prep.clean_text(value.address_id) AS address_id,
    value.first_seen_event_seq AS first_seen_seq,
    tf_gnn_prep.to_ms(value.first_seen_unix_time) AS first_seen_ts_ms,
    tf_gnn_prep.clean_text(value.country_code) AS country_code,
    tf_gnn_prep.clean_text(value.region_code) AS region_code,
    tf_gnn_prep.clean_text(value.postal_code_prefix) AS postal_code_prefix
FROM tf_gnn_prep.address_values AS value;


-- =====================================================================
-- 11  Identity_Document
-- =====================================================================
CREATE OR REPLACE VIEW
tf_gnn_prep.load_identity_documents AS
SELECT
    tf_gnn_prep.clean_text(value.document_id) AS document_id,
    value.first_seen_event_seq AS first_seen_seq,
    tf_gnn_prep.to_ms(value.first_seen_unix_time) AS first_seen_ts_ms,
    tf_gnn_prep.clean_text(value.document_type) AS document_type,
    tf_gnn_prep.clean_text(value.issuing_country) AS issuing_country
FROM tf_gnn_prep.document_values AS value;


-- =====================================================================
-- 12-21  Slowly changing relations
--
-- Every one of these is (from, to, valid_from_seq, valid_to_seq,
-- confidence, source_system), matching the schema's shared edge shape.
--
-- confidence is 1.0 on every row and that is a statement, not a filler:
-- these relations are asserted by the source, not inferred by matching.
-- The column exists so a future entity-resolution module can write
-- probabilistic edges into the same relation without a schema change; it
-- is not a place to record how much anyone trusts the source.
--
-- source_system names the producer so a second feed can be told apart
-- from this one after the fact.
-- =====================================================================

CREATE OR REPLACE VIEW
tf_gnn_prep.load_party_owns_account AS
SELECT
    tf_gnn_prep.clean_text(edge.party_id) AS party_id,
    tf_gnn_prep.clean_text(edge.account_id) AS account_id,
    edge.valid_from_seq,
    edge.valid_to_seq,
    1.0::real AS confidence,
    'phantomledger' AS source_system
FROM tf_gnn_prep.loaded_party_owns_account AS edge;


CREATE OR REPLACE VIEW
tf_gnn_prep.load_account_has_card AS
SELECT
    tf_gnn_prep.clean_text(edge.account_id) AS account_id,
    tf_gnn_prep.clean_text(edge.card_id) AS card_id,
    edge.valid_from_seq,
    edge.valid_to_seq,
    1.0::real AS confidence,
    'phantomledger' AS source_system
FROM tf_gnn_prep.loaded_account_has_card AS edge;


CREATE OR REPLACE VIEW
tf_gnn_prep.load_party_has_email AS
SELECT
    tf_gnn_prep.clean_text(edge.party_id) AS party_id,
    tf_gnn_prep.clean_text(edge.email_id) AS email_id,
    edge.valid_from_seq,
    edge.valid_to_seq,
    1.0::real AS confidence,
    'phantomledger' AS source_system
FROM tf_gnn_prep.loaded_party_has_email AS edge;


CREATE OR REPLACE VIEW
tf_gnn_prep.load_party_has_phone AS
SELECT
    tf_gnn_prep.clean_text(edge.party_id) AS party_id,
    tf_gnn_prep.clean_text(edge.phone_id) AS phone_id,
    edge.valid_from_seq,
    edge.valid_to_seq,
    1.0::real AS confidence,
    'phantomledger' AS source_system
FROM tf_gnn_prep.loaded_party_has_phone AS edge;


CREATE OR REPLACE VIEW
tf_gnn_prep.load_party_has_address AS
SELECT
    tf_gnn_prep.clean_text(edge.party_id) AS party_id,
    tf_gnn_prep.clean_text(edge.address_id) AS address_id,
    edge.valid_from_seq,
    edge.valid_to_seq,
    1.0::real AS confidence,
    'phantomledger' AS source_system
FROM tf_gnn_prep.loaded_party_has_address AS edge;


CREATE OR REPLACE VIEW
tf_gnn_prep.load_party_has_identity_document AS
SELECT
    tf_gnn_prep.clean_text(edge.party_id) AS party_id,
    tf_gnn_prep.clean_text(edge.document_id) AS document_id,
    edge.valid_from_seq,
    edge.valid_to_seq,
    1.0::real AS confidence,
    'phantomledger' AS source_system
FROM tf_gnn_prep.loaded_party_has_identity_document AS edge;


CREATE OR REPLACE VIEW
tf_gnn_prep.load_party_has_device AS
SELECT
    tf_gnn_prep.clean_text(edge.party_id) AS party_id,
    tf_gnn_prep.clean_text(edge.device_id) AS device_id,
    edge.valid_from_seq,
    edge.valid_to_seq,
    1.0::real AS confidence,
    'phantomledger' AS source_system
FROM tf_gnn_prep.loaded_party_has_device AS edge;


CREATE OR REPLACE VIEW
tf_gnn_prep.load_party_has_ip AS
SELECT
    tf_gnn_prep.clean_text(edge.party_id) AS party_id,
    tf_gnn_prep.clean_text(edge.ip_id) AS ip_id,
    edge.valid_from_seq,
    edge.valid_to_seq,
    1.0::real AS confidence,
    'phantomledger' AS source_system
FROM tf_gnn_prep.loaded_party_has_ip AS edge;


CREATE OR REPLACE VIEW
tf_gnn_prep.load_party_operates_merchant AS
SELECT
    tf_gnn_prep.clean_text(edge.party_id) AS party_id,
    tf_gnn_prep.clean_text(edge.merchant_id) AS merchant_id,
    edge.valid_from_seq,
    edge.valid_to_seq,
    1.0::real AS confidence,
    'phantomledger' AS source_system
FROM tf_gnn_prep.loaded_party_operates_merchant AS edge;


CREATE OR REPLACE VIEW
tf_gnn_prep.load_merchant_has_location AS
SELECT
    tf_gnn_prep.clean_text(edge.merchant_id) AS merchant_id,
    tf_gnn_prep.clean_text(edge.location_id) AS location_id,
    edge.valid_from_seq,
    edge.valid_to_seq,
    1.0::real AS confidence,
    'phantomledger' AS source_system
FROM tf_gnn_prep.loaded_merchant_has_location AS edge;


-- =====================================================================
-- 22  Payment_Transaction, plus its four participation edges
--
-- ONE PSV ROW FEEDS FIVE GRAPH OBJECTS: the vertex,
-- Transaction_From_Account, Transaction_Used_Card,
-- Transaction_At_Merchant and (when the merchant has an outlet)
-- Transaction_At_Location. That is what makes the cardinality contract
-- true by construction --- exactly one Account and Merchant per event,
-- because they come off the same line --- and it is why there are no
-- separate endpoint datasets.
--
-- Columns 0-23 are the vertex in schema order. Columns 24-27 are the
-- endpoint keys, appended, and are never vertex attributes: the schema
-- deliberately keeps no denormalised join keys on the fact vertex.
--
-- event_time is re-derived from unix_time rather than copied from
-- cf_Payment_Transaction.transaction_time. 001 asserts the two agree, so
-- this is lossless, and it guarantees the readable DATETIME and the
-- authoritative event_ts_ms cannot drift apart.
--
-- location_id is EMPTY for an online merchant. The loading job's
-- Transaction_At_Location clause is guarded on that, so an empty value
-- creates no edge and no zero-id vertex.
-- =====================================================================
CREATE OR REPLACE VIEW
tf_gnn_prep.load_transactions AS
SELECT
    tf_gnn_prep.clean_text(txn.transaction_id) AS transaction_id,

    to_char(
        to_timestamp(txn.unix_time) AT TIME ZONE 'UTC',
        'YYYY-MM-DD HH24:MI:SS'
    ) AS event_time,

    tf_gnn_prep.to_ms(txn.unix_time) AS event_ts_ms,
    txn.event_seq,

    txn.amount,
    tf_gnn_prep.boolean_text(txn.amount_present) AS amount_present,
    tf_gnn_prep.clean_text(txn.currency) AS currency,
    tf_gnn_prep.clean_text(txn.transaction_type) AS transaction_type,
    tf_gnn_prep.clean_text(txn.channel) AS channel,
    tf_gnn_prep.clean_text(txn.entry_mode) AS entry_mode,
    tf_gnn_prep.boolean_text(txn.card_present) AS card_present,
    tf_gnn_prep.boolean_text(txn.is_cross_border) AS is_cross_border,
    tf_gnn_prep.boolean_text(txn.is_recurring) AS is_recurring,
    tf_gnn_prep.clean_text(txn.merchant_category_code)
        AS merchant_category_code,

    tf_gnn_prep.boolean_text(txn.has_event_location)
        AS has_event_location,
    txn.event_latitude,
    txn.event_longitude,

    txn.device_risk_score,
    txn.ip_risk_score,

    txn.fraud_label,
    tf_gnn_prep.boolean_text(txn.label_known) AS label_known,
    txn.label_available_seq,
    tf_gnn_prep.to_ms(txn.label_available_unix_time)
        AS label_available_ts_ms,
    tf_gnn_prep.clean_text(txn.label_source) AS label_source,

    -- Endpoint keys, columns 24-27.
    tf_gnn_prep.clean_text(txn.account_id) AS account_id,
    tf_gnn_prep.clean_text(txn.card_id) AS card_id,
    tf_gnn_prep.clean_text(txn.merchant_id) AS merchant_id,
    tf_gnn_prep.clean_text(coalesce(txn.location_id, '')) AS location_id

FROM tf_gnn_prep.transaction_manifest AS txn;


-- =====================================================================
-- 23  Transaction_Used_Device
-- 24  Transaction_From_IP
--
-- Separate datasets rather than more columns on 22, because these are
-- OPTIONAL endpoints with their own row counts: a transaction with no
-- recorded device produces no row here at all. Folding them into the
-- fact row would require the same empty-value guard for a relation that
-- is genuinely sparse, and would make the manifest's row count no longer
-- equal to the edge count.
-- =====================================================================

CREATE OR REPLACE VIEW
tf_gnn_prep.load_transaction_used_device AS
SELECT
    tf_gnn_prep.clean_text(edge.transaction_id) AS transaction_id,
    tf_gnn_prep.clean_text(edge.device_id) AS device_id,
    tf_gnn_prep.to_ms(edge.unix_time) AS event_ts_ms,
    edge.event_seq
FROM tf_gnn_prep.loaded_transaction_used_device AS edge;


CREATE OR REPLACE VIEW
tf_gnn_prep.load_transaction_from_ip AS
SELECT
    tf_gnn_prep.clean_text(edge.transaction_id) AS transaction_id,
    tf_gnn_prep.clean_text(edge.ip_id) AS ip_id,
    tf_gnn_prep.to_ms(edge.unix_time) AS event_ts_ms,
    edge.event_seq
FROM tf_gnn_prep.loaded_transaction_from_ip AS edge;
