-- =====================================================================
-- PARTY, ACCOUNT, CARD, MERCHANT AND THEIR SLOWLY CHANGING RELATIONS
--
-- ALL entities load. Separation between fitting and scoring windows is a
-- cutoff on event_seq applied downstream, never absence from the graph:
-- a late transaction needs its Account, Card and Merchant resident so
-- that cutoff-scoped features and temporal sampling can reach them.
--
-- ---------------------------------------------------------------------
-- THE ACCOUNT DERIVATION
-- ---------------------------------------------------------------------
-- tf_gnn_prep.funding_account_id (020) recovers the funding instrument
-- from the card id by stripping the -G<n> reissue suffix. One Account,
-- many Card generations. See 020 for why that is the source's own
-- identifier scheme rather than an invention.
--
-- The invariant it serves is the schema's: exactly one INITIATING /
-- FUNDING Account per authorization. That is not a claim that only one
-- account participates economically --- the merchant is the receiving
-- side and is the mandatory Merchant endpoint. Account-to-account
-- transfers or genuine split funding would need role-specific source and
-- destination account relations; this corpus is merchant payments, where
-- a split tender arrives as separate authorization records.
--
-- ---------------------------------------------------------------------
-- HOW valid_from_seq IS STAMPED, AND WHAT IT IS NOT
-- ---------------------------------------------------------------------
-- The schema's visibility rule is
--     valid_from_seq <= s.event_seq
--     AND (valid_to_seq == 0 OR s.event_seq < valid_to_seq)
--
-- The source carries NO per-attachment observation time for ownership or
-- PII: cf_Party_Has_Card, cf_Is_Merchant and every cf_Has_* table are
-- bare (from, to) pairs. So the stamps below are OBSERVABILITY LOWER
-- BOUNDS, not attachment times:
--
--     valid_from_seq = max(first_seen_seq of the two endpoints)
--
-- --- the relation cannot have been observed before both of its endpoints
-- were. This is the tightest defensible value, and specifically it is
-- never 0, which would make the edge visible to cutoffs from before
-- either endpoint existed.
--
-- WHAT MUST NOT BE BUILT ON IT: any "<quantity> at onset" feature ---
-- how many cards this holder had when this one was attached, how many
-- parties shared this endpoint at attachment. Under a lower-bound stamp
-- those degenerate into the party's FINAL counts, which is a whole-window
-- leak wearing a point-in-time name. The failure is silent: the join
-- succeeds and returns a plausible number.
--
-- THE ONE EXCEPTION IS Account_Has_Card, and it is a real tenure. Card
-- generations are ordered by the -G<n> suffix, so generation n's tenure
-- ends exactly where generation n+1's begins. That is a genuine
-- point-in-time fact recovered from the data, and it is what makes
-- "the card in force at this authorization" answerable.
-- =====================================================================

DROP VIEW IF EXISTS tf_gnn_prep.loaded_parties CASCADE;
DROP VIEW IF EXISTS tf_gnn_prep.loaded_accounts CASCADE;
DROP VIEW IF EXISTS tf_gnn_prep.loaded_cards CASCADE;
DROP VIEW IF EXISTS tf_gnn_prep.loaded_merchants CASCADE;
DROP VIEW IF EXISTS tf_gnn_prep.card_account CASCADE;
DROP VIEW IF EXISTS tf_gnn_prep.card_tenure CASCADE;
DROP VIEW IF EXISTS tf_gnn_prep.merchant_category CASCADE;
DROP VIEW IF EXISTS tf_gnn_prep.loaded_party_owns_account CASCADE;
DROP VIEW IF EXISTS tf_gnn_prep.loaded_account_has_card CASCADE;
DROP VIEW IF EXISTS tf_gnn_prep.loaded_party_operates_merchant CASCADE;
DROP VIEW IF EXISTS tf_gnn_prep.loaded_merchant_has_location CASCADE;

-- Retired with the schema types they fed.
DROP VIEW IF EXISTS tf_gnn_prep.loaded_categories CASCADE;
DROP VIEW IF EXISTS tf_gnn_prep.loaded_party_has_card CASCADE;
DROP VIEW IF EXISTS tf_gnn_prep.loaded_party_is_merchant CASCADE;
DROP VIEW IF EXISTS tf_gnn_prep.loaded_merchant_assigned CASCADE;


-- ------------------------------------------------------------
-- Card -> Account, with the reissue generation.
-- ------------------------------------------------------------

CREATE VIEW
tf_gnn_prep.card_account AS
SELECT
    card.card_number AS card_id,

    tf_gnn_prep.funding_account_id(card.card_number) AS account_id,

    tf_gnn_prep.card_generation(card.card_number) AS generation,

    tf_gnn_prep.instrument_type(card.card_number) AS instrument_type

FROM card_fraud."cf_Card" AS card;


-- ------------------------------------------------------------
-- PARTY
--
-- No name, no gender, no date of birth. The schema states that
-- "protected attributes such as name, gender and ethnicity are
-- intentionally absent", and the enforcement is that nothing reads
-- cf_Party.name / .gender / .dob anywhere in this pipeline --- not the
-- source contract in 001, not this view, not the load views in 080.
--
-- customer_since_ts_ms is cf_Party.created_at, which PhantomLedger
-- documents as the membership join timestamp (H3) and which is
-- point-in-time honest. It is NOT the same as first_seen: a customer can
-- join months before their first authorization, and the gap is the
-- signal a dormant-then-active bust-out is made of.
-- ------------------------------------------------------------

CREATE VIEW
tf_gnn_prep.loaded_parties AS
SELECT
    party.id AS party_id,

    coalesce(
        first_seen.first_seen_event_seq,
        floor_stamp.first_seen_event_seq
    ) AS first_seen_event_seq,

    coalesce(
        first_seen.first_seen_unix_time,
        floor_stamp.first_seen_unix_time
    ) AS first_seen_unix_time,

    CASE
        WHEN party.created_at IS NULL THEN 0
        ELSE greatest(
            floor(
                extract(
                    epoch FROM (party.created_at::timestamp AT TIME ZONE 'UTC')
                )
            )::bigint,
            0
        )
    END AS customer_since_unix_time,

    -- The schema's vocabulary is individual / company / merchant_entity,
    -- with unknown "permitted only when the source has not classified
    -- the party". Unrecognised source values map to unknown rather than
    -- passing through, so the attribute stays a closed vocabulary; 090
    -- reports the distribution so an unmapped value is a number somebody
    -- saw rather than a silent bucket.
    --
    -- Operating a merchant does NOT make a party merchant_entity.
    -- PhantomLedger's merchant register points at a human PROPRIETOR
    -- (merchant-ownership-2026-07), and overriding the source's own
    -- classification from the topology would make party_type a
    -- restatement of Party_Operates_Merchant.
    CASE lower(btrim(coalesce(party.party_type, '')))
        WHEN 'individual' THEN 'individual'
        WHEN 'person' THEN 'individual'
        WHEN 'natural' THEN 'individual'
        WHEN 'natural_person' THEN 'individual'
        WHEN 'company' THEN 'company'
        WHEN 'business' THEN 'company'
        WHEN 'corporate' THEN 'company'
        WHEN 'corporation' THEN 'company'
        WHEN 'organisation' THEN 'company'
        WHEN 'organization' THEN 'company'
        WHEN 'legal_entity' THEN 'company'
        WHEN 'merchant' THEN 'merchant_entity'
        WHEN 'merchant_entity' THEN 'merchant_entity'
        ELSE 'unknown'
    END AS party_type,

    -- Empty by construction: the source models no country. See 040.
    '' AS home_country,

    (first_seen.party_id IS NULL) AS first_seen_is_floor

FROM card_fraud."cf_Party" AS party

LEFT JOIN tf_gnn_prep.party_first_seen AS first_seen
  ON first_seen.party_id = party.id

CROSS JOIN tf_gnn_prep.first_seen_floor AS floor_stamp;


-- ------------------------------------------------------------
-- ACCOUNT
--
-- opened_ts_ms is 0 = unknown. The source has no account open date, and
-- a card's first transaction is emphatically not one: "issued, then sat
-- unused" is the bust-out shape, and first_seen cannot see it. Loading
-- first_seen here would assert that every account was opened the moment
-- it was first used, which is exactly the covariate a bust-out detector
-- would then read as real.
--
-- base_currency and country_code are empty for the same reason the
-- transaction's currency is: the source models neither.
-- ------------------------------------------------------------

CREATE VIEW
tf_gnn_prep.loaded_accounts AS
SELECT
    account.account_id,

    first_seen.first_seen_event_seq,
    first_seen.first_seen_unix_time,

    0::bigint AS opened_unix_time,

    -- Every card generation of one account shares the C/D prefix by
    -- construction (the prefix is part of the account key), so min() is
    -- the value and not a pick.
    min(account.instrument_type) AS account_type,

    '' AS base_currency,
    '' AS country_code

FROM tf_gnn_prep.card_account AS account

JOIN tf_gnn_prep.account_first_seen AS first_seen
  ON first_seen.account_id = account.account_id

GROUP BY
    account.account_id,
    first_seen.first_seen_event_seq,
    first_seen.first_seen_unix_time;


-- ------------------------------------------------------------
-- CARD
--
-- issued_ts_ms is 0 = unknown, for the same reason Account.opened_ts_ms
-- is. card_network, card_product and is_virtual have no source at all
-- and take the schema's own "unknown" / false defaults.
--
-- card_type IS derivable and is the one card attribute that carries
-- information: the C/D prefix separates a credit liability from a
-- deposit-funded debit authorization, and the two have materially
-- different fraud profiles.
--
-- A card with no transactions is LEGAL, not a bug: it is the "issued,
-- then sat unused" shape. It takes the declared first_seen floor (030)
-- and is counted by 090.
-- ------------------------------------------------------------

CREATE VIEW
tf_gnn_prep.loaded_cards AS
SELECT
    account.card_id,

    coalesce(
        first_seen.first_seen_event_seq,
        floor_stamp.first_seen_event_seq
    ) AS first_seen_event_seq,

    coalesce(
        first_seen.first_seen_unix_time,
        floor_stamp.first_seen_unix_time
    ) AS first_seen_unix_time,

    0::bigint AS issued_unix_time,

    account.instrument_type AS card_type,

    'unknown' AS card_network,
    'unknown' AS card_product,

    false AS is_virtual,

    (first_seen.card_number IS NULL) AS first_seen_is_floor

FROM tf_gnn_prep.card_account AS account

LEFT JOIN tf_gnn_prep.card_first_seen AS first_seen
  ON first_seen.card_number = account.card_id

CROSS JOIN tf_gnn_prep.first_seen_floor AS floor_stamp;


-- ------------------------------------------------------------
-- MERCHANT
--
-- Merchant_Category the vertex and Merchant_Assigned the edge are both
-- deleted from the schema; the category survives as
-- Merchant.merchant_category_code and, per transaction, as
-- Payment_Transaction.merchant_category_code.
--
-- A merchant carrying more than one category is an audit failure (090),
-- so min() below is the value rather than a pick.
--
-- merchant_type has no source and stays "unknown". is_online carries the
-- acceptance-environment fact that would otherwise want to live there,
-- and it is derived from geography presence rather than from a source
-- flag: PhantomLedger emits geography for physically located outlets
-- only.
-- ------------------------------------------------------------

CREATE VIEW
tf_gnn_prep.merchant_category AS
SELECT
    edge.merchant_id,
    count(DISTINCT edge.category) AS category_count,
    min(edge.category) AS category
FROM card_fraud."cf_Merchant_Assigned" AS edge
GROUP BY edge.merchant_id;


CREATE VIEW
tf_gnn_prep.loaded_merchants AS
SELECT
    merchant.id AS merchant_id,

    coalesce(
        first_seen.first_seen_event_seq,
        floor_stamp.first_seen_event_seq
    ) AS first_seen_event_seq,

    coalesce(
        first_seen.first_seen_unix_time,
        floor_stamp.first_seen_unix_time
    ) AS first_seen_unix_time,

    0::bigint AS onboarded_unix_time,

    tf_gnn_prep.clean_text(coalesce(category.category, ''))
        AS merchant_category_code,

    '' AS merchant_country,

    'unknown' AS merchant_type,

    coalesce(online.is_online, true) AS is_online,

    (first_seen.merchant_id IS NULL) AS first_seen_is_floor

FROM card_fraud."cf_Merchant" AS merchant

LEFT JOIN tf_gnn_prep.merchant_first_seen AS first_seen
  ON first_seen.merchant_id = merchant.id

LEFT JOIN tf_gnn_prep.merchant_category AS category
  ON category.merchant_id = merchant.id

LEFT JOIN tf_gnn_prep.merchant_is_online AS online
  ON online.merchant_id = merchant.id

CROSS JOIN tf_gnn_prep.first_seen_floor AS floor_stamp;


-- ------------------------------------------------------------
-- Party_Owns_Account
--
-- Derived through the cards: a party owns the funding instrument behind
-- every card it holds. Several parties on one account is a JOINT
-- ACCOUNT, which the schema explicitly allows ("multiple owners are
-- allowed"), so this is DISTINCT rather than aggregated to one owner.
--
-- valid_to_seq is 0 = no known end. The source never says an ownership
-- ended; inventing an end from the last transaction would close every
-- dormant account at its last use.
-- ------------------------------------------------------------

CREATE VIEW
tf_gnn_prep.loaded_party_owns_account AS
SELECT DISTINCT
    ownership.party_id,
    account.account_id,

    greatest(
        party.first_seen_event_seq,
        account_seen.first_seen_event_seq
    ) AS valid_from_seq,

    0::bigint AS valid_to_seq

FROM card_fraud."cf_Party_Has_Card" AS ownership

JOIN tf_gnn_prep.card_account AS account
  ON account.card_id = ownership.card_number

JOIN tf_gnn_prep.account_first_seen AS account_seen
  ON account_seen.account_id = account.account_id

JOIN tf_gnn_prep.party_first_seen AS party
  ON party.party_id = ownership.party_id;


-- ------------------------------------------------------------
-- Account_Has_Card --- the one real tenure in this schema
--
-- Generation n holds from its own first sighting until generation n+1's,
-- which is a reissue: the replacement plastic arrives and the previous
-- card stops being the one in force.
--
-- THE GUARD MATTERS. lead() is taken over the generation order, but the
-- tenure is only closed when the successor's first sighting is strictly
-- LATER. A successor observed at or before its predecessor would produce
-- valid_from_seq >= valid_to_seq, an empty interval, and the edge would
-- be invisible to every seed --- silently removing the card from the
-- graph rather than failing. Such a pair is left open (0) and counted by
-- 090.
-- ------------------------------------------------------------

CREATE VIEW
tf_gnn_prep.loaded_account_has_card AS
SELECT
    tenure.account_id,
    tenure.card_id,
    tenure.valid_from_seq,

    CASE
        WHEN tenure.successor_seq IS NULL THEN 0
        WHEN tenure.successor_seq > tenure.valid_from_seq
        THEN tenure.successor_seq
        ELSE 0
    END AS valid_to_seq

FROM (
    SELECT
        account.account_id,
        account.card_id,

        coalesce(
            first_seen.first_seen_event_seq,
            floor_stamp.first_seen_event_seq
        ) AS valid_from_seq,

        lead(
            coalesce(
                first_seen.first_seen_event_seq,
                floor_stamp.first_seen_event_seq
            )
        ) OVER (
            PARTITION BY account.account_id
            ORDER BY account.generation, account.card_id
        ) AS successor_seq

    FROM tf_gnn_prep.card_account AS account

    LEFT JOIN tf_gnn_prep.card_first_seen AS first_seen
      ON first_seen.card_number = account.card_id

    CROSS JOIN tf_gnn_prep.first_seen_floor AS floor_stamp
) AS tenure;


-- ------------------------------------------------------------
-- Party_Operates_Merchant
--
-- cf_Is_Merchant is the merchant -> PROPRIETOR register; the schema's
-- direction is Party -> Merchant, so the columns flip here.
--
-- The schema's cardinality contract is "at the graph high-water mark,
-- each Merchant has exactly one cutoff-visible operating Party"; 090
-- asserts it on the source rather than discovering it after the load.
-- ------------------------------------------------------------

CREATE VIEW
tf_gnn_prep.loaded_party_operates_merchant AS
SELECT DISTINCT
    edge.party_id,
    edge.merchant_id,

    greatest(
        party.first_seen_event_seq,
        merchant.first_seen_event_seq
    ) AS valid_from_seq,

    0::bigint AS valid_to_seq

FROM card_fraud."cf_Is_Merchant" AS edge

JOIN tf_gnn_prep.party_first_seen AS party
  ON party.party_id = edge.party_id

JOIN tf_gnn_prep.merchant_first_seen AS merchant
  ON merchant.merchant_id = edge.merchant_id;


-- ------------------------------------------------------------
-- Merchant_Has_Location
--
-- Physical merchants only (040). The location's first_seen is the
-- merchant's, so valid_from_seq is that same rank: the outlet cannot be
-- observed before the merchant.
-- ------------------------------------------------------------

CREATE VIEW
tf_gnn_prep.loaded_merchant_has_location AS
SELECT
    location.merchant_id,
    location.location_id,
    location.first_seen_event_seq AS valid_from_seq,
    0::bigint AS valid_to_seq

FROM tf_gnn_prep.merchant_locations AS location;
