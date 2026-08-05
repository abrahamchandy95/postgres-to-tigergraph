CREATE SCHEMA IF NOT EXISTS tf_gnn_prep;


-- =====================================================================
-- POLICIES AND VALUE HELPERS
--
-- Two policies survive into TransactionFraud_GNN, and one is gone.
--
-- GONE: THE SPLIT POLICY. The old schema carried split_id and
-- causal_fold on Payment_Transaction and this file assigned them from
-- the IBM/TabFormer benchmark calendar (train before 2018, validation
-- 2018, test 2019+). The new schema carries NO split attribute at all:
-- separation is a cutoff on event_seq, applied by whoever is training,
-- and a stored split column would be a calendar proxy resident on the
-- fact vertex. Nothing here computes one, and 090 asserts that no load
-- view emits one.
--
-- KEPT: THE LABEL POLICY. label_known, label_available_seq,
-- label_available_ts_ms and label_source are first-class schema
-- attributes, and they are where victim-reported scam fraud folds in.
-- A coached payment ("your son is in jail, wire the bail fee") is
-- AUTHORISED by the legitimate cardholder: the row is unremarkable at
-- the authorization request, and the fraud becomes knowable only when
-- the victim realises and reports. fraud_label records what happened;
-- label_available_* record when it became knowable.
--
-- NEW: THE PII TOKEN POLICY. The schema's privacy contract is explicit
-- --- "Email, phone, address, identity-document, device and IP primary
-- IDs must be salted/tokenized before loading. Raw PII is not stored."
-- This file is where that happens, and it is the only place.
-- =====================================================================


-- ------------------------------------------------------------------
-- PostgreSQL version gate.
--
-- sha256() is a CORE function from PostgreSQL 11. The whole tokenisation
-- layer is built on it precisely so no extension is required: the
-- previous revision of this pipeline hand-wrote Soundex in SQL for the
-- same reason (fuzzystrmatch was not installable on the target
-- instance), and pgcrypto is no safer a bet.
-- ------------------------------------------------------------------

DO $version_gate$
BEGIN
    IF current_setting('server_version_num')::int < 110000 THEN
        RAISE EXCEPTION
            'PostgreSQL 11+ is required: the PII tokenisation layer is '
            'built on the core sha256() function so that no extension '
            'has to be installable on the source instance. Found %.',
            current_setting('server_version');
    END IF;
END
$version_gate$;


-- ------------------------------------------------------------------
-- TEXT HELPERS
-- ------------------------------------------------------------------

-- Replace delimiter and line-breaking characters before PSV export.
CREATE OR REPLACE FUNCTION
tf_gnn_prep.clean_text(value text)
RETURNS text
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT btrim(
        replace(
            replace(
                replace(
                    replace(
                        replace(
                            coalesce(value, ''),
                            '|',
                            ' '
                        ),
                        E'\t',
                        ' '
                    ),
                    E'\n',
                    ' '
                ),
                E'\r',
                ' '
            ),
            E'\\',
            ' '
        )
    );
$$;


-- Normalize PostgreSQL text values into TigerGraph-compatible booleans.
CREATE OR REPLACE FUNCTION
tf_gnn_prep.boolean_text(value boolean)
RETURNS text
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT CASE WHEN coalesce(value, false) THEN 'true' ELSE 'false' END;
$$;


-- Epoch seconds -> milliseconds. event_ts_ms is the authoritative clock
-- in the schema ("DATETIME does not preserve sub-second ordering"), and
-- the source's resolution is whole seconds, so every millisecond value
-- in this graph ends in 000. That is a property of the corpus, not of
-- the contract: a sub-second source would flow through unchanged.
CREATE OR REPLACE FUNCTION
tf_gnn_prep.to_ms(epoch_seconds bigint)
RETURNS bigint
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT CASE
        WHEN epoch_seconds IS NULL OR epoch_seconds <= 0 THEN 0
        ELSE epoch_seconds * 1000
    END;
$$;


-- ------------------------------------------------------------------
-- PII TOKENISATION
--
-- HMAC-SHA256 over the core sha256() primitive, keyed by a salt the
-- operator supplies out of band.
--
-- THE SALT ARRIVES AS A SESSION GUC, NEVER AS A LITERAL IN THIS FILE.
-- src/tf_gnn_loader/postgres/connection.py issues
--     SET tfgnn.pii_salt = '<TFGNN_PII_SALT>'
-- on every connection, and refuses to connect when the environment
-- variable is unset. A repo-resident salt would make the tokens
-- reversible by anyone who can read the repo, which is every reader.
--
-- WHY THE PADS ARE STORED. HMAC pads the key to the 64-byte block size
-- and XORs it with two constants. Doing that per value would put a
-- 64-iteration loop on ~500,000 calls; doing it once and storing the two
-- padded keys makes tf_gnn_prep.pii_token two sha256() calls and an
-- indexed one-row lookup.
--
-- The stored pads are the salt XOR a public constant, so a reader with
-- SELECT on tf_gnn_prep can recover the salt. THAT IS ACCEPTED AND
-- DELIBERATE: tf_gnn_prep lives in the same database as card_fraud,
-- where the raw email addresses and phone numbers already are, so this
-- widens nothing. The boundary the tokens defend is the EXPORTED PSV
-- shards and the loaded graph, neither of which ever sees a raw value.
-- An operator who wants the stricter property drops tf_gnn_prep after
-- the export completes; the manifest and shards stand alone.
--
-- THE SALT IS PINNED. Changing it changes every Device, IP_Address,
-- Email, Phone, Address and Identity_Document primary id, which silently
-- re-keys the graph and orphans every previously loaded edge. The digest
-- is recorded and a mismatch is a hard failure with the remedy spelled
-- out, not a warning.
-- ------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS tf_gnn_prep.pii_token_policy (
    policy_id text PRIMARY KEY,
    algorithm text NOT NULL,
    token_hex_length integer NOT NULL,
    salt_digest text NOT NULL,
    key_ipad bytea NOT NULL,
    key_opad bytea NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT pii_token_policy_length_check
        CHECK (token_hex_length BETWEEN 16 AND 64)
);


-- Build the two padded HMAC keys once. Key material is sha256(salt), so
-- it is exactly 32 bytes and the zero-extension to 64 is unconditional.
CREATE OR REPLACE FUNCTION
tf_gnn_prep.hmac_pads(salt text)
RETURNS TABLE (key_ipad bytea, key_opad bytea)
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $hmac_pads$
DECLARE
    block_key bytea;
    ipad bytea := ''::bytea;
    opad bytea := ''::bytea;
    position_index integer;
    key_byte integer;
BEGIN
    block_key := sha256(convert_to(salt, 'UTF8'))
                 || decode(repeat('00', 32), 'hex');

    FOR position_index IN 0..63 LOOP
        key_byte := get_byte(block_key, position_index);

        -- 54 = 0x36 (ipad), 92 = 0x5c (opad); # is bitwise XOR.
        ipad := ipad || set_byte(decode('00', 'hex'), 0, key_byte # 54);
        opad := opad || set_byte(decode('00', 'hex'), 0, key_byte # 92);
    END LOOP;

    RETURN QUERY SELECT ipad, opad;
END
$hmac_pads$;


DO $pii_policy$
DECLARE
    salt text;
    digest text;
    pads record;
    existing text;
BEGIN
    salt := current_setting('tfgnn.pii_salt', true);

    IF salt IS NULL OR btrim(salt) = '' THEN
        RAISE EXCEPTION
            'tfgnn.pii_salt is not set. The TransactionFraud_GNN schema '
            'requires email, phone, address, identity-document, device '
            'and IP primary ids to be salted before loading. Export '
            'TFGNN_PII_SALT in the environment (or set it in .env) and '
            're-run `tf-gnn-load prepare`.';
    END IF;

    IF length(btrim(salt)) < 16 THEN
        RAISE EXCEPTION
            'tfgnn.pii_salt is only % characters. A short salt is '
            'dictionary-searchable, and phone numbers and email '
            'addresses carry little enough entropy that the salt is the '
            'whole defence. Use at least 16 characters.',
            length(btrim(salt));
    END IF;

    digest := encode(sha256(convert_to(salt, 'UTF8')), 'hex');

    SELECT policy.salt_digest INTO existing
    FROM tf_gnn_prep.pii_token_policy AS policy
    WHERE policy.policy_id = 'hmac_sha256_v1';

    IF existing IS NOT NULL AND existing <> digest THEN
        RAISE EXCEPTION
            'tfgnn.pii_salt does not match the salt this preparation '
            'schema was built with. Re-keying would change every '
            'Device, IP_Address, Email, Phone, Address and '
            'Identity_Document primary id, orphaning every edge already '
            'loaded into TransactionFraud_GNN. Restore the original '
            'salt, or DROP SCHEMA tf_gnn_prep CASCADE and reload the '
            'graph from scratch.';
    END IF;

    SELECT * INTO pads FROM tf_gnn_prep.hmac_pads(salt);

    INSERT INTO tf_gnn_prep.pii_token_policy (
        policy_id,
        algorithm,
        token_hex_length,
        salt_digest,
        key_ipad,
        key_opad
    )
    VALUES (
        'hmac_sha256_v1',
        'HMAC-SHA256(key = sha256(salt), message = kind || '':'' || value)',
        -- 32 hex characters = 128 bits. At ~10^6 distinct values the
        -- birthday collision probability is ~10^-27, and a 64-character
        -- primary id on every PII vertex is pure graph weight.
        32,
        digest,
        pads.key_ipad,
        pads.key_opad
    )
    ON CONFLICT (policy_id)
    DO UPDATE SET
        algorithm = EXCLUDED.algorithm,
        token_hex_length = EXCLUDED.token_hex_length,
        key_ipad = EXCLUDED.key_ipad,
        key_opad = EXCLUDED.key_opad;

    RAISE NOTICE 'PII token policy ready (salt digest %...).',
        left(digest, 12);
END
$pii_policy$;


-- kind is DOMAIN SEPARATION, not decoration. It is inside the HMAC, so
-- the same string held as a device id and as an identity-document number
-- produces two unrelated tokens and no cross-type join is possible.
--
-- The returned id is '<prefix>_<32 hex>' so a token is self-describing
-- in the graph and in an error message, and so the six id spaces cannot
-- collide even in principle.
CREATE OR REPLACE FUNCTION
tf_gnn_prep.pii_token(kind text, prefix text, value text)
RETURNS text
LANGUAGE sql
STABLE
PARALLEL SAFE
AS $$
    SELECT prefix
           || '_'
           || left(
                  encode(
                      sha256(
                          policy.key_opad
                          || sha256(
                                 policy.key_ipad
                                 || convert_to(
                                        kind || ':' || coalesce(value, ''),
                                        'UTF8'
                                    )
                             )
                      ),
                      'hex'
                  ),
                  policy.token_hex_length
              )
    FROM tf_gnn_prep.pii_token_policy AS policy
    WHERE policy.policy_id = 'hmac_sha256_v1';
$$;


-- ------------------------------------------------------------------
-- NON-PII DERIVATIONS THAT SURVIVE TOKENISATION
--
-- Email.domain is the one attribute the schema keeps alongside a
-- tokenized id, and it is kept because a domain is not personal: it is
-- the mail provider, and "this ring all signs up at the same throwaway
-- provider" is exactly the signal shared-PII topology is for. The local
-- part never leaves this database.
-- ------------------------------------------------------------------

CREATE OR REPLACE FUNCTION
tf_gnn_prep.email_domain(value text)
RETURNS text
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT CASE
        WHEN position('@' IN coalesce(value, '')) = 0 THEN ''
        ELSE lower(btrim(split_part(value, '@', 2)))
    END;
$$;


-- A postcode PREFIX, not the postcode. The schema asks for
-- postal_code_prefix on Merchant_Location and Address, which is a
-- geographic bucket rather than a locator: at US ZIP resolution the
-- first three digits are the sectional centre facility, tens of
-- thousands of addresses wide.
CREATE OR REPLACE FUNCTION
tf_gnn_prep.postal_prefix(value text)
RETURNS text
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT left(
        regexp_replace(coalesce(value, ''), '[^A-Za-z0-9]', '', 'g'),
        3
    );
$$;


-- ------------------------------------------------------------------
-- ACCOUNT DERIVATION
--
-- The source has no account table. PhantomLedger's card identifiers
-- (include/phantomledger/exporter/card_fraud/derive.hpp, cardId) are
--
--     [C|D] <rendered funding key> [ -G<generation> ]
--
-- where a 'D' card renders the DEPOSIT-ACCOUNT key and a 'C' card
-- renders the CREDIT-CARD key --- "cards and accounts are distinct id
-- spaces" --- and the -G suffix is the reissue generation, because a
-- cardholder receives replacement cards over a three-year window.
--
-- Stripping the generation suffix therefore recovers the FUNDING
-- INSTRUMENT the authorization drew on, which is exactly what the
-- schema's Account is: "each Payment_Transaction has exactly one
-- Account", meaning one initiating/funding account. The merchant is the
-- receiving side and is the Merchant endpoint; there is no second
-- Account, and a split tender would have to arrive as separate
-- authorization records.
--
-- What this buys over the obvious alternative (one Account per Party):
-- Account -> Card becomes a real one-to-many over reissue generations
-- rather than a synonym for "all this customer's cards", and
-- Account_Has_Card gets genuine valid_from/valid_to tenures --- see 050.
--
-- IF THE SOURCE EVER EXPORTS ACCOUNTS, REPLACE THIS FUNCTION AND
-- NOTHING ELSE. Every downstream view reads funding_account_id and does
-- not care how it was derived.
-- ------------------------------------------------------------------

CREATE OR REPLACE FUNCTION
tf_gnn_prep.funding_account_id(card_number text)
RETURNS text
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT regexp_replace(
        btrim(coalesce(card_number, '')),
        '-G[0-9]+$',
        ''
    );
$$;


-- 0 for the original issue, n for the nth replacement. Ordering the
-- generations is what turns Account_Has_Card into a slowly changing
-- relation with a real end: generation n's tenure closes when
-- generation n+1 starts.
CREATE OR REPLACE FUNCTION
tf_gnn_prep.card_generation(card_number text)
RETURNS integer
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT CASE
        WHEN coalesce(card_number, '') ~ '-G[0-9]+$'
        THEN (regexp_match(card_number, '-G([0-9]+)$'))[1]::integer
        ELSE 0
    END;
$$;


-- C -> credit, D -> debit, anything else -> unknown. Used for both
-- Card.card_type and Account.account_type: a credit card and its
-- account are the same liability, a debit card and its account are the
-- same deposit relationship.
CREATE OR REPLACE FUNCTION
tf_gnn_prep.instrument_type(card_number text)
RETURNS text
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT CASE left(btrim(coalesce(card_number, '')), 1)
        WHEN 'C' THEN 'credit'
        WHEN 'D' THEN 'debit'
        ELSE 'unknown'
    END;
$$;


-- ------------------------------------------------------------------
-- LABEL POLICY
--
-- PhantomLedger supplies no per-row report timestamp, so availability is
-- synthesised deterministically:
--
--   confirmed fraud   available = event time + confirmation delay
--   matured clean     available = event time + maturation window,
--                     granted only when that deadline falls inside the
--                     observed data window
--   unresolved        deadline beyond the observed window; the row is
--                     NOT a usable negative, and label_known is false
--
-- When the source grows a real report timestamp (or a fraud-typology
-- column that distinguishes scam reports from chargebacks), replace the
-- synthetic delay with the real column HERE, in one place. Do not
-- spread this decision.
-- ------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS tf_gnn_prep.label_policy (
    policy_id text PRIMARY KEY,
    fraud_confirmation_delay_seconds bigint NOT NULL,
    maturation_window_seconds bigint NOT NULL,
    description text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT label_policy_delay_check
        CHECK (fraud_confirmation_delay_seconds > 0),

    CONSTRAINT label_policy_window_check
        CHECK (maturation_window_seconds > 0)
);


INSERT INTO tf_gnn_prep.label_policy (
    policy_id,
    fraud_confirmation_delay_seconds,
    maturation_window_seconds,
    description
)
VALUES (
    'phantomledger_synthetic_v1',
    2592000,
    10368000,
    'Fraud verdicts (chargebacks and victim scam reports alike) become '
    'knowable 30 days after the authorization; negatives mature after a '
    '120-day chargeback window. Synthetic constants standing in for the '
    'per-row report timestamps the source does not carry. Written to '
    'Payment_Transaction.label_source on every row whose label is known.'
)
ON CONFLICT (policy_id)
DO UPDATE SET
    fraud_confirmation_delay_seconds =
        EXCLUDED.fraud_confirmation_delay_seconds,

    maturation_window_seconds =
        EXCLUDED.maturation_window_seconds,

    description =
        EXCLUDED.description;


-- ------------------------------------------------------------------
-- Retired objects from the TF_GNN revision of this pipeline.
--
-- Dropped rather than left in place: split_policy fed split_id and
-- causal_fold, which the new schema does not declare, and the name
-- helpers fed Full_Name_Hash, which it does not declare either. Leaving
-- them would leave a working code path to attributes the schema has
-- deliberately removed.
-- ------------------------------------------------------------------

DROP TABLE IF EXISTS tf_gnn_prep.split_policy CASCADE;
DROP TABLE IF EXISTS tf_gnn_prep.graph_policy CASCADE;
DROP TABLE IF EXISTS tf_gnn_prep.train_cards CASCADE;
DROP TABLE IF EXISTS tf_gnn_prep.train_merchants CASCADE;

DROP FUNCTION IF EXISTS tf_gnn_prep.soundex(text) CASCADE;
DROP FUNCTION IF EXISTS tf_gnn_prep.soundex_code(text) CASCADE;
DROP FUNCTION IF EXISTS tf_gnn_prep.normalize_name_part(text) CASCADE;
DROP FUNCTION IF EXISTS tf_gnn_prep.first_name_token(text) CASCADE;
DROP FUNCTION IF EXISTS tf_gnn_prep.last_name_token(text) CASCADE;
DROP FUNCTION IF EXISTS tf_gnn_prep.name_hash(text) CASCADE;
DROP FUNCTION IF EXISTS tf_gnn_prep.phone_hash(text) CASCADE;
DROP FUNCTION IF EXISTS tf_gnn_prep.email_hash(text) CASCADE;
DROP FUNCTION IF EXISTS tf_gnn_prep.boolean_text(text) CASCADE;
