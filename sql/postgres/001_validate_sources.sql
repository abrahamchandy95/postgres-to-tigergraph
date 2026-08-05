-- =====================================================================
-- SOURCE CONTRACT VALIDATION
--
-- The source is PhantomLedger's `card_fraud` export. Its authoritative
-- table list is the generator's own
-- include/phantomledger/exporter/card_fraud/schema.hpp (kTableCount = 43);
-- src/tf_gnn_loader/postgres/contract.py mirrors the subset this loader
-- reads.
--
-- WHAT CHANGED FOR TransactionFraud_GNN
-- -------------------------------------
-- Three source tables that the previous TF_GNN loader never read are now
-- REQUIRED, because the new schema has first-class homes for them:
--
--   cf_Transaction_Uses_Device -> Transaction_Used_Device
--   cf_Transaction_Uses_IP     -> Transaction_From_IP
--
-- These are the EVENT-TIME endpoints. The old loader only had the
-- whole-window cf_Has_Device / cf_Has_IP party associations, so a
-- transaction could not say which device or IP it actually came from.
-- The new schema's per-transaction device and IP edges are exactly that
-- fact, and a corpus without those tables cannot populate them.
--
-- Four source tables are deliberately NOT required any more, because the
-- attributes they fed are gone from the schema:
--
--   cf_Full_Name, cf_Has_Full_Name   Full_Name / Full_Name_Hash deleted.
--   cf_DOB, cf_Has_DOB               Birthdate deleted.
--
-- Name, gender and date of birth are PROTECTED ATTRIBUTES and the schema
-- states they are intentionally absent. Dropping them from the contract
-- is the enforcement: a column nobody validates is a column nobody can
-- accidentally load. cf_Party.name / .gender / .dob are likewise no
-- longer required columns.
--
--   cf_Email_Minhash, cf_Has_Email_Minhash, cf_Ground_Truth_Label exist
--   in the source and are NOT read here. The first two are ER blocking
--   structure the v1 schema deliberately excludes ("entity-resolution,
--   match, and unify vertices or edges are intentionally outside this
--   schema"); the third is a whole-window entity label overlay, which is
--   the exact leak the schema's feature contract forbids.
--
-- ACCOUNT HAS NO SOURCE TABLE, and this file cannot invent one. The
-- derivation lives in 050_create_entity_views.sql (funding-instrument key
-- = card id minus its -G<n> reissue suffix) and the invariant it must
-- satisfy is checked in 090_create_audit_views.sql. Recorded here so the
-- gap is visible from the contract file rather than only from the view
-- that papers over it.
-- =====================================================================

DO $validation$
DECLARE
    item record;

    transaction_count bigint;
    card_edge_count bigint;
    merchant_edge_count bigint;

    card_count bigint;
    merchant_count bigint;
    party_count bigint;

    device_edge_count bigint;
    ip_edge_count bigint;

    orphan_count bigint;
    bad_time_count bigint;
    bad_label_count bigint;
    reissue_count bigint;
BEGIN
    -- ========================================================
    -- 1. Required tables
    -- ========================================================

    FOR item IN
        SELECT *
        FROM (
            VALUES
                -- Event vertex and its endpoint relations
                ('card_fraud', 'cf_Payment_Transaction'),
                ('card_fraud', 'cf_Card_Send_Transaction'),
                ('card_fraud', 'cf_Merchant_Receive_Transaction'),

                -- Event-time endpoints (attacker-infra-2026-07). New
                -- requirement: Transaction_Used_Device and
                -- Transaction_From_IP have no other source.
                ('card_fraud', 'cf_Transaction_Uses_Device'),
                ('card_fraud', 'cf_Transaction_Uses_IP'),

                -- Core registries
                ('card_fraud', 'cf_Card'),
                ('card_fraud', 'cf_Merchant'),
                ('card_fraud', 'cf_Party'),

                -- Ownership
                ('card_fraud', 'cf_Party_Has_Card'),
                ('card_fraud', 'cf_Is_Merchant'),
                ('card_fraud', 'cf_Merchant_Assigned'),

                -- Geography. City / State / Zipcode are no longer graph
                -- vertices; they survive as the attribute source for
                -- Merchant_Location.region_code / postal_code_prefix and
                -- Address.region_code / postal_code_prefix.
                ('card_fraud', 'cf_City'),
                ('card_fraud', 'cf_State'),
                ('card_fraud', 'cf_Zipcode'),
                ('card_fraud', 'cf_Merchant_Location'),
                ('card_fraud', 'cf_Has_City'),
                ('card_fraud', 'cf_Has_State'),
                ('card_fraud', 'cf_Has_Zip'),
                ('card_fraud', 'cf_Has_Std_City'),
                ('card_fraud', 'cf_Has_Std_Postcode'),
                ('card_fraud', 'cf_Has_Std_State'),

                -- Tokenised PII value tables and their link tables
                ('card_fraud', 'cf_Address'),
                ('card_fraud', 'cf_Phone'),
                ('card_fraud', 'cf_Email'),
                ('card_fraud', 'cf_IP'),
                ('card_fraud', 'cf_Device'),
                ('card_fraud', 'cf_ID'),
                ('card_fraud', 'cf_Has_Address'),
                ('card_fraud', 'cf_Has_Phone'),
                ('card_fraud', 'cf_Has_Email'),
                ('card_fraud', 'cf_Has_IP'),
                ('card_fraud', 'cf_Has_Device'),
                ('card_fraud', 'cf_Has_ID')
        ) AS required(schema_name, table_name)
    LOOP
        IF to_regclass(
            format(
                '%I.%I',
                item.schema_name,
                item.table_name
            )
        ) IS NULL THEN
            RAISE EXCEPTION
                'Missing required table %.%. The TransactionFraud_GNN '
                'loader needs PhantomLedger''s 43-table card_fraud '
                'export; cf_Transaction_Uses_Device and '
                'cf_Transaction_Uses_IP in particular are newer than '
                'the previous TF_GNN contract.',
                item.schema_name,
                item.table_name;
        END IF;
    END LOOP;

    RAISE NOTICE 'Required table validation passed.';

    -- ========================================================
    -- 2. Required columns
    -- ========================================================

    FOR item IN
        SELECT *
        FROM (
            VALUES
                ('cf_Payment_Transaction', 'id'),
                ('cf_Payment_Transaction', 'transaction_time'),
                ('cf_Payment_Transaction', 'amount'),
                ('cf_Payment_Transaction', 'is_fraud'),
                ('cf_Payment_Transaction', 'unix_time'),
                ('cf_Payment_Transaction', 'mer_cat'),
                -- use_chip is the ONLY acceptance-environment signal the
                -- source carries and it is causal (use-chip-causal-2026-07):
                -- 050 derives channel, entry_mode and card_present from it.
                ('cf_Payment_Transaction', 'use_chip'),

                ('cf_Card_Send_Transaction', 'txn_id'),
                ('cf_Card_Send_Transaction', 'card_number'),
                ('cf_Card_Send_Transaction', 'edge_unix_time'),

                ('cf_Merchant_Receive_Transaction', 'txn_id'),
                ('cf_Merchant_Receive_Transaction', 'merchant_id'),
                ('cf_Merchant_Receive_Transaction', 'edge_unix_time'),

                ('cf_Transaction_Uses_Device', 'txn_id'),
                ('cf_Transaction_Uses_Device', 'device_id'),
                ('cf_Transaction_Uses_Device', 'edge_unix_time'),

                ('cf_Transaction_Uses_IP', 'txn_id'),
                ('cf_Transaction_Uses_IP', 'ip_id'),
                ('cf_Transaction_Uses_IP', 'edge_unix_time'),

                ('cf_Card', 'card_number'),
                ('cf_Merchant', 'id'),

                -- NO name / gender / dob. See the header: they are
                -- protected attributes and the schema has no home for
                -- them, so requiring them would keep them alive in the
                -- contract for no consumer.
                ('cf_Party', 'id'),
                ('cf_Party', 'party_type'),
                ('cf_Party', 'created_at'),

                ('cf_Party_Has_Card', 'party_id'),
                ('cf_Party_Has_Card', 'card_number'),
                ('cf_Is_Merchant', 'merchant_id'),
                ('cf_Is_Merchant', 'party_id'),
                ('cf_Merchant_Assigned', 'merchant_id'),
                ('cf_Merchant_Assigned', 'category'),

                ('cf_City', 'id'),
                ('cf_City', 'city'),
                ('cf_City', 'lat'),
                ('cf_City', 'lon'),
                ('cf_State', 'id'),
                ('cf_Zipcode', 'id'),
                ('cf_Zipcode', 'lat'),
                ('cf_Zipcode', 'lon'),
                ('cf_Merchant_Location', 'merchant_id'),
                ('cf_Merchant_Location', 'lat'),
                ('cf_Merchant_Location', 'lon'),

                ('cf_Has_City', 'merchant_id'),
                ('cf_Has_City', 'city_id'),
                ('cf_Has_State', 'merchant_id'),
                ('cf_Has_State', 'state_id'),
                ('cf_Has_Zip', 'merchant_id'),
                ('cf_Has_Zip', 'zipcode_id'),

                -- since_unix_time is REQUIRED (relocation-2026-07): it is
                -- the only real attachment time in the whole source, and
                -- 060 uses it to pick the tenure that produced an
                -- Address's region/postcode snapshot.
                ('cf_Has_Std_City', 'party_id'),
                ('cf_Has_Std_City', 'city_id'),
                ('cf_Has_Std_City', 'since_unix_time'),
                ('cf_Has_Std_Postcode', 'party_id'),
                ('cf_Has_Std_Postcode', 'zipcode_id'),
                ('cf_Has_Std_Postcode', 'since_unix_time'),
                ('cf_Has_Std_State', 'party_id'),
                ('cf_Has_Std_State', 'state_id'),
                ('cf_Has_Std_State', 'since_unix_time'),

                ('cf_Address', 'address'),
                ('cf_Phone', 'phone_number'),
                ('cf_Email', 'email'),
                ('cf_IP', 'id'),
                ('cf_Device', 'id'),
                ('cf_ID', 'id'),
                ('cf_ID', 'id_type'),

                ('cf_Has_Address', 'party_id'),
                ('cf_Has_Address', 'address'),
                ('cf_Has_Phone', 'party_id'),
                ('cf_Has_Phone', 'phone_number'),
                ('cf_Has_Email', 'party_id'),
                ('cf_Has_Email', 'email'),
                ('cf_Has_IP', 'party_id'),
                ('cf_Has_IP', 'ip_id'),
                ('cf_Has_Device', 'party_id'),
                ('cf_Has_Device', 'device_id'),
                ('cf_Has_ID', 'party_id'),
                ('cf_Has_ID', 'id')
        ) AS required(table_name, column_name)
    LOOP
        IF NOT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'card_fraud'
              AND table_name = item.table_name
              AND column_name = item.column_name
        ) THEN
            RAISE EXCEPTION
                'Missing required column card_fraud.%.%',
                item.table_name,
                item.column_name;
        END IF;
    END LOOP;

    RAISE NOTICE 'Required column validation passed.';

    -- ========================================================
    -- 3. Non-empty core relations
    --
    -- A zero-row core table exports a zero-row dataset, which then
    -- verifies as expected == actual == 0 and the missing data is never
    -- flagged. Catch it at the source instead.
    -- ========================================================

    SELECT count(*) INTO transaction_count
    FROM card_fraud."cf_Payment_Transaction";

    SELECT count(*) INTO card_edge_count
    FROM card_fraud."cf_Card_Send_Transaction";

    SELECT count(*) INTO merchant_edge_count
    FROM card_fraud."cf_Merchant_Receive_Transaction";

    SELECT count(*) INTO card_count
    FROM card_fraud."cf_Card";

    SELECT count(*) INTO merchant_count
    FROM card_fraud."cf_Merchant";

    SELECT count(*) INTO party_count
    FROM card_fraud."cf_Party";

    IF transaction_count = 0 THEN
        RAISE EXCEPTION 'cf_Payment_Transaction is empty';
    END IF;

    IF card_count = 0 THEN
        RAISE EXCEPTION 'cf_Card is empty';
    END IF;

    IF merchant_count = 0 THEN
        RAISE EXCEPTION 'cf_Merchant is empty';
    END IF;

    IF party_count = 0 THEN
        RAISE EXCEPTION 'cf_Party is empty';
    END IF;

    -- ========================================================
    -- 4. THE CARDINALITY CONTRACT, at the source
    --
    -- "Each Payment_Transaction has exactly one Account and Merchant,
    --  and at most one Card." Account and Card are derived from the SAME
    --  cf_Card_Send_Transaction row (Account is the card id minus its
    --  reissue suffix), so exactly-one-card is what makes both true. It
    --  is asserted here, before 27M rows are exported, rather than only
    --  after they reach the graph.
    -- ========================================================

    IF card_edge_count <> transaction_count THEN
        RAISE EXCEPTION
            'cf_Card_Send_Transaction has % rows for % transactions; '
            'the schema requires exactly one initiating/funding Account '
            'per authorization and that Account is derived from this '
            'edge.',
            card_edge_count,
            transaction_count;
    END IF;

    IF merchant_edge_count <> transaction_count THEN
        RAISE EXCEPTION
            'cf_Merchant_Receive_Transaction has % rows for % '
            'transactions; the schema requires exactly one receiving '
            'Merchant per authorization.',
            merchant_edge_count,
            transaction_count;
    END IF;

    SELECT count(*) INTO orphan_count
    FROM card_fraud."cf_Card_Send_Transaction" AS edge
    LEFT JOIN card_fraud."cf_Card" AS card
      ON card.card_number = edge.card_number
    WHERE card.card_number IS NULL;

    IF orphan_count <> 0 THEN
        RAISE EXCEPTION
            '% cf_Card_Send_Transaction rows reference a card_number '
            'absent from cf_Card. Every Card and every derived Account '
            'must exist before the fact table loads, or TigerGraph '
            'upserts a zero-stamped vertex that passes every '
            'first_seen_seq cutoff.',
            orphan_count;
    END IF;

    SELECT count(*) INTO orphan_count
    FROM card_fraud."cf_Merchant_Receive_Transaction" AS edge
    LEFT JOIN card_fraud."cf_Merchant" AS merchant
      ON merchant.id = edge.merchant_id
    WHERE merchant.id IS NULL;

    IF orphan_count <> 0 THEN
        RAISE EXCEPTION
            '% cf_Merchant_Receive_Transaction rows reference a '
            'merchant absent from cf_Merchant.',
            orphan_count;
    END IF;

    -- Device and IP are OPTIONAL endpoints ("at most one canonical
    -- Device and canonical source IP_Address"), so zero rows is legal and
    -- is reported rather than raised. More than one row per transaction
    -- is NOT legal: canonical means one.

    SELECT count(*) INTO device_edge_count
    FROM card_fraud."cf_Transaction_Uses_Device";

    SELECT count(*) INTO ip_edge_count
    FROM card_fraud."cf_Transaction_Uses_IP";

    SELECT count(*) INTO orphan_count
    FROM (
        SELECT txn_id
        FROM card_fraud."cf_Transaction_Uses_Device"
        GROUP BY txn_id
        HAVING count(*) > 1
    ) AS multi;

    IF orphan_count <> 0 THEN
        RAISE EXCEPTION
            '% transactions carry more than one cf_Transaction_Uses_Device '
            'row. Transaction_Used_Device is at-most-one (canonical '
            'device); a multi-device row set needs a session relation, '
            'not this edge.',
            orphan_count;
    END IF;

    SELECT count(*) INTO orphan_count
    FROM (
        SELECT txn_id
        FROM card_fraud."cf_Transaction_Uses_IP"
        GROUP BY txn_id
        HAVING count(*) > 1
    ) AS multi;

    IF orphan_count <> 0 THEN
        RAISE EXCEPTION
            '% transactions carry more than one cf_Transaction_Uses_IP '
            'row. Transaction_From_IP is at-most-one (canonical source '
            'IP).',
            orphan_count;
    END IF;

    IF device_edge_count = 0 THEN
        RAISE NOTICE
            'cf_Transaction_Uses_Device is empty: Transaction_Used_Device '
            'will not load. Legal (optional endpoint) but it removes the '
            'account-takeover arm entirely.';
    END IF;

    IF ip_edge_count = 0 THEN
        RAISE NOTICE
            'cf_Transaction_Uses_IP is empty: Transaction_From_IP will '
            'not load. Legal (optional endpoint) but it removes the '
            'source-IP arm entirely.';
    END IF;

    -- ========================================================
    -- 5. THE TEMPORAL CONTRACT, at the source
    --
    -- event_ts_ms is authoritative and event_seq is ordered by
    -- (event_ts_ms, transaction_id). Both derive from unix_time, so
    -- unix_time must be a usable positive epoch on every row and
    -- transaction_time must agree with it read as UTC — otherwise the
    -- DATETIME the schema keeps "for operator readability" disagrees
    -- with the clock every query uses.
    -- ========================================================

    SELECT count(*) INTO bad_time_count
    FROM card_fraud."cf_Payment_Transaction"
    WHERE unix_time IS NULL
       OR btrim(unix_time::text) = ''
       OR unix_time::bigint <= 0;

    IF bad_time_count <> 0 THEN
        RAISE EXCEPTION
            '% cf_Payment_Transaction rows have a null, empty or '
            'non-positive unix_time. event_ts_ms and event_seq both '
            'derive from it.',
            bad_time_count;
    END IF;

    SELECT count(*) INTO bad_time_count
    FROM card_fraud."cf_Payment_Transaction"
    WHERE floor(
              extract(
                  epoch FROM (transaction_time::timestamp AT TIME ZONE 'UTC')
              )
          )::bigint
          <> unix_time::bigint;

    IF bad_time_count <> 0 THEN
        RAISE EXCEPTION
            '% cf_Payment_Transaction rows have a transaction_time that '
            'does not equal unix_time read as UTC. event_time is '
            're-derived from unix_time at export, so the two must agree '
            'or the readable column would contradict the clock.',
            bad_time_count;
    END IF;

    SELECT count(*) INTO orphan_count
    FROM (
        SELECT id
        FROM card_fraud."cf_Payment_Transaction"
        GROUP BY id
        HAVING count(*) > 1
    ) AS duplicated;

    IF orphan_count <> 0 THEN
        RAISE EXCEPTION
            '% duplicate cf_Payment_Transaction.id values. event_seq is '
            'ordered by (unix_time, id) and must be a dense permutation.',
            orphan_count;
    END IF;

    -- ========================================================
    -- 6. Label domain
    --
    -- fraud_label is INT DEFAULT -1 in the schema and label_known is the
    -- mask, so a value outside {0, 1} at the source would load as a third
    -- silent class.
    -- ========================================================

    SELECT count(*) INTO bad_label_count
    FROM card_fraud."cf_Payment_Transaction"
    WHERE is_fraud IS NULL
       OR is_fraud::integer NOT IN (0, 1);

    IF bad_label_count <> 0 THEN
        RAISE EXCEPTION
            '% cf_Payment_Transaction rows carry an is_fraud outside '
            '{0, 1}.',
            bad_label_count;
    END IF;

    -- ========================================================
    -- 7. The Account derivation's one source-side premise
    --
    -- account_id = card_number with a trailing -G<n> reissue suffix
    -- removed (PhantomLedger derive.hpp: a card id is
    -- [C|D]<rendered funding key>[-G<generation>]). If NO card id
    -- carries the C/D prefix the derivation is being applied to an
    -- identifier scheme it was not written for, and every Account would
    -- silently equal its Card.
    -- ========================================================

    SELECT count(*) INTO orphan_count
    FROM card_fraud."cf_Card"
    WHERE card_number !~ '^[CD]';

    IF orphan_count = card_count THEN
        RAISE EXCEPTION
            'No cf_Card.card_number carries the C/D funding-instrument '
            'prefix. account_id is derived from the card id by stripping '
            'the -G<n> reissue suffix, which assumes PhantomLedger''s '
            'derive.hpp identifier scheme. Re-derive Account in '
            '050_create_entity_views.sql before loading.';
    END IF;

    IF orphan_count <> 0 THEN
        RAISE NOTICE
            '% of % cf_Card rows have no C/D prefix; their '
            'Account.account_type loads as "unknown".',
            orphan_count,
            card_count;
    END IF;

    SELECT count(*) INTO reissue_count
    FROM card_fraud."cf_Card"
    WHERE card_number ~ '-G[0-9]+$';

    RAISE NOTICE
        'Source contract validated: % transactions, % cards '
        '(% reissued generations), % merchants, % parties, '
        '% event-time device rows, % event-time IP rows.',
        transaction_count,
        card_count,
        reissue_count,
        merchant_count,
        party_count,
        device_edge_count,
        ip_edge_count;
END
$validation$;
