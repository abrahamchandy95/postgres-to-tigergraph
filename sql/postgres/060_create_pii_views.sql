-- =====================================================================
-- TOKENISED PII VALUE VERTICES AND THEIR RELATIONS
--
-- The schema's privacy contract, verbatim:
--
--   Email, phone, address, identity-document, device and IP primary IDs
--   must be salted/tokenized before loading. Raw PII is not stored. IDs
--   are join keys, never numeric features.
--
-- THE ENFORCEMENT IS STRUCTURAL, NOT EDITORIAL. Each type gets a
-- MATERIALISED value table below holding the raw value (as the join key
-- inside PostgreSQL) beside its token. Every downstream view --- the
-- vertex loads, the party relations, and the 27M-row transaction
-- endpoint relations in 070 --- joins these tables and selects the TOKEN
-- column. No load view in 080 references a cf_* PII column directly, and
-- 090 asserts that by inspecting the load views' own column lists.
--
-- Materialising also bounds the cost. The HMAC runs once per DISTINCT
-- value (order 5x10^5 here) instead of once per referencing row (order
-- 5x10^7 once Transaction_Used_Device and Transaction_From_IP are
-- counted).
--
-- ---------------------------------------------------------------------
-- WHAT IS GONE, AND WHY IT IS NOT A LOSS
-- ---------------------------------------------------------------------
-- Full_Name, Full_Name_Hash, Birthdate, Phone_Hash and
-- Email_Address_Hash all existed to serve entity resolution: an exact
-- vertex for jaroWinklerDistance and a hash vertex for blocking. The new
-- schema declares none of them, and says why: "entity-resolution, match,
-- and unify vertices or edges are intentionally outside this schema and
-- may be added later as an optional, separately versioned module."
--
-- Name and date of birth are also protected attributes. Removing the
-- vertex removes the temptation; there is no view here that could be
-- switched back on.
--
-- What survives is SHARED-VALUE TOPOLOGY, which is what the schema says
-- is sufficient for v1: two parties holding the same tokenized phone are
-- two hops apart through that Phone vertex, and the token preserves the
-- equality that makes the path exist while destroying the value that
-- makes it personal.
--
-- ---------------------------------------------------------------------
-- WHICH VALUES LOAD
-- ---------------------------------------------------------------------
-- Email / Phone / Address / Identity_Document: values referenced by at
-- least one party relation. A registry value nobody holds would be an
-- isolated vertex; 090 counts the exclusions so they are measured.
--
-- Device / IP_Address: the UNION of values a party holds and values a
-- TRANSACTION used. That union is load-bearing. PhantomLedger's
-- attacker-infra round made registry coverage deliberately partial, so an
-- endpoint that transacts with no party on file is not a data defect ---
-- it is the unenrolled endpoint, and dropping it would delete exactly the
-- rows an account-takeover arm is about.
-- =====================================================================

DROP TABLE IF EXISTS tf_gnn_prep.email_values CASCADE;
DROP TABLE IF EXISTS tf_gnn_prep.phone_values CASCADE;
DROP TABLE IF EXISTS tf_gnn_prep.address_values CASCADE;
DROP TABLE IF EXISTS tf_gnn_prep.document_values CASCADE;
DROP TABLE IF EXISTS tf_gnn_prep.device_values CASCADE;
DROP TABLE IF EXISTS tf_gnn_prep.ip_values CASCADE;

DROP VIEW IF EXISTS tf_gnn_prep.loaded_party_has_email CASCADE;
DROP VIEW IF EXISTS tf_gnn_prep.loaded_party_has_phone CASCADE;
DROP VIEW IF EXISTS tf_gnn_prep.loaded_party_has_address CASCADE;
DROP VIEW IF EXISTS tf_gnn_prep.loaded_party_has_identity_document CASCADE;
DROP VIEW IF EXISTS tf_gnn_prep.loaded_party_has_device CASCADE;
DROP VIEW IF EXISTS tf_gnn_prep.loaded_party_has_ip CASCADE;

-- Retired with the vertex types the new schema deletes.
DROP VIEW IF EXISTS tf_gnn_prep.loaded_full_names CASCADE;
DROP VIEW IF EXISTS tf_gnn_prep.loaded_full_name_hashes CASCADE;
DROP VIEW IF EXISTS tf_gnn_prep.loaded_birthdates CASCADE;
DROP VIEW IF EXISTS tf_gnn_prep.loaded_phone_hashes CASCADE;
DROP VIEW IF EXISTS tf_gnn_prep.loaded_email_address_hashes CASCADE;
DROP VIEW IF EXISTS tf_gnn_prep.loaded_street_addresses CASCADE;
DROP VIEW IF EXISTS tf_gnn_prep.loaded_phones CASCADE;
DROP VIEW IF EXISTS tf_gnn_prep.loaded_email_addresses CASCADE;
DROP VIEW IF EXISTS tf_gnn_prep.loaded_ips CASCADE;
DROP VIEW IF EXISTS tf_gnn_prep.loaded_devices CASCADE;
DROP VIEW IF EXISTS tf_gnn_prep.loaded_identity_documents CASCADE;
DROP VIEW IF EXISTS tf_gnn_prep.loaded_party_has_street_address CASCADE;
DROP VIEW IF EXISTS tf_gnn_prep.loaded_party_has_phone_hash CASCADE;
DROP VIEW IF EXISTS tf_gnn_prep.loaded_party_has_email_address CASCADE;
DROP VIEW IF EXISTS tf_gnn_prep.loaded_party_has_email_address_hash CASCADE;
DROP VIEW IF EXISTS tf_gnn_prep.loaded_party_has_name CASCADE;
DROP VIEW IF EXISTS tf_gnn_prep.loaded_party_has_name_hash CASCADE;
DROP VIEW IF EXISTS tf_gnn_prep.loaded_party_has_birthdate CASCADE;


-- ---------------------------------------------------------------
-- EMAIL
--
-- domain is the one attribute kept alongside the token, and it is not
-- personal: it identifies the mail provider. "This group all signs up at
-- the same throwaway provider" is precisely the kind of shared-value
-- signal the schema keeps topology for. The local part never leaves this
-- database.
-- ---------------------------------------------------------------
CREATE UNLOGGED TABLE tf_gnn_prep.email_values AS
SELECT
    value.email AS email_raw,

    tf_gnn_prep.pii_token('email', 'eml', value.email) AS email_id,

    tf_gnn_prep.clean_text(tf_gnn_prep.email_domain(value.email))
        AS domain,

    coalesce(holder.first_seen_event_seq, floor_stamp.first_seen_event_seq)
        AS first_seen_event_seq,

    coalesce(holder.first_seen_unix_time, floor_stamp.first_seen_unix_time)
        AS first_seen_unix_time

FROM (
    SELECT DISTINCT edge.email
    FROM card_fraud."cf_Has_Email" AS edge
    WHERE btrim(coalesce(edge.email, '')) <> ''
) AS value

LEFT JOIN (
    SELECT
        edge.email,
        min(party.first_seen_event_seq) AS first_seen_event_seq,
        min(party.first_seen_unix_time) AS first_seen_unix_time
    FROM card_fraud."cf_Has_Email" AS edge
    JOIN tf_gnn_prep.party_first_seen AS party
      ON party.party_id = edge.party_id
    GROUP BY edge.email
) AS holder
  ON holder.email = value.email

CROSS JOIN tf_gnn_prep.first_seen_floor AS floor_stamp;

ALTER TABLE tf_gnn_prep.email_values ADD PRIMARY KEY (email_raw);
CREATE UNIQUE INDEX email_values_token_uq
    ON tf_gnn_prep.email_values (email_id);


-- ---------------------------------------------------------------
-- PHONE
--
-- country_code and line_type have no source. A leading '+' would not
-- settle the country either --- calling codes are one to three digits and
-- the split is ambiguous without a prefix table --- so both take the
-- schema's own "" / "unknown" defaults rather than a parse that is right
-- most of the time and silently wrong for the rest.
-- ---------------------------------------------------------------
CREATE UNLOGGED TABLE tf_gnn_prep.phone_values AS
SELECT
    value.phone_number AS phone_raw,

    tf_gnn_prep.pii_token('phone', 'phn', value.phone_number) AS phone_id,

    '' AS country_code,
    'unknown' AS line_type,

    coalesce(holder.first_seen_event_seq, floor_stamp.first_seen_event_seq)
        AS first_seen_event_seq,

    coalesce(holder.first_seen_unix_time, floor_stamp.first_seen_unix_time)
        AS first_seen_unix_time

FROM (
    SELECT DISTINCT edge.phone_number
    FROM card_fraud."cf_Has_Phone" AS edge
    WHERE btrim(coalesce(edge.phone_number, '')) <> ''
) AS value

LEFT JOIN (
    SELECT
        edge.phone_number,
        min(party.first_seen_event_seq) AS first_seen_event_seq,
        min(party.first_seen_unix_time) AS first_seen_unix_time
    FROM card_fraud."cf_Has_Phone" AS edge
    JOIN tf_gnn_prep.party_first_seen AS party
      ON party.party_id = edge.party_id
    GROUP BY edge.phone_number
) AS holder
  ON holder.phone_number = value.phone_number

CROSS JOIN tf_gnn_prep.first_seen_floor AS floor_stamp;

ALTER TABLE tf_gnn_prep.phone_values ADD PRIMARY KEY (phone_raw);
CREATE UNIQUE INDEX phone_values_token_uq
    ON tf_gnn_prep.phone_values (phone_id);


-- ---------------------------------------------------------------
-- ADDRESS
--
-- region_code and postal_code_prefix come from the holding parties'
-- earliest home tenure (040). One address string can be held by several
-- parties, so the area has to be RESOLVED rather than picked:
--
--   exactly one distinct (region, postcode prefix) among the holders
--       -> use it. Coresidents share an area, so this is the ordinary
--          case.
--   more than one
--       -> the address string is not a single place. MASK IT. Picking
--          one, or blending them, invents a location neither party has.
--   no holder with a home area
--       -> mask.
--
-- distinct_area_count is carried through so 090 can count the
-- masked-because-ambiguous cases; an empty region_code from ambiguity
-- and one from a party with no home area are otherwise identical.
--
-- No coordinates. The old Street_Address carried lat/lon as the
-- cardholder end of a distance feature; the new Address declares no
-- coordinate columns at all, so the postcode centroid has nowhere to go
-- and is not computed.
-- ---------------------------------------------------------------
CREATE UNLOGGED TABLE tf_gnn_prep.address_values AS
SELECT
    value.address AS address_raw,

    tf_gnn_prep.pii_token('address', 'adr', value.address) AS address_id,

    '' AS country_code,

    CASE
        WHEN coalesce(resolved.distinct_areas, 0) = 1
        THEN tf_gnn_prep.clean_text(resolved.region_code)
        ELSE ''
    END AS region_code,

    CASE
        WHEN coalesce(resolved.distinct_areas, 0) = 1
        THEN tf_gnn_prep.postal_prefix(resolved.zipcode_id)
        ELSE ''
    END AS postal_code_prefix,

    coalesce(resolved.distinct_areas, 0) AS distinct_area_count,

    coalesce(holder.first_seen_event_seq, floor_stamp.first_seen_event_seq)
        AS first_seen_event_seq,

    coalesce(holder.first_seen_unix_time, floor_stamp.first_seen_unix_time)
        AS first_seen_unix_time

FROM (
    SELECT DISTINCT edge.address
    FROM card_fraud."cf_Has_Address" AS edge
    WHERE btrim(coalesce(edge.address, '')) <> ''
) AS value

LEFT JOIN (
    SELECT
        edge.address,
        min(party.first_seen_event_seq) AS first_seen_event_seq,
        min(party.first_seen_unix_time) AS first_seen_unix_time
    FROM card_fraud."cf_Has_Address" AS edge
    JOIN tf_gnn_prep.party_first_seen AS party
      ON party.party_id = edge.party_id
    GROUP BY edge.address
) AS holder
  ON holder.address = value.address

LEFT JOIN (
    SELECT
        edge.address,
        count(DISTINCT (home.state_id, home.zipcode_id)) AS distinct_areas,
        -- Only read when distinct_areas = 1, where min() IS the value.
        min(home.state_id) AS region_code,
        min(home.zipcode_id) AS zipcode_id
    FROM card_fraud."cf_Has_Address" AS edge
    JOIN tf_gnn_prep.party_home_area AS home
      ON home.party_id = edge.party_id
    WHERE home.state_id IS NOT NULL
       OR home.zipcode_id IS NOT NULL
    GROUP BY edge.address
) AS resolved
  ON resolved.address = value.address

CROSS JOIN tf_gnn_prep.first_seen_floor AS floor_stamp;

ALTER TABLE tf_gnn_prep.address_values ADD PRIMARY KEY (address_raw);
CREATE UNIQUE INDEX address_values_token_uq
    ON tf_gnn_prep.address_values (address_id);


-- ---------------------------------------------------------------
-- IDENTITY DOCUMENT
--
-- document_type is cf_ID.id_type and is not itself identifying: it says
-- passport or driving licence, not which one. issuing_country has no
-- source (040).
-- ---------------------------------------------------------------
CREATE UNLOGGED TABLE tf_gnn_prep.document_values AS
SELECT
    value.id AS document_raw,

    tf_gnn_prep.pii_token('document', 'doc', value.id) AS document_id,

    tf_gnn_prep.clean_text(
        lower(btrim(coalesce(document.id_type, 'unknown')))
    ) AS document_type,

    '' AS issuing_country,

    coalesce(holder.first_seen_event_seq, floor_stamp.first_seen_event_seq)
        AS first_seen_event_seq,

    coalesce(holder.first_seen_unix_time, floor_stamp.first_seen_unix_time)
        AS first_seen_unix_time

FROM (
    SELECT DISTINCT edge.id
    FROM card_fraud."cf_Has_ID" AS edge
    WHERE btrim(coalesce(edge.id, '')) <> ''
) AS value

LEFT JOIN card_fraud."cf_ID" AS document
  ON document.id = value.id

LEFT JOIN (
    SELECT
        edge.id,
        min(party.first_seen_event_seq) AS first_seen_event_seq,
        min(party.first_seen_unix_time) AS first_seen_unix_time
    FROM card_fraud."cf_Has_ID" AS edge
    JOIN tf_gnn_prep.party_first_seen AS party
      ON party.party_id = edge.party_id
    GROUP BY edge.id
) AS holder
  ON holder.id = value.id

CROSS JOIN tf_gnn_prep.first_seen_floor AS floor_stamp;

ALTER TABLE tf_gnn_prep.document_values ADD PRIMARY KEY (document_raw);
CREATE UNIQUE INDEX document_values_token_uq
    ON tf_gnn_prep.document_values (document_id);


-- ---------------------------------------------------------------
-- DEVICE
--
-- device_type, os_family and browser_family have no source and take
-- "unknown". They are also exactly the attributes the schema's
-- immutability rule is aimed at: "a mutable observation (for example a
-- later device OS ...) belongs on the contemporaneous
-- Payment_Transaction ..., not in-place here". If the source ever
-- carries them, they must be the FIRST-OBSERVED values, not the current
-- ones.
--
-- The value universe is the union of held and transacted, and
-- device_first_seen (030) already spans both.
-- ---------------------------------------------------------------
CREATE UNLOGGED TABLE tf_gnn_prep.device_values AS
SELECT
    value.device_raw,

    tf_gnn_prep.pii_token('device', 'dev', value.device_raw) AS device_id,

    'unknown' AS device_type,
    'unknown' AS os_family,
    'unknown' AS browser_family,

    coalesce(seen.first_seen_event_seq, floor_stamp.first_seen_event_seq)
        AS first_seen_event_seq,

    coalesce(seen.first_seen_unix_time, floor_stamp.first_seen_unix_time)
        AS first_seen_unix_time,

    value.held_by_party,
    value.used_by_transaction

FROM (
    SELECT
        device_raw,
        bool_or(held) AS held_by_party,
        bool_or(used) AS used_by_transaction
    FROM (
        SELECT edge.device_id AS device_raw, true AS held, false AS used
        FROM card_fraud."cf_Has_Device" AS edge
        WHERE btrim(coalesce(edge.device_id, '')) <> ''

        UNION ALL

        SELECT edge.device_id AS device_raw, false AS held, true AS used
        FROM card_fraud."cf_Transaction_Uses_Device" AS edge
        WHERE btrim(coalesce(edge.device_id, '')) <> ''
    ) AS observations
    GROUP BY device_raw
) AS value

LEFT JOIN tf_gnn_prep.device_first_seen AS seen
  ON seen.device_id = value.device_raw

CROSS JOIN tf_gnn_prep.first_seen_floor AS floor_stamp;

ALTER TABLE tf_gnn_prep.device_values ADD PRIMARY KEY (device_raw);
CREATE UNIQUE INDEX device_values_token_uq
    ON tf_gnn_prep.device_values (device_id);


-- ---------------------------------------------------------------
-- IP ADDRESS
--
-- asn is -1 = unavailable and network_type is "unknown"; the source
-- carries neither. country_code is empty for the reason in 040.
--
-- cf_IP.is_blocked is DELIBERATELY NOT READ, and the new schema declares
-- no attribute it could reach. A timeless blocklist boolean is a label
-- leak: entries are written AFTER fraud is confirmed, so the flag is a
-- function of the label and will look like the best feature in the
-- graph. If a real block TIMESTAMP ever arrives it belongs on a
-- separately versioned fact gated on
--     blocked_seq > 0 AND blocked_seq < seed.event_seq
-- and never as a boolean on this vertex.
-- ---------------------------------------------------------------
CREATE UNLOGGED TABLE tf_gnn_prep.ip_values AS
SELECT
    value.ip_raw,

    tf_gnn_prep.pii_token('ip', 'ip', value.ip_raw) AS ip_id,

    '' AS country_code,
    -1 AS asn,
    'unknown' AS network_type,

    coalesce(seen.first_seen_event_seq, floor_stamp.first_seen_event_seq)
        AS first_seen_event_seq,

    coalesce(seen.first_seen_unix_time, floor_stamp.first_seen_unix_time)
        AS first_seen_unix_time,

    value.held_by_party,
    value.used_by_transaction

FROM (
    SELECT
        ip_raw,
        bool_or(held) AS held_by_party,
        bool_or(used) AS used_by_transaction
    FROM (
        SELECT edge.ip_id AS ip_raw, true AS held, false AS used
        FROM card_fraud."cf_Has_IP" AS edge
        WHERE btrim(coalesce(edge.ip_id, '')) <> ''

        UNION ALL

        SELECT edge.ip_id AS ip_raw, false AS held, true AS used
        FROM card_fraud."cf_Transaction_Uses_IP" AS edge
        WHERE btrim(coalesce(edge.ip_id, '')) <> ''
    ) AS observations
    GROUP BY ip_raw
) AS value

LEFT JOIN tf_gnn_prep.ip_first_seen AS seen
  ON seen.ip_id = value.ip_raw

CROSS JOIN tf_gnn_prep.first_seen_floor AS floor_stamp;

ALTER TABLE tf_gnn_prep.ip_values ADD PRIMARY KEY (ip_raw);
CREATE UNIQUE INDEX ip_values_token_uq
    ON tf_gnn_prep.ip_values (ip_id);


-- ---------------------------------------------------------------
-- PARTY -> VALUE RELATIONS
--
-- valid_from_seq is max(first_seen of the two endpoints): the
-- observability lower bound described at the top of 050. valid_to_seq is
-- 0 --- the source never records a PII value being given up, and
-- inventing an end would silently hide the value from later seeds.
--
-- Every one of these joins the materialised value table and selects the
-- TOKEN. None of them can emit a raw value.
-- ---------------------------------------------------------------

CREATE VIEW
tf_gnn_prep.loaded_party_has_email AS
SELECT DISTINCT
    edge.party_id,
    value.email_id,
    greatest(party.first_seen_event_seq, value.first_seen_event_seq)
        AS valid_from_seq,
    0::bigint AS valid_to_seq
FROM card_fraud."cf_Has_Email" AS edge
JOIN tf_gnn_prep.email_values AS value
  ON value.email_raw = edge.email
JOIN tf_gnn_prep.party_first_seen AS party
  ON party.party_id = edge.party_id;


CREATE VIEW
tf_gnn_prep.loaded_party_has_phone AS
SELECT DISTINCT
    edge.party_id,
    value.phone_id,
    greatest(party.first_seen_event_seq, value.first_seen_event_seq)
        AS valid_from_seq,
    0::bigint AS valid_to_seq
FROM card_fraud."cf_Has_Phone" AS edge
JOIN tf_gnn_prep.phone_values AS value
  ON value.phone_raw = edge.phone_number
JOIN tf_gnn_prep.party_first_seen AS party
  ON party.party_id = edge.party_id;


CREATE VIEW
tf_gnn_prep.loaded_party_has_address AS
SELECT DISTINCT
    edge.party_id,
    value.address_id,
    greatest(party.first_seen_event_seq, value.first_seen_event_seq)
        AS valid_from_seq,
    0::bigint AS valid_to_seq
FROM card_fraud."cf_Has_Address" AS edge
JOIN tf_gnn_prep.address_values AS value
  ON value.address_raw = edge.address
JOIN tf_gnn_prep.party_first_seen AS party
  ON party.party_id = edge.party_id;


CREATE VIEW
tf_gnn_prep.loaded_party_has_identity_document AS
SELECT DISTINCT
    edge.party_id,
    value.document_id,
    greatest(party.first_seen_event_seq, value.first_seen_event_seq)
        AS valid_from_seq,
    0::bigint AS valid_to_seq
FROM card_fraud."cf_Has_ID" AS edge
JOIN tf_gnn_prep.document_values AS value
  ON value.document_raw = edge.id
JOIN tf_gnn_prep.party_first_seen AS party
  ON party.party_id = edge.party_id;


CREATE VIEW
tf_gnn_prep.loaded_party_has_device AS
SELECT DISTINCT
    edge.party_id,
    value.device_id,
    greatest(party.first_seen_event_seq, value.first_seen_event_seq)
        AS valid_from_seq,
    0::bigint AS valid_to_seq
FROM card_fraud."cf_Has_Device" AS edge
JOIN tf_gnn_prep.device_values AS value
  ON value.device_raw = edge.device_id
JOIN tf_gnn_prep.party_first_seen AS party
  ON party.party_id = edge.party_id;


CREATE VIEW
tf_gnn_prep.loaded_party_has_ip AS
SELECT DISTINCT
    edge.party_id,
    value.ip_id,
    greatest(party.first_seen_event_seq, value.first_seen_event_seq)
        AS valid_from_seq,
    0::bigint AS valid_to_seq
FROM card_fraud."cf_Has_IP" AS edge
JOIN tf_gnn_prep.ip_values AS value
  ON value.ip_raw = edge.ip_id
JOIN tf_gnn_prep.party_first_seen AS party
  ON party.party_id = edge.party_id;


ANALYZE tf_gnn_prep.email_values;
ANALYZE tf_gnn_prep.phone_values;
ANALYZE tf_gnn_prep.address_values;
ANALYZE tf_gnn_prep.document_values;
ANALYZE tf_gnn_prep.device_values;
ANALYZE tf_gnn_prep.ip_values;
