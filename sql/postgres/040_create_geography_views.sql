-- =====================================================================
-- GEOGRAPHY
--
-- City, State and Zipcode ARE NO LONGER GRAPH VERTICES. The new schema
-- deletes all three and folds geography into attributes:
--
--   Merchant_Location(country_code, region_code, postal_code_prefix,
--                     latitude, longitude, has_coordinates)
--   Address(country_code, region_code, postal_code_prefix)
--
-- The cf_City / cf_State / cf_Zipcode tables are still READ --- they are
-- where the region code, the postcode and the centroids come from --- but
-- nothing derived from them becomes a vertex or an edge, so the
-- registry-union problem the previous revision had (a party living
-- somewhere no merchant trades would dangle an edge at a City that never
-- loaded) simply does not arise.
--
-- ---------------------------------------------------------------------
-- ONE LOCATION PER MERCHANT, AND ONLINE MERCHANTS HAVE NONE
-- ---------------------------------------------------------------------
-- PhantomLedger supplies at most one city/state/zip per merchant, so one
-- Merchant_Location per merchant is materialised, keyed
-- <merchant_id>@<zipcode_id>.
--
-- The previous revision also minted a '<merchant_id>@online' pseudo
-- location for card-not-present merchants, because the old schema made
-- Merchant_Has_Location effectively mandatory. The new schema makes
-- Merchant_Location OPTIONAL ("Optional endpoints may be absent"), so an
-- online merchant now has no location row, no Merchant_Has_Location edge
-- and no Transaction_At_Location edge. Absence is the mask, expressed as
-- cardinality instead of as a placeholder vertex carrying
-- has_coordinates = false.
--
-- Merchant.is_online carries the fact itself, so nothing is lost.
--
-- ---------------------------------------------------------------------
-- COORDINATES
-- ---------------------------------------------------------------------
-- The values are AREA CENTROIDS, not street coordinates: PhantomLedger
-- models postal areas, so two merchants in one area share a point. Do not
-- build a feature that assumes distinct outlet coordinates.
--
-- has_coordinates is the mask and is derived twice over: from row
-- presence in cf_Merchant_Location, and forced false whenever the
-- merchant has no physical area at all. Never test lat <> 0 --- the prime
-- meridian and the equator are both real, and 0,0 is a place in the Gulf
-- of Guinea.
--
-- ---------------------------------------------------------------------
-- COUNTRY CODES ARE EMPTY, DELIBERATELY
-- ---------------------------------------------------------------------
-- The source carries no country anywhere: cf_State has a single `id`
-- column holding a subdivision code, and PhantomLedger's catalogue mixes
-- US states with foreign subdivisions (LND, ON, CMX, ...) that carry no
-- country of their own. Guessing "US for anything that looks like a US
-- state code, blank otherwise" would make country a US-residency flag
-- with a plausible name, so every *_country / country_code in this
-- pipeline loads as "" --- the schema's own "not loaded" default.
--
-- The consequence is recorded rather than hidden:
-- Payment_Transaction.is_cross_border is false on every row, because it
-- is unknowable, not because every authorization is domestic. If the
-- source grows a country column, this comment and the four expressions
-- that reference it in 050/060/070 are the entire change.
-- =====================================================================

DROP VIEW IF EXISTS tf_gnn_prep.merchant_geography CASCADE;
DROP VIEW IF EXISTS tf_gnn_prep.merchant_locations CASCADE;
DROP VIEW IF EXISTS tf_gnn_prep.party_home_area CASCADE;

-- Views from the TF_GNN revision whose targets no longer exist.
DROP VIEW IF EXISTS tf_gnn_prep.party_home_coordinates CASCADE;
DROP VIEW IF EXISTS tf_gnn_prep.loaded_party_has_std_city CASCADE;
DROP VIEW IF EXISTS tf_gnn_prep.loaded_party_has_std_postcode CASCADE;
DROP VIEW IF EXISTS tf_gnn_prep.loaded_party_has_std_state CASCADE;
DROP VIEW IF EXISTS tf_gnn_prep.loaded_cities CASCADE;
DROP VIEW IF EXISTS tf_gnn_prep.loaded_states CASCADE;
DROP VIEW IF EXISTS tf_gnn_prep.loaded_zipcodes CASCADE;
DROP VIEW IF EXISTS tf_gnn_prep.loaded_location_in_city CASCADE;
DROP VIEW IF EXISTS tf_gnn_prep.loaded_location_in_zip CASCADE;
DROP VIEW IF EXISTS tf_gnn_prep.loaded_zip_assigned_to_city CASCADE;
DROP VIEW IF EXISTS tf_gnn_prep.loaded_city_located_in_state CASCADE;


CREATE VIEW
tf_gnn_prep.merchant_geography AS
SELECT
    merchant.id AS merchant_id,

    count(DISTINCT city_edge.city_id) AS city_count,
    min(city_edge.city_id) AS city_id,

    count(DISTINCT state_edge.state_id) AS state_count,
    min(state_edge.state_id) AS state_id,

    count(DISTINCT zip_edge.zipcode_id) AS zipcode_count,
    min(zip_edge.zipcode_id) AS zipcode_id

FROM card_fraud."cf_Merchant" AS merchant

LEFT JOIN card_fraud."cf_Has_City" AS city_edge
  ON city_edge.merchant_id = merchant.id

LEFT JOIN card_fraud."cf_Has_State" AS state_edge
  ON state_edge.merchant_id = merchant.id

LEFT JOIN card_fraud."cf_Has_Zip" AS zip_edge
  ON zip_edge.merchant_id = merchant.id

GROUP BY merchant.id;


-- is_online is the complement of "has all three geography components".
-- A merchant with partial geography is an audit failure (090), so the
-- defensive collapse to online here can only ever apply to rows the
-- audit already blocks.
CREATE VIEW
tf_gnn_prep.merchant_is_online AS
SELECT
    geography.merchant_id,

    NOT (
        geography.city_id IS NOT NULL
        AND geography.state_id IS NOT NULL
        AND geography.zipcode_id IS NOT NULL
    ) AS is_online

FROM tf_gnn_prep.merchant_geography AS geography;


-- PHYSICAL MERCHANTS ONLY. An online merchant produces no row here, and
-- therefore no Merchant_Location vertex, no Merchant_Has_Location edge
-- and no Transaction_At_Location edge.
CREATE VIEW
tf_gnn_prep.merchant_locations AS
SELECT
    geography.merchant_id,

    geography.merchant_id || '@' || geography.zipcode_id AS location_id,

    -- The source models no country. See the header.
    '' AS country_code,

    tf_gnn_prep.clean_text(geography.state_id) AS region_code,

    tf_gnn_prep.postal_prefix(geography.zipcode_id)
        AS postal_code_prefix,

    CASE
        WHEN coords.merchant_id IS NOT NULL
        THEN coords.lat::double precision
        ELSE 0
    END AS latitude,

    CASE
        WHEN coords.merchant_id IS NOT NULL
        THEN coords.lon::double precision
        ELSE 0
    END AS longitude,

    (coords.merchant_id IS NOT NULL) AS has_coordinates,

    -- Merchant_Location is unreachable from a transaction except through
    -- Transaction_At_Location, which carries the transaction's own
    -- stamps, so the location's own first_seen is its merchant's. A
    -- location cannot be observed before the merchant that operates it.
    first_seen.first_seen_unix_time,
    first_seen.first_seen_event_seq

FROM tf_gnn_prep.merchant_geography AS geography

LEFT JOIN card_fraud."cf_Merchant_Location" AS coords
  ON coords.merchant_id = geography.merchant_id

JOIN tf_gnn_prep.merchant_first_seen AS first_seen
  ON first_seen.merchant_id = geography.merchant_id

WHERE geography.city_id IS NOT NULL
  AND geography.state_id IS NOT NULL
  AND geography.zipcode_id IS NOT NULL;


-- ------------------------------------------------------------
-- PARTY HOME AREA
--
-- The only party-linked geography the source carries, and under the new
-- schema its ONLY consumer is Address: region_code and
-- postal_code_prefix on the Address vertex, resolved per address value
-- in 060.
--
-- relocation-2026-07 makes cf_Has_Std_* one row per occupied tenure with
-- a since_unix_time. The schema requires every non-key attribute on a
-- persistent entity vertex to be an IMMUTABLE FIRST-OBSERVATION
-- SNAPSHOT --- "their values must have been known no later than the
-- vertex's first_seen_seq ... and must never be overwritten with later
-- state" --- so the EARLIEST tenure is the correct one to read, not the
-- latest.
--
-- That is the opposite of what the TF_GNN revision did. It took the
-- LATEST tenure, because Street_Address.lat/lon was documented as the
-- party's CURRENT home. Under an immutable-snapshot contract "current"
-- is precisely the wrong choice: it writes state that postdates the
-- vertex's own first_seen, which is what makes full node-table export
-- unsafe for historical sampling.
--
-- A mover's later homes are not lost information the graph needs; a
-- point-in-time home belongs on the contemporaneous transaction or in a
-- separately versioned fact, exactly as the schema says.
-- ------------------------------------------------------------

CREATE VIEW
tf_gnn_prep.party_home_area AS
SELECT
    party.id AS party_id,

    home_state.state_id,
    home_zip.zipcode_id,

    coalesce(home_zip.tenure_count, 0) AS postcode_tenure_count,
    coalesce(home_state.tenure_count, 0) AS state_tenure_count

FROM card_fraud."cf_Party" AS party

LEFT JOIN (
    SELECT DISTINCT ON (edge.party_id)
        edge.party_id,
        edge.zipcode_id,
        (
            SELECT count(DISTINCT inner_edge.zipcode_id)
            FROM card_fraud."cf_Has_Std_Postcode" AS inner_edge
            WHERE inner_edge.party_id = edge.party_id
        ) AS tenure_count
    FROM card_fraud."cf_Has_Std_Postcode" AS edge
    ORDER BY
        edge.party_id,
        edge.since_unix_time::bigint ASC,
        edge.zipcode_id
) AS home_zip
  ON home_zip.party_id = party.id

LEFT JOIN (
    SELECT DISTINCT ON (edge.party_id)
        edge.party_id,
        edge.state_id,
        (
            SELECT count(DISTINCT inner_edge.state_id)
            FROM card_fraud."cf_Has_Std_State" AS inner_edge
            WHERE inner_edge.party_id = edge.party_id
        ) AS tenure_count
    FROM card_fraud."cf_Has_Std_State" AS edge
    ORDER BY
        edge.party_id,
        edge.since_unix_time::bigint ASC,
        edge.state_id
) AS home_state
  ON home_state.party_id = party.id;
