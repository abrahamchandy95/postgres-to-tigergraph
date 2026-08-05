"""Reference copy of the PhantomLedger source contract.

The ENFORCED contract lives in sql/postgres/001_validate_sources.sql;
this module mirrors it for tooling and documentation. The authoritative
upstream list is PhantomLedger's own
`include/phantomledger/exporter/card_fraud/schema.hpp` (kTableCount = 43).

HOW THE SOURCE MAPS ONTO TransactionFraud_GNN
---------------------------------------------

    Party                cf_Party (id, party_type, created_at)
    Account              DERIVED. No source table --- see ACCOUNT below.
    Card                 cf_Card
    Merchant             cf_Merchant + cf_Merchant_Assigned
    Merchant_Location    cf_Merchant_Location + cf_Has_City / _State / _Zip
    Device               cf_Device, tokenised
    IP_Address           cf_IP, tokenised
    Email                cf_Email, tokenised (domain retained)
    Phone                cf_Phone, tokenised
    Address              cf_Address, tokenised
    Identity_Document    cf_ID, tokenised (id_type retained)
    Payment_Transaction  cf_Payment_Transaction

    Transaction_From_Account   cf_Card_Send_Transaction, via the
                               funding-instrument derivation
    Transaction_Used_Card      cf_Card_Send_Transaction
    Transaction_At_Merchant    cf_Merchant_Receive_Transaction
    Transaction_At_Location    the merchant's outlet, when it has one
    Transaction_Used_Device    cf_Transaction_Uses_Device
    Transaction_From_IP        cf_Transaction_Uses_IP

    Party_Owns_Account         cf_Party_Has_Card, through the derivation
    Account_Has_Card           cf_Card, grouped by funding instrument
    Party_Operates_Merchant    cf_Is_Merchant (direction flipped)
    Merchant_Has_Location      derived, one outlet per physical merchant
    Party_Has_*                cf_Has_Email / _Phone / _Address / _ID /
                               _Device / _IP

ACCOUNT
-------
The source exports no account table. account_id is recovered from the
card identifier, which PhantomLedger's `derive.hpp` documents as

    [C|D] <rendered funding key> [ -G<generation> ]

where a 'D' card renders the DEPOSIT-ACCOUNT key, a 'C' card renders the
CREDIT-CARD key ("cards and accounts are distinct id spaces"), and the
-G suffix is the reissue generation. Stripping that suffix therefore
yields the funding instrument the authorization drew on, which is what
the schema's Account is: exactly one INITIATING/FUNDING account per
authorization, with the merchant as the receiving side.

The derivation is `tf_gnn_prep.funding_account_id` in
sql/postgres/020_create_policies.sql. If the source ever exports real
accounts, that function is the only thing that changes.

NEW REQUIREMENTS RELATIVE TO THE TF_GNN LOADER
----------------------------------------------
cf_Transaction_Uses_Device and cf_Transaction_Uses_IP are now REQUIRED.
They are the EVENT-TIME endpoints --- which device and which source IP
this authorization actually came from --- and the new schema's
Transaction_Used_Device / Transaction_From_IP have no other source. The
previous loader only had the whole-window cf_Has_Device / cf_Has_IP
associations and could not build either relation.

NO LONGER REQUIRED, AND DELIBERATELY NOT READ
---------------------------------------------
cf_Full_Name, cf_Has_Full_Name, cf_DOB, cf_Has_DOB, and the name / gender
/ dob columns of cf_Party. The schema states that protected attributes
such as name, gender and ethnicity are intentionally absent, and it
declares no Full_Name, Full_Name_Hash or Birthdate vertex. Removing them
from the contract is the enforcement: a column nothing validates is a
column nothing can accidentally load.

cf_Email_Minhash, cf_Has_Email_Minhash: entity-resolution blocking
structure. The schema puts ER outside its scope explicitly ("may be added
later as an optional, separately versioned module").

cf_Ground_Truth_Label: a whole-window entity label overlay. Reading it
into the graph would reintroduce exactly the leak it exists to
quarantine --- "this card ever carried a flag-1 row" answers the training
question before the model sees a transaction.

cf_IP.is_blocked, cf_Device.is_blocked: timeless booleans, and a
blocklist entry is written AFTER fraud is confirmed. The schema declares
no attribute they could reach.

cf_Payment_Transaction.error: a decline code is part of the authorization
RESPONSE, and the schema is explicit that "the scoring moment is the
authorization request, before that response exists".

CITY / STATE / ZIPCODE ARE READ BUT ARE NOT VERTICES
-----------------------------------------------------
The schema deletes all three. They survive only as the attribute source
for Merchant_Location.region_code / postal_code_prefix and
Address.region_code / postal_code_prefix. cf_Has_Std_* keeps its
since_unix_time requirement because the earliest tenure is what supplies
an Address's immutable first-observation snapshot.
"""

from __future__ import annotations

from typing import Final

SOURCE_SCHEMA: Final = "card_fraud"
PREP_SCHEMA: Final = "tf_gnn_prep"
TARGET_GRAPH: Final = "TransactionFraud_GNN"

REQUIRED_COLUMNS: Final[dict[str, frozenset[str]]] = {
    # NO error: it is an authorization response. See the module docstring.
    "cf_Payment_Transaction": frozenset(
        {
            "id",
            "transaction_time",
            "amount",
            "is_fraud",
            "unix_time",
            "mer_cat",
            "use_chip",
        }
    ),
    "cf_Card_Send_Transaction": frozenset({"txn_id", "card_number", "edge_unix_time"}),
    "cf_Merchant_Receive_Transaction": frozenset(
        {"txn_id", "merchant_id", "edge_unix_time"}
    ),
    # Event-time endpoints. New in this revision.
    "cf_Transaction_Uses_Device": frozenset({"txn_id", "device_id", "edge_unix_time"}),
    "cf_Transaction_Uses_IP": frozenset({"txn_id", "ip_id", "edge_unix_time"}),
    "cf_Card": frozenset({"card_number"}),
    "cf_Merchant": frozenset({"id"}),
    # NO name / gender / dob: protected attributes.
    "cf_Party": frozenset({"id", "party_type", "created_at"}),
    "cf_Party_Has_Card": frozenset({"party_id", "card_number"}),
    "cf_Is_Merchant": frozenset({"merchant_id", "party_id"}),
    "cf_Merchant_Assigned": frozenset({"merchant_id", "category"}),
}

GEO_COLUMNS: Final[dict[str, frozenset[str]]] = {
    "cf_City": frozenset({"id", "city", "lat", "lon"}),
    "cf_State": frozenset({"id"}),
    "cf_Zipcode": frozenset({"id", "lat", "lon"}),
    "cf_Merchant_Location": frozenset({"merchant_id", "lat", "lon"}),
    "cf_Has_City": frozenset({"merchant_id", "city_id"}),
    "cf_Has_State": frozenset({"merchant_id", "state_id"}),
    "cf_Has_Zip": frozenset({"merchant_id", "zipcode_id"}),
    # since_unix_time is REQUIRED: it is the only real attachment time in
    # the source, and the EARLIEST tenure is what gives an Address its
    # immutable first-observation region and postcode prefix.
    "cf_Has_Std_City": frozenset({"party_id", "city_id", "since_unix_time"}),
    "cf_Has_Std_Postcode": frozenset({"party_id", "zipcode_id", "since_unix_time"}),
    "cf_Has_Std_State": frozenset({"party_id", "state_id", "since_unix_time"}),
}

PII_COLUMNS: Final[dict[str, frozenset[str]]] = {
    "cf_Address": frozenset({"address"}),
    "cf_Phone": frozenset({"phone_number"}),
    "cf_Email": frozenset({"email"}),
    # is_blocked deliberately NOT required: see the module docstring.
    "cf_IP": frozenset({"id"}),
    "cf_Device": frozenset({"id"}),
    "cf_ID": frozenset({"id", "id_type"}),
    "cf_Has_Address": frozenset({"party_id", "address"}),
    "cf_Has_Phone": frozenset({"party_id", "phone_number"}),
    "cf_Has_Email": frozenset({"party_id", "email"}),
    "cf_Has_IP": frozenset({"party_id", "ip_id"}),
    "cf_Has_Device": frozenset({"party_id", "device_id"}),
    "cf_Has_ID": frozenset({"party_id", "id"}),
}

# Present in the source, deliberately unread. Listed so `tf-gnn-load
# inspect` can report them as "known and skipped" rather than as
# unexpected tables somebody might helpfully wire up.
DELIBERATELY_UNREAD_TABLES: Final[dict[str, str]] = {
    "cf_Full_Name": "protected attribute; no Full_Name vertex in the schema",
    "cf_Has_Full_Name": "protected attribute",
    "cf_DOB": "protected attribute; no Birthdate vertex in the schema",
    "cf_Has_DOB": "protected attribute",
    "cf_Merchant_Category": (
        "Merchant_Category the vertex is deleted; the category is an "
        "attribute on Merchant and on Payment_Transaction"
    ),
    "cf_Email_Minhash": "entity-resolution blocking key; ER is out of schema scope",
    "cf_Has_Email_Minhash": "entity-resolution blocking key",
    "cf_Ground_Truth_Label": (
        "whole-window entity labels; reading them into the graph is the "
        "leak the table exists to quarantine"
    ),
    "cf_Assigned_To": "zip -> city; City and Zipcode are not vertices any more",
    "cf_Located_In": "city -> state; City and State are not vertices any more",
}

# Columns that must never appear in a tf_gnn_prep.load_* view. The
# enforced version is tf_gnn_prep.audit_forbidden_columns, which reads the
# PostgreSQL catalogue rather than this tuple.
FORBIDDEN_LOAD_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "email",
        "email_raw",
        "phone_number",
        "phone_raw",
        "address",
        "address_raw",
        "device_raw",
        "ip_raw",
        "document_raw",
        "name",
        "gender",
        "dob",
        "birthdate",
        "full_name",
        "split_id",
        "causal_fold",
        "error",
        "is_blocked",
        "label_resolution_status",
    }
)
