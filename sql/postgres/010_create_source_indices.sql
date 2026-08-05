-- =====================================================================
-- SOURCE INDICES
--
-- The cf_* mirrors are all-text and carry no indices of their own. These
-- support the joins the prep does at row scale:
--
--   1. the (unix_time, id) ordering that defines event_seq;
--   2. txn_id lookups on the four per-transaction endpoint tables;
--   3. card_number / merchant_id lookups for the first-seen stamps.
--
-- Nothing here changes a source value. CREATE INDEX only.
-- =====================================================================

CREATE UNIQUE INDEX IF NOT EXISTS
    cf_payment_transaction_id_uq
ON card_fraud."cf_Payment_Transaction" (id);


CREATE INDEX IF NOT EXISTS
    cf_payment_transaction_unix_time_idx
ON card_fraud."cf_Payment_Transaction"
   ((unix_time::bigint));


-- Supports the deterministic event_seq ordering:
-- row_number() OVER (ORDER BY unix_time::bigint, id).
--
-- The schema orders event_seq by (event_ts_ms, transaction_id).
-- event_ts_ms is unix_time * 1000, a strictly increasing function of
-- unix_time, so the two orderings are identical and this index serves
-- both.
CREATE INDEX IF NOT EXISTS
    cf_payment_transaction_event_order_idx
ON card_fraud."cf_Payment_Transaction"
   ((unix_time::bigint), id);


CREATE UNIQUE INDEX IF NOT EXISTS
    cf_card_send_transaction_txn_id_uq
ON card_fraud."cf_Card_Send_Transaction" (txn_id);


CREATE INDEX IF NOT EXISTS
    cf_card_send_transaction_card_idx
ON card_fraud."cf_Card_Send_Transaction" (card_number);


CREATE UNIQUE INDEX IF NOT EXISTS
    cf_merchant_receive_transaction_txn_id_uq
ON card_fraud."cf_Merchant_Receive_Transaction" (txn_id);


CREATE INDEX IF NOT EXISTS
    cf_merchant_receive_transaction_merchant_idx
ON card_fraud."cf_Merchant_Receive_Transaction" (merchant_id);


-- Event-time endpoints. UNIQUE on txn_id is the "canonical device" /
-- "canonical source IP" half of the cardinality contract expressed as a
-- constraint rather than only as a 001 check: at most one per
-- transaction, so Transaction_Used_Device and Transaction_From_IP cannot
-- become multi-edges by accident.
CREATE UNIQUE INDEX IF NOT EXISTS
    cf_transaction_uses_device_txn_id_uq
ON card_fraud."cf_Transaction_Uses_Device" (txn_id);


CREATE INDEX IF NOT EXISTS
    cf_transaction_uses_device_device_idx
ON card_fraud."cf_Transaction_Uses_Device" (device_id);


CREATE UNIQUE INDEX IF NOT EXISTS
    cf_transaction_uses_ip_txn_id_uq
ON card_fraud."cf_Transaction_Uses_IP" (txn_id);


CREATE INDEX IF NOT EXISTS
    cf_transaction_uses_ip_ip_idx
ON card_fraud."cf_Transaction_Uses_IP" (ip_id);


-- Ownership: Account_Has_Card and Party_Owns_Account both walk this
-- table, and 050 groups it by the derived funding-instrument key.
CREATE INDEX IF NOT EXISTS
    cf_party_has_card_card_idx
ON card_fraud."cf_Party_Has_Card" (card_number);


CREATE INDEX IF NOT EXISTS
    cf_party_has_card_party_idx
ON card_fraud."cf_Party_Has_Card" (party_id);


ANALYZE card_fraud."cf_Payment_Transaction";
ANALYZE card_fraud."cf_Card_Send_Transaction";
ANALYZE card_fraud."cf_Merchant_Receive_Transaction";
ANALYZE card_fraud."cf_Transaction_Uses_Device";
ANALYZE card_fraud."cf_Transaction_Uses_IP";
ANALYZE card_fraud."cf_Party_Has_Card";
