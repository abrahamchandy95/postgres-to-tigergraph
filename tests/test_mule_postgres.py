"""Optional real-SQL tests using only session-local PostgreSQL fixtures.

Run with MULE_TEST_DSN set. No permanent objects or source rows are changed.
"""

from contextlib import redirect_stdout
import io
import os
from typing import Any
import unittest

import psycopg

from tf_gnn_loader.mule.contract import DATASETS
from tf_gnn_loader.mule.postgres import audit_snapshot, view_query


class FixtureConnection:
    def __init__(self, connection: Any) -> None:
        self.connection = connection

    def execute(self, query: bytes) -> Any:
        source = query.decode()
        for d in DATASETS:
            source = source.replace(d.table, f'pg_temp."raw_{d.name}"')
        return self.connection.execute(source.encode())


@unittest.skipUnless(
    os.getenv("MULE_TEST_DSN"), "Set MULE_TEST_DSN for PostgreSQL fixture tests"
)
class TemporalSourceAudits(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = psycopg.connect(os.environ["MULE_TEST_DSN"], autocommit=True)
        self.addCleanup(self.conn.close)
        self.fixture: Any = FixtureConnection(self.conn)
        ids = {
            "Party": "p_",
            "Account": "a_",
            "Token": "t_",
            "Device": "d_",
            "IP": "i_",
            "Address": "h_",
        }
        ids = {k: v + "0" * 32 for k, v in ids.items()}
        ids.update(Payment_Transaction="T1", Zelle_Transfer="T2")
        for d in DATASETS:
            definition = ",".join(f'"{name}" text' for name in d.columns)
            self.conn.execute(
                f'CREATE TEMP TABLE "raw_{d.name}" ({definition})'.encode()
            )
            attrs = {name: "unknown" for name in d.columns}
            if d.vertex:
                attrs[d.columns[0]] = ids[d.name]
                attrs.update(
                    first_seen_seq="1",
                    first_seen_ts_ms="1000",
                    # PhantomLedger renders booleans as True and False.
                    is_external="False",
                    is_mule="0",
                    mule_label_known="False",
                    is_mule_masked="True",
                    pu_label="0",
                    mule_label_effective_seq="1",
                    mule_label_effective_ts_ms="1000",
                    mule_label_available_seq="1",
                    mule_label_available_ts_ms="1000",
                    mule_ring_id="-1",
                    mule_label_source="phantomledger_role",
                )
                if d.name in ("Payment_Transaction", "Zelle_Transfer"):
                    zelle = d.name == "Zelle_Transfer"
                    attrs.update(
                        event_time="1970-01-01 00:00:10"
                        if zelle
                        else "1970-01-01 00:00:05",
                        event_ts_ms="10000" if zelle else "5000",
                        event_seq="10" if zelle else "5",
                        amount="10",
                        amount_present="true",
                        currency="USD",
                        payment_rail="ach",
                        channel="digital",
                        fraud_label="0",
                        label_known="true",
                        label_available_seq="11",
                        label_available_ts_ms="10000",
                    )
            else:
                attrs.update(from_id=ids[d.source], to_id=ids[d.target])
                if d.association:
                    attrs.update(
                        valid_from_seq="2",
                        valid_to_seq="0",
                        confidence="1",
                        source_system="synthetic_registry",
                    )
                else:
                    attrs.update(
                        event_seq="10" if d.source == "Zelle_Transfer" else "5",
                        event_ts_ms="10000" if d.source == "Zelle_Transfer" else "5000",
                    )
            values = [attrs[col] for col in d.columns]
            self.conn.execute(
                (
                    f'INSERT INTO "raw_{d.name}" VALUES ('
                    + ",".join(["%s"] * len(values))
                    + ")"
                ).encode(),
                values,
            )
            self.conn.execute(view_query(d, f'"raw_{d.name}"').encode())

    def audit(self) -> dict[str, Any]:
        with redirect_stdout(io.StringIO()):
            return audit_snapshot(self.fixture)

    def change(self, table: str, column: str, value: str | None) -> None:
        self.conn.execute(f'UPDATE "raw_{table}" SET "{column}"=%s'.encode(), (value,))

    def test_valid_fixture_and_disjoint_repeated_tenures(self) -> None:
        self.assertTrue(self.audit()["passed"])
        self.change("Party_Uses_Device", "valid_to_seq", "4")
        self.conn.execute(
            b'''INSERT INTO "raw_Party_Uses_Device" SELECT from_id,to_id,'6','0',confidence,source_system FROM "raw_Party_Uses_Device"'''
        )
        self.assertTrue(self.audit()["passed"])

    def test_overlap_is_rejected(self) -> None:
        self.conn.execute(
            b'''INSERT INTO "raw_Party_Uses_Device" SELECT from_id,to_id,'6','0',confidence,source_system FROM "raw_Party_Uses_Device"'''
        )
        with self.assertRaisesRegex(RuntimeError, "overlapping tenures"):
            self.audit()

    def test_account_mule_flag_validation_and_distribution(self) -> None:
        self.change("Account", "is_mule", "1")
        self.assertEqual(self.audit()["account_mule_flags"], {"1": 1})
        self.change("Account", "is_mule", "2")
        with self.assertRaisesRegex(RuntimeError, "account mule flag"):
            self.audit()

    def test_account_label_columns(self) -> None:
        for column, value, valid in (
            ("mule_ring_id", None, "-1"),
            ("mule_label_known", "yes", "False"),
            ("is_mule_masked", None, "True"),
            ("pu_label", "0.5", "0"),
            ("mule_label_available_ts_ms", "-1", "1000"),
            ("mule_label_source", "a|b", "phantomledger_role"),
        ):
            with self.subTest(column=column, value=value):
                self.change("Account", column, value)
                with self.assertRaisesRegex(RuntimeError, "Account: source values"):
                    self.audit()
                self.change("Account", column, valid)
        self.change("Account", "mule_ring_id", "0")
        self.change("Account", "is_mule", "1")
        self.assertEqual(self.audit()["account_mule_rings"], {"0": 1})
        # An external account's empty source arrives as NULL and loads empty.
        self.change("Account", "mule_label_source", None)
        self.assertTrue(self.audit()["passed"])
        row = self.conn.execute(b'SELECT mule_label_source FROM "mt_Account"')
        self.assertEqual(row.fetchone(), ("",))
        self.change("Account", "mule_ring_id", "-2")
        with self.assertRaisesRegex(RuntimeError, "account mule ring"):
            self.audit()

    def test_zero_clock_and_fractional_sequence_are_rejected(self) -> None:
        self.change("Party", "first_seen_seq", "0")
        with self.assertRaisesRegex(RuntimeError, "first observation"):
            self.audit()
        self.change("Party", "first_seen_seq", "1.1")
        with self.assertRaisesRegex(RuntimeError, "source values"):
            self.audit()

    def test_mismatched_edge_clock_is_rejected(self) -> None:
        self.change("Transfer_Used_Device", "event_seq", "12")
        with self.assertRaisesRegex(RuntimeError, "repeated clocks"):
            self.audit()

    def test_zelle_duplicate_is_rejected(self) -> None:
        self.change("Zelle_Transfer", "transfer_id", "T1")
        for d in DATASETS:
            if d.source == "Zelle_Transfer":
                self.change(d.name, "from_id", "T1")
        with self.assertRaisesRegex(RuntimeError, "exclusive payment identities"):
            self.audit()

    def test_future_entity_is_rejected(self) -> None:
        self.change("Device", "first_seen_seq", "20")
        with self.assertRaisesRegex(RuntimeError, "endpoints"):
            self.audit()

    def test_closed_binding_is_not_visible_at_its_end(self) -> None:
        self.change("Token_Bound_To_Account", "valid_to_seq", "10")
        with self.assertRaisesRegex(RuntimeError, "visible binding"):
            self.audit()

    def test_label_and_amount_sentinels(self) -> None:
        self.change("Zelle_Transfer", "label_known", "false")
        with self.assertRaisesRegex(RuntimeError, "label availability"):
            self.audit()
        for col, value in (
            ("fraud_label", "-1"),
            ("label_available_seq", "0"),
            ("label_available_ts_ms", "0"),
        ):
            self.change("Zelle_Transfer", col, value)
        self.assertTrue(self.audit()["passed"])
        self.change("Zelle_Transfer", "amount_present", "false")
        with self.assertRaisesRegex(RuntimeError, "event values"):
            self.audit()

    def test_unsafe_psv_and_raw_pii_id_are_rejected(self) -> None:
        self.change("Party", "party_type", "first|second")
        with self.assertRaisesRegex(RuntimeError, "source values"):
            self.audit()
        self.change("Party", "party_type", "person")
        self.change("Party", "id", "person@example.com")
        with self.assertRaisesRegex(RuntimeError, "opaque IDs"):
            self.audit()


if __name__ == "__main__":
    unittest.main()
