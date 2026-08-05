"""Verify the data loaded into TransactionFraud_GNN.

Runs the installed tfgnn_validate_graph query (gsql/verify_load.gsql) and
evaluates the schema's loading contract against the export manifest:

- every dataset that must transfer has non-zero rows in the manifest. A
  zero-row dataset would otherwise verify as expected == actual == 0 and
  the missing data would never be flagged;
- every loaded vertex and edge count equals its dataset's manifest row
  count;
- event_seq is dense from 1 (min, max, zero-rows, and the arithmetic sum
  as the density witness), and event_ts_ms is never 0;
- every participation edge repeats its transaction's event_seq and
  event_ts_ms, which is the premise every cutoff-safe query rests on;
- exactly one Account and one Merchant per authorization, at most one
  Card, Location, Device and IP_Address;
- no vertex carries first_seen_seq == 0 and no relation carries
  valid_from_seq == 0 or an inverted validity interval;
- label_known agrees with the availability fields, and a confirmed fraud
  verdict becomes knowable strictly after the event it describes;
- every Account has an owning Party and no Merchant has two operators.

WHAT IS REPORTED RATHER THAN FAILED, and why each one:

  merchant_location_masked          an online merchant legitimately has
                                    no coordinates.
  closed_card_tenures               zero means the corpus has no card
                                    reissues, which makes reissue
                                    features dead weight --- worth
                                    knowing, not a defect.
  unknown_channel_rows              a source that grew a fourth use_chip
                                    value would land here. Non-zero
                                    means the vocabulary needs
                                    extending, not that the load broke.
  amount_absent_rows                amount_present separates a real
                                    0.00 authorization from an unloaded
                                    amount.

There is no declared-but-unloadable type set. Every vertex and edge type
the schema declares is loaded by this pipeline, so there is nothing to
assert zero --- and a type that does not exist cannot be populated by
mistake.
"""

from collections.abc import Mapping
import json
from pathlib import Path
from typing import TypedDict

from tf_gnn_loader.postgres.export import ALLOWED_EMPTY_DATASETS
from tf_gnn_loader.tigergraph.client import Client
from tf_gnn_loader.tigergraph.settings import Settings


_VERIFY_QUERY = "tfgnn_validate_graph"

_MANIFEST_FILENAME = "export_manifest.json"
_REPORT_FILENAME = "tigergraph_verification.json"


_LOADED_VERTEX_TYPES: tuple[str, ...] = (
    "Party",
    "Account",
    "Card",
    "Merchant",
    "Merchant_Location",
    "Device",
    "IP_Address",
    "Email",
    "Phone",
    "Address",
    "Identity_Document",
    "Payment_Transaction",
)


_LOADED_EDGE_TYPES: tuple[str, ...] = (
    "Party_Owns_Account",
    "Account_Has_Card",
    "Party_Has_Email",
    "Party_Has_Phone",
    "Party_Has_Address",
    "Party_Has_Identity_Document",
    "Party_Has_Device",
    "Party_Has_IP",
    "Party_Operates_Merchant",
    "Merchant_Has_Location",
    "Transaction_From_Account",
    "Transaction_Used_Card",
    "Transaction_At_Merchant",
    "Transaction_At_Location",
    "Transaction_Used_Device",
    "Transaction_From_IP",
)


_INVARIANT_KEYS: tuple[str, ...] = (
    "event_seq_min",
    "event_seq_max",
    "event_seq_zero_rows",
    "event_seq_sum",
    "event_ts_ms_zero_rows",
    "edge_stamp_mismatch_rows",
    "txn_missing_account_edge",
    "txn_multi_account_edge",
    "txn_missing_card_edge",
    "txn_multi_card_edge",
    "txn_missing_merchant_edge",
    "txn_multi_merchant_edge",
    "txn_multi_location_edge",
    "txn_multi_device_edge",
    "txn_multi_ip_edge",
    "accounts_without_owner",
    "merchants_with_multiple_operators",
    "vertices_with_zero_first_seen",
    "relations_with_zero_valid_from",
    "relations_with_empty_interval",
    "closed_card_tenures",
    "label_known_rows",
    "label_unknown_rows",
    "label_fraud_rows",
    "label_out_of_domain_rows",
    "label_violation_rows",
    "amount_absent_rows",
    "card_present_rows",
    "ecommerce_rows",
    "unknown_channel_rows",
    "event_location_rows",
    "online_merchant_count",
    "merchant_location_with_coordinates",
    "merchant_location_masked",
)


# Counts that must be zero on a correct load. Everything not in this
# tuple is reported; see the module docstring for why each exclusion is
# an exclusion.
_ZERO_INVARIANTS: tuple[str, ...] = (
    "event_seq_zero_rows",
    "event_ts_ms_zero_rows",
    "edge_stamp_mismatch_rows",
    "txn_missing_account_edge",
    "txn_multi_account_edge",
    "txn_missing_card_edge",
    "txn_multi_card_edge",
    "txn_missing_merchant_edge",
    "txn_multi_merchant_edge",
    "txn_multi_location_edge",
    "txn_multi_device_edge",
    "txn_multi_ip_edge",
    "accounts_without_owner",
    "merchants_with_multiple_operators",
    "vertices_with_zero_first_seen",
    "relations_with_zero_valid_from",
    "relations_with_empty_interval",
    "label_out_of_domain_rows",
    "label_violation_rows",
)


_AMOUNT_KEYS: tuple[str, ...] = ("amount_sum",)


# Dataset name -> graph objects whose loaded count must equal the
# dataset's manifest row count.
#
# 22_transactions is the one dataset that feeds several objects: one PSV
# row creates the Payment_Transaction vertex plus its Account, Card and
# Merchant edges, so all four must equal its row count exactly.
#
# Transaction_At_Location is deliberately ABSENT from that tuple. It is
# created from the same row but only when the merchant has an outlet, so
# its count is the physical-merchant subset and asserting equality with
# the full row count would fail on every corpus that has an online
# merchant in it.
_EXPECTED_OBJECTS: dict[str, tuple[str, ...]] = {
    "01_parties": ("Party",),
    "02_accounts": ("Account",),
    "03_cards": ("Card",),
    "04_merchants": ("Merchant",),
    "05_merchant_locations": ("Merchant_Location",),
    "06_devices": ("Device",),
    "07_ip_addresses": ("IP_Address",),
    "08_emails": ("Email",),
    "09_phones": ("Phone",),
    "10_addresses": ("Address",),
    "11_identity_documents": ("Identity_Document",),
    "12_party_owns_account": ("Party_Owns_Account",),
    "13_account_has_card": ("Account_Has_Card",),
    "14_party_has_email": ("Party_Has_Email",),
    "15_party_has_phone": ("Party_Has_Phone",),
    "16_party_has_address": ("Party_Has_Address",),
    "17_party_has_identity_document": ("Party_Has_Identity_Document",),
    "18_party_has_device": ("Party_Has_Device",),
    "19_party_has_ip": ("Party_Has_IP",),
    "20_party_operates_merchant": ("Party_Operates_Merchant",),
    "21_merchant_has_location": ("Merchant_Has_Location",),
    "22_transactions": (
        "Payment_Transaction",
        "Transaction_From_Account",
        "Transaction_Used_Card",
        "Transaction_At_Merchant",
    ),
    "23_transaction_used_device": ("Transaction_Used_Device",),
    "24_transaction_from_ip": ("Transaction_From_IP",),
}


class VerificationReport(TypedDict):
    graphname: str
    vertices: dict[str, int]
    edges: dict[str, int]
    invariants: dict[str, int]
    amount_sums: dict[str, float]
    expected: dict[str, int]
    failures: dict[str, str]
    passed: bool


def _mapping(
    value: object,
    context: str,
) -> dict[str, object]:
    if not isinstance(value, dict):
        raise RuntimeError(
            f"Expected an object for {context}, got " + type(value).__name__
        )

    result: dict[str, object] = {}

    for key, item in value.items():
        if not isinstance(key, str):
            raise RuntimeError(f"Expected string keys for {context}")

        result[key] = item

    return result


def _integer(
    value: object,
    context: str,
) -> int:
    if isinstance(value, bool):
        raise RuntimeError(f"Expected an integer for {context}, got boolean")

    if isinstance(value, int):
        return value

    if isinstance(value, float) and value.is_integer():
        return int(value)

    raise RuntimeError(
        f"Expected an integer for {context}, got "
        + f"{type(value).__name__}: {value!r}"
    )


def _number(
    value: object,
    context: str,
) -> float:
    if isinstance(value, bool):
        raise RuntimeError(f"Expected a number for {context}, got boolean")

    if isinstance(value, (int, float)):
        return float(value)

    raise RuntimeError(
        f"Expected a number for {context}, got " + f"{type(value).__name__}: {value!r}"
    )


def _read_manifest_rows(
    output_dir: Path,
) -> dict[str, int]:
    path = output_dir / _MANIFEST_FILENAME

    if not path.is_file():
        raise RuntimeError(f"Export manifest does not exist: {path}")

    raw: object = json.loads(
        path.read_text(
            encoding="utf-8",
        )
    )

    manifest = _mapping(
        raw,
        "export manifest",
    )

    datasets = _mapping(
        manifest.get("datasets"),
        "export manifest datasets",
    )

    rows: dict[str, int] = {}

    for dataset_name, dataset_value in datasets.items():
        dataset = _mapping(
            dataset_value,
            f"dataset {dataset_name}",
        )

        rows[dataset_name] = _integer(
            dataset.get("rows"),
            f"{dataset_name} rows",
        )

    return rows


def _query_result(
    client: Client,
) -> dict[str, object]:
    raw = client.run_installed_with_timeout(
        _VERIFY_QUERY,
        {},
        size_limit=32_000_000,
    )

    merged: dict[str, object] = {}

    for index, item in enumerate(raw):
        mapping = _mapping(
            item,
            f"{_VERIFY_QUERY} result {index}",
        )

        merged.update(mapping)

    if not merged:
        raise RuntimeError(f"{_VERIFY_QUERY!r} returned no values")

    return merged


def _counts(
    result: Mapping[str, object],
    names: tuple[str, ...],
) -> dict[str, int]:
    return {
        name: _integer(
            result.get(name),
            name,
        )
        for name in names
    }


def _amounts(
    result: Mapping[str, object],
    names: tuple[str, ...],
) -> dict[str, float]:
    return {
        name: _number(
            result.get(name),
            name,
        )
        for name in names
    }


def _expected_counts(
    dataset_rows: Mapping[str, int],
) -> dict[str, int]:
    expected: dict[str, int] = {}

    for dataset_name, objects in _EXPECTED_OBJECTS.items():
        if dataset_name not in dataset_rows:
            raise RuntimeError(f"Manifest is missing dataset {dataset_name!r}")

        rows = dataset_rows[dataset_name]

        for object_name in objects:
            expected[object_name] = rows

    return expected


def _empty_dataset_failures(
    dataset_rows: Mapping[str, int],
) -> dict[str, str]:
    """Fail datasets whose manifest row count is zero.

    Without this, a dataset that silently exported nothing verifies as
    expected == actual == 0 and the missing data is never flagged.

    The two optional event-time endpoint datasets are exempt: the schema
    calls Device and IP_Address optional endpoints, so a corpus without
    cf_Transaction_Uses_* is a corpus without those relations, not a
    broken export.
    """

    failures: dict[str, str] = {}

    for dataset_name, objects in _EXPECTED_OBJECTS.items():
        if dataset_name in ALLOWED_EMPTY_DATASETS:
            continue

        if dataset_rows.get(dataset_name) != 0:
            continue

        failures[dataset_name] = (
            "manifest has zero rows; "
            + ", ".join(objects)
            + " would be missing from the graph. Re-run "
            + "`tf-gnn-load export` against corrected source data."
        )

    return failures


def _failures(
    vertices: Mapping[str, int],
    edges: Mapping[str, int],
    invariants: Mapping[str, int],
    expected: Mapping[str, int],
) -> dict[str, str]:
    failures: dict[str, str] = {}

    actual_objects: dict[str, int] = {
        **vertices,
        **edges,
    }

    for object_name, expected_count in expected.items():
        actual_count = actual_objects.get(object_name)

        if actual_count != expected_count:
            failures[object_name] = (
                f"expected={expected_count:,}, " + f"actual={actual_count!r}"
            )

    transaction_count = vertices["Payment_Transaction"]

    # The temporal contract: dense rank from 1, no zeros, and the
    # arithmetic-series sum as the density witness. The sum is what
    # catches a duplicate-plus-gap pair, which min and max both pass.
    if transaction_count > 0:
        if invariants["event_seq_min"] != 1:
            failures["event_seq_min"] = (
                "expected=1, " + f"actual={invariants['event_seq_min']:,}"
            )

        if invariants["event_seq_max"] != transaction_count:
            failures["event_seq_max"] = (
                f"expected={transaction_count:,}, "
                + f"actual={invariants['event_seq_max']:,}"
            )

        expected_sum = transaction_count * (transaction_count + 1) // 2

        if invariants["event_seq_sum"] != expected_sum:
            failures["event_seq_sum"] = (
                f"expected={expected_sum:,}, "
                + f"actual={invariants['event_seq_sum']:,}"
            )

    for check_name in _ZERO_INVARIANTS:
        value = invariants[check_name]

        if value != 0:
            failures[check_name] = f"expected=0, actual={value:,}"

    # label_known must partition the transactions: every row is either
    # usable supervision or explicitly not, with no third state.
    label_total = invariants["label_known_rows"] + invariants["label_unknown_rows"]

    if label_total != transaction_count:
        failures["label_known_total"] = (
            f"expected={transaction_count:,}, " + f"actual={label_total:,}"
        )

    # Transaction_At_Location is the physical-merchant subset, so it is
    # not compared against the manifest row count. What it must never do
    # is exceed it.
    location_edges = edges["Transaction_At_Location"]

    if location_edges > transaction_count:
        failures["Transaction_At_Location"] = (
            f"expected<={transaction_count:,}, " + f"actual={location_edges:,}"
        )

    return failures


def verify(
    settings: Settings,
    output_dir: Path,
) -> VerificationReport:
    """Run exact load verification through the installed GSQL query."""

    client = Client(settings)

    result = _query_result(client)

    vertices = _counts(
        result,
        _LOADED_VERTEX_TYPES,
    )

    edges = _counts(
        result,
        _LOADED_EDGE_TYPES,
    )

    invariants = _counts(
        result,
        _INVARIANT_KEYS,
    )

    amount_sums = _amounts(
        result,
        _AMOUNT_KEYS,
    )

    dataset_rows = _read_manifest_rows(
        output_dir,
    )

    expected = _expected_counts(
        dataset_rows,
    )

    failures = _empty_dataset_failures(
        dataset_rows,
    )

    failures.update(
        _failures(
            vertices=vertices,
            edges=edges,
            invariants=invariants,
            expected=expected,
        )
    )

    report: VerificationReport = {
        "graphname": settings.graphname,
        "vertices": vertices,
        "edges": edges,
        "invariants": invariants,
        "amount_sums": amount_sums,
        "expected": expected,
        "failures": failures,
        "passed": not failures,
    }

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_path = output_dir / _REPORT_FILENAME

    _ = output_path.write_text(
        json.dumps(
            report,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    print("TigerGraph verification report written to " + str(output_path))

    print(
        "labels: "
        + f"{invariants['label_fraud_rows']:,} fraud, "
        + f"{invariants['label_known_rows']:,} known, "
        + f"{invariants['label_unknown_rows']:,} unresolved"
    )

    print(
        "reissues: "
        + f"{invariants['closed_card_tenures']:,} Account_Has_Card "
        + "tenures are closed by a later card generation"
    )

    print(
        "geography: "
        + f"{invariants['merchant_location_with_coordinates']:,} outlets "
        + "with coordinates, "
        + f"{invariants['merchant_location_masked']:,} masked, "
        + f"{invariants['online_merchant_count']:,} online merchants"
    )

    if invariants["unknown_channel_rows"]:
        print(
            "Note: "
            + f"{invariants['unknown_channel_rows']:,} transactions "
            + "carry channel='unknown'. The source grew a use_chip value "
            + "sql/postgres/070_create_transaction_views.sql does not map."
        )

    if failures:
        details = "; ".join(f"{name}: {message}" for name, message in failures.items())

        raise RuntimeError("TigerGraph verification failures: " + details)

    print("TransactionFraud_GNN load verification passed.")

    return report
