"""Regression coverage for schema identity, artifact integrity and resumption."""

import copy
import json
from pathlib import Path
import re
from tempfile import TemporaryDirectory
from threading import Barrier
from types import SimpleNamespace
from typing import Any
import unittest
from unittest.mock import Mock, patch

import requests

from tf_gnn_loader.cli import parser
from tf_gnn_loader.mule.contract import (
    DATASETS,
    FORMAT_VERSION,
    GRAPH,
    GSQL,
    VERTICES,
    visible,
)
from tf_gnn_loader.mule.gsql import loading_jobs, verify_query
from tf_gnn_loader.mule.tigergraph import (
    load,
    read_manifest,
    upload_shard,
    validate_schema,
)
from tf_gnn_loader.tigergraph.loading import sha256
from tf_gnn_loader.tigergraph.settings import Settings


def schema_fixture() -> dict[str, Any]:
    schema: dict[str, Any] = {"GraphName": GRAPH, "VertexTypes": [], "EdgeTypes": []}
    for d in DATASETS:
        attrs: list[dict[str, Any]] = [
            {
                "AttributeName": n,
                "AttributeType": {"Name": "LIST", "ValueTypeName": "DOUBLE"}
                if k == "LIST<DOUBLE>"
                else {"Name": k},
            }
            for n, k in d.graph_fields[(1 if d.vertex else 2) :]
        ]
        item: dict[str, Any] = {"Name": d.name, "Attributes": attrs}
        if d.vertex:
            item["PrimaryId"] = {
                "AttributeName": d.columns[0],
                "AttributeType": {"Name": "STRING"},
                "PrimaryIdAsAttribute": True,
            }
            schema["VertexTypes"].append(item)
        else:
            item.update(
                FromVertexTypeName=d.source,
                ToVertexTypeName=d.target,
                Config={"REVERSE_EDGE": d.reverse},
            )
            if d.association:
                attrs[0]["IsDiscriminator"] = True
            schema["EdgeTypes"].append(item)
    return schema


def manifest_fixture(directory: Path) -> dict[str, Any]:
    path = directory / "payments.psv"
    path.write_text("T1|1|USD|ach|digital|2024-01-01 00:00:00|1704067200000|1|true\n")
    datasets = {
        d.name: {"loading_job": d.job, "rows": 0, "bytes": 0, "shards": []}
        for d in DATASETS
    }
    shard = {
        "path": str(path),
        "rows": 1,
        "bytes": path.stat().st_size,
        "sha256": sha256(path),
    }
    datasets["Payment_Transaction"].update(rows=1, bytes=shard["bytes"], shards=[shard])
    manifest = {
        "format_version": FORMAT_VERSION,
        "graphname": GRAPH,
        "separator": "|",
        "header": False,
        "datasets": datasets,
        "audit": {
            "passed": True,
            "counts": {k: v["rows"] for k, v in datasets.items()},
        },
    }
    (directory / "export_manifest.json").write_text(json.dumps(manifest))
    return manifest


class TemporalContracts(unittest.TestCase):
    def test_default_use_case_is_unchanged(self):
        self.assertEqual(parser().parse_args(["push"]).use_case, "transaction-fraud")
        self.assertEqual(
            parser().parse_args(["--use-case", "mule-temporal", "push"]).use_case,
            "mule-temporal",
        )

    def test_tenure_boundaries_and_repeated_tenures(self):
        for seed, expected in (
            (9, False),
            (10, True),
            (19, True),
            (20, False),
            (29, False),
            (30, True),
            (100, True),
        ):
            self.assertEqual(visible(10, 20, seed) or visible(30, 0, seed), expected)

    def test_supplied_schema_matches_dataset_columns(self):
        source = re.sub(
            r"/\*.*?\*/", "", (GSQL / "schema.gsql").read_text(), flags=re.S
        )
        for d in DATASETS:
            kind = "VERTEX" if d.vertex else "DIRECTED EDGE"
            match = re.search(rf"ADD {kind} {d.name}\s*\((.*?)\) WITH", source, re.S)
            assert match is not None
            body = match.group(1)
            attrs = tuple(
                re.findall(
                    r"(?:PRIMARY_ID\s+)?(\w+)\s+(LIST<DOUBLE>|STRING|UINT|BOOL|DOUBLE|FLOAT|INT|DATETIME)(?=[\s,)])",
                    body,
                )
            )
            self.assertEqual(attrs, d.graph_fields if d.vertex else d.fields[2:])
            if not d.vertex:
                self.assertEqual("DISCRIMINATOR" in body, d.association)
        self.assertEqual((len(VERTICES), len(DATASETS)), (8, 27))

    def test_generated_gsql_is_current(self):
        self.assertEqual((GSQL / "loading_jobs.gsql").read_text(), loading_jobs())
        self.assertEqual((GSQL / "verify_load.gsql").read_text(), verify_query())

    def test_source_mule_flag_maps_to_final_graph_attribute(self):
        self.assertIn(
            "Account VALUES ($0, $1, $2, $3, $4, _, _, _, _, _, _, _, _, _, $5)",
            loading_jobs(),
        )
        self.assertIn("$8, _, _, _, _, _, _, _, _)", loading_jobs())

    def test_schema_rejects_lost_discriminator_and_reordered_attributes(self):
        schema = schema_fixture()
        validate_schema(schema)
        bad = copy.deepcopy(schema)
        bad["EdgeTypes"][0]["Attributes"][0].pop("IsDiscriminator")
        with self.assertRaisesRegex(RuntimeError, "Discriminator"):
            validate_schema(bad)
        bad = copy.deepcopy(schema)
        bad["VertexTypes"][0]["Attributes"].reverse()
        with self.assertRaisesRegex(RuntimeError, "order mismatch"):
            validate_schema(bad)

    def test_schema_rejects_wrong_graph_and_reverse_edge(self):
        bad = schema_fixture()
        bad["GraphName"] = "TransactionFraud_GNN"
        with self.assertRaises(RuntimeError):
            validate_schema(bad)
        bad = schema_fixture()
        bad["EdgeTypes"][0]["Config"]["REVERSE_EDGE"] = "wrong"
        with self.assertRaises(RuntimeError):
            validate_schema(bad)


class TemporalUploads(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.manifest = manifest_fixture(self.directory)
        options: dict[str, Any] = {
            "_env_file": None,
            "host": "https://test.invalid",
            "graphname": GRAPH,
            "secret": "test-secret",
        }
        self.settings = Settings(**options)
        self.counts = {d.name: 0 for d in VERTICES}
        self.client: Any = SimpleNamespace(
            conn=SimpleNamespace(getVertexCount=Mock(side_effect=self.remote_counts)),
            gsql=Mock(return_value="\n".join(d.job for d in DATASETS)),
        )

    def remote_counts(self, *args: Any, **kwargs: Any) -> dict[str, int]:
        return dict(self.counts)

    def test_mutated_shard_is_rejected_before_network(self):
        (self.directory / "payments.psv").write_text("changed\n")
        with patch("tf_gnn_loader.mule.tigergraph.checked_client") as client:
            with self.assertRaises(RuntimeError):
                load(self.settings, self.directory)
            client.assert_not_called()

    def test_fresh_load_refuses_nonempty_graph(self):
        self.counts["Party"] = 1
        with patch(
            "tf_gnn_loader.mule.tigergraph.checked_client", return_value=self.client
        ):
            with self.assertRaisesRegex(RuntimeError, "populated graph"):
                load(self.settings, self.directory)
        self.assertFalse((self.directory / "tigergraph_load_state.json").exists())

    def test_completed_shards_are_resumed_without_upload(self):
        with (
            patch(
                "tf_gnn_loader.mule.tigergraph.checked_client", return_value=self.client
            ),
            patch(
                "tf_gnn_loader.mule.tigergraph.run_loading_job_with_file",
                return_value={"error": False, "validLine": 1},
            ) as upload,
        ):
            self.assertEqual(load(self.settings, self.directory)["loaded_shards"], 1)
            self.counts["Payment_Transaction"] = 1
            self.assertEqual(load(self.settings, self.directory)["skipped_shards"], 1)
            self.assertEqual(upload.call_count, 1)
            self.settings.host = "https://other.invalid"
            with self.assertRaisesRegex(RuntimeError, "another host"):
                load(self.settings, self.directory)

    def test_rejected_rows_do_not_mark_shard_complete(self):
        with (
            patch(
                "tf_gnn_loader.mule.tigergraph.checked_client", return_value=self.client
            ),
            patch(
                "tf_gnn_loader.mule.tigergraph.run_loading_job_with_file",
                return_value={"error": False, "invalidLine": 1},
            ),
        ):
            with self.assertRaises(RuntimeError):
                load(self.settings, self.directory)
        state = json.loads((self.directory / "tigergraph_load_state.json").read_text())
        self.assertEqual(state["completed_shards"], {})

    def test_concurrent_failure_checkpoints_success_and_stops_next_dataset(self):
        data = self.manifest["datasets"]["Payment_Transaction"]
        second = self.directory / "payments2.psv"
        second.write_text(
            "T2|2|USD|ach|digital|2024-01-01 00:00:00|1704067200000|2|true\n"
        )
        data["shards"].append(
            {
                "path": str(second),
                "rows": 1,
                "bytes": second.stat().st_size,
                "sha256": sha256(second),
            }
        )
        data["rows"] = 2
        data["bytes"] += second.stat().st_size
        self.manifest["audit"]["counts"]["Payment_Transaction"] = 2
        later = copy.deepcopy(data)
        later["loading_job"] = "mt_load_zelle_transfer"
        later["shards"] = [dict(data["shards"][0])]
        later_file = self.directory / "later.psv"
        later_file.write_bytes((self.directory / "payments.psv").read_bytes())
        later["shards"][0]["path"] = str(later_file)
        later["rows"] = 1
        later["bytes"] = later_file.stat().st_size
        self.manifest["datasets"]["Zelle_Transfer"] = later
        self.manifest["audit"]["counts"]["Zelle_Transfer"] = 1
        (self.directory / "export_manifest.json").write_text(json.dumps(self.manifest))
        together = Barrier(2, timeout=5)

        def send(*args: Any) -> None:
            self.assertEqual(args[4], "mt_load_payment_transaction")
            together.wait()
            if args[2].name == "payments.psv":
                raise RuntimeError("simulated upload failure")

        with (
            patch(
                "tf_gnn_loader.mule.tigergraph.checked_client", return_value=self.client
            ),
            patch(
                "tf_gnn_loader.mule.tigergraph.MuleSettings",
                return_value=SimpleNamespace(
                    mule_upload_workers=2, mule_upload_bytes=10_000_000
                ),
            ),
            patch("tf_gnn_loader.mule.tigergraph.upload_shard", side_effect=send),
        ):
            with self.assertRaisesRegex(RuntimeError, "simulated upload failure"):
                load(self.settings, self.directory)
        state = json.loads((self.directory / "tigergraph_load_state.json").read_text())
        self.assertEqual(
            set(state["completed_shards"]), {"Payment_Transaction/payments2.psv"}
        )

    def test_manifest_rejects_wrong_job_or_missing_dataset(self):
        self.manifest["datasets"]["Party"]["loading_job"] = "tfgnn_load_parties"
        (self.directory / "export_manifest.json").write_text(json.dumps(self.manifest))
        with self.assertRaises(RuntimeError):
            read_manifest(self.directory)
        self.manifest["datasets"].pop("Token")
        (self.directory / "export_manifest.json").write_text(json.dumps(self.manifest))
        with self.assertRaises(RuntimeError):
            read_manifest(self.directory)

    def test_upload_partitioning_preserves_every_byte_and_row(self):
        path = self.directory / "partition.psv"
        original = b"a|b|1\nc|d|2\ne|f|3\ng|h|4\n"
        path.write_bytes(original)
        uploaded: list[bytes] = []

        def send(*args: Any) -> dict[str, Any]:
            content = args[2].read_bytes()
            uploaded.append(content)
            return {"error": False, "validLine": content.count(b"\n")}

        with patch(
            "tf_gnn_loader.mule.tigergraph.run_loading_job_with_file", side_effect=send
        ):
            upload_shard(self.client, Mock(), path, 4, "job", 13)
        self.assertEqual(b"".join(uploaded), original)
        self.assertTrue(all(len(part) <= 13 for part in uploaded))
        self.assertEqual(path.read_bytes(), original)

    def test_upload_partition_rejects_wrong_row_total_before_sending(self):
        path = self.directory / "partition.psv"
        path.write_bytes(b"a|b|1\nc|d|2\n")
        with patch("tf_gnn_loader.mule.tigergraph.run_loading_job_with_file") as send:
            with self.assertRaisesRegex(RuntimeError, "row count"):
                upload_shard(self.client, Mock(), path, 99, "job", 7)
            send.assert_not_called()

    def test_license_limit_is_reported_as_capacity_block(self):
        response = Mock(spec=requests.Response)
        response.status_code = 403
        response.json.return_value = {
            "error": True,
            "message": "Exceeds the license limit, please check gadmin license status",
            "code": "SYS-0001",
        }
        error = requests.HTTPError("Forbidden", response=response)
        with patch(
            "tf_gnn_loader.mule.tigergraph.run_loading_job_with_file", side_effect=error
        ):
            with self.assertRaisesRegex(RuntimeError, "licensed capacity"):
                upload_shard(
                    self.client,
                    Mock(),
                    self.directory / "payments.psv",
                    1,
                    "job",
                    10_000_000,
                )


if __name__ == "__main__":
    unittest.main()
