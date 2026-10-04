"""Regression coverage for schema identity, artifact integrity and resumption."""

import copy
import json
import os
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
    BY_NAME,
    DATASETS,
    FORMAT_VERSION,
    GRAPH,
    GSQL,
    MPL_SCOPE_EDGES,
    MPL_SCOPE_VERTEX,
    ROOT,
    VERTICES,
    visible,
)
from tf_gnn_loader.mule.gsql import loading_jobs, verify_query
from tf_gnn_loader.mule.postgres import copy_query, invalid_source_values, view_query
from tf_gnn_loader.mule.tigergraph import (
    load,
    read_manifest,
    upload_shard,
    validate_schema,
    verify,
)
from tf_gnn_loader.tigergraph.loading import sha256
from tf_gnn_loader.tigergraph.settings import Settings


# MulePatternLearner's Account label contract (its docs/reference/labels.md,
# "Loading accounts"), in load order, with the types of its schema.gsql.
ACCOUNT_CONTRACT = (
    ("id", "STRING"),
    ("account_type", "STRING"),
    ("is_external", "BOOL"),
    ("first_seen_seq", "UINT"),
    ("first_seen_ts_ms", "UINT"),
    ("is_mule", "INT"),
    ("mule_label_known", "BOOL"),
    ("is_mule_masked", "BOOL"),
    ("pu_label", "INT"),
    ("mule_label_effective_seq", "UINT"),
    ("mule_label_effective_ts_ms", "UINT"),
    ("mule_label_available_seq", "UINT"),
    ("mule_label_available_ts_ms", "UINT"),
    ("mule_ring_id", "INT"),
    ("mule_label_source", "STRING"),
)
ACCOUNT_JOB = (
    "Account VALUES ($0, $1, $2, $3, $4, $6, $7, $8, $9, $10, $11, $12, $13, $14, $5)"
)
# The sibling MulePatternLearner checkout, read only, for the identity tests.
MPL = Path(os.getenv("MULE_PATTERN_LEARNER_DIR") or ROOT.parent / "MulePatternLearner")


def mpl_text(relative: str) -> str:
    path = MPL / relative
    if not path.is_file():
        raise unittest.SkipTest(
            f"No MulePatternLearner checkout at {MPL}; set MULE_PATTERN_LEARNER_DIR"
        )
    return path.read_text()


def ddl_schema(ddl: str) -> dict[str, Any]:
    """The getSchema answer of a graph created by running fresh-graph DDL."""
    source = re.sub(r"/\*.*?\*/", "", ddl, flags=re.S)
    graph = re.search(r"CREATE GRAPH (\w+)", source)
    assert graph is not None
    schema: dict[str, Any] = {
        "GraphName": graph.group(1),
        "VertexTypes": [],
        "EdgeTypes": [],
    }
    for kind, name, body, options in re.findall(
        r"ADD (VERTEX|DIRECTED EDGE) (\w+)\s*\((.*?)\) WITH ([^;]*);", source, re.S
    ):
        item: dict[str, Any] = {"Name": name, "Attributes": []}
        for part in (p.strip() for p in body.split(",")):
            if endpoint := re.fullmatch(r"(FROM|TO) (\w+)", part):
                key = (
                    "FromVertexTypeName"
                    if endpoint[1] == "FROM"
                    else "ToVertexTypeName"
                )
                item[key] = endpoint[2]
                continue
            declared = re.fullmatch(
                r"(PRIMARY_ID |DISCRIMINATOR\()?(\w+) (LIST<\w+>|\w+)\)?(?: DEFAULT .*)?",
                part,
            )
            assert declared is not None, part
            prefix, attribute, kind_name = declared.groups()
            value = re.fullmatch(r"LIST<(\w+)>", kind_name)
            attribute_type: dict[str, Any] = (
                {"Name": "LIST", "ValueTypeName": value[1]}
                if value
                else {"Name": kind_name}
            )
            entry: dict[str, Any] = {
                "AttributeName": attribute,
                "AttributeType": attribute_type,
            }
            if prefix == "PRIMARY_ID ":
                item["PrimaryId"] = {
                    **entry,
                    "PrimaryIdAsAttribute": 'PRIMARY_ID_AS_ATTRIBUTE="true"' in options,
                }
            else:
                if prefix:
                    entry["IsDiscriminator"] = True
                item["Attributes"].append(entry)
        if kind == "VERTEX":
            schema["VertexTypes"].append(item)
        else:
            reverse = re.search(r'REVERSE_EDGE="(\w+)"', options)
            item["Config"] = {"REVERSE_EDGE": reverse[1] if reverse else ""}
            schema["EdgeTypes"].append(item)
    return schema


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
            "account_mule_flags": {},
            "account_mule_rings": {},
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
        self.assertIn(ACCOUNT_JOB, loading_jobs())
        self.assertIn("$8, _, _, _, _, _, _, _, _)", loading_jobs())

    def test_account_table_is_the_fifteen_column_label_contract(self):
        account = BY_NAME["Account"]
        self.assertEqual(account.fields, ACCOUNT_CONTRACT)
        self.assertEqual(account.nullable, ("mule_label_source",))
        self.assertEqual(
            [d.name for d in DATASETS if d.nullable and d.name != "Account"], []
        )
        self.assertGreaterEqual(FORMAT_VERSION, 5)

    def test_account_storage_order_stores_is_mule_last(self):
        stored = BY_NAME["Account"].graph_fields
        self.assertEqual(
            stored, ACCOUNT_CONTRACT[:5] + ACCOUNT_CONTRACT[6:] + ACCOUNT_CONTRACT[5:6]
        )
        self.assertEqual(sorted(stored), sorted(ACCOUNT_CONTRACT))

    def test_account_job_loads_every_column_and_no_default(self):
        job = re.search(
            r"CREATE LOADING JOB mt_load_account .*?VALUES \((.*?)\)",
            loading_jobs(),
            re.S,
        )
        assert job is not None
        values = [v.strip() for v in job.group(1).split(",")]
        self.assertNotIn("_", values)
        self.assertEqual(sorted(int(v.lstrip("$")) for v in values), list(range(15)))
        self.assertIn(ACCOUNT_JOB, job.group(0))

    def test_every_loaded_attribute_reads_its_own_column(self):
        for d in DATASETS:
            job = re.search(
                rf"TO (?:VERTEX|EDGE) {d.name} VALUES \((.*?)\)", loading_jobs()
            )
            assert job is not None
            values = [v.strip() for v in job.group(1).split(",")]
            self.assertEqual(len(values), len(d.graph_fields))
            for (name, _), value in zip(d.graph_fields, values):
                if value == "_":
                    self.assertNotIn(name, d.columns)
                else:
                    self.assertEqual(d.columns[int(value.lstrip("$"))], name)

    def test_account_job_maps_like_mpl_load_accounts(self):
        mpl = mpl_text("gsql/schema/account_loading.gsql")
        header = re.search(r"DEFINE HEADER \w+ = (.*?);", mpl, re.S)
        values = re.search(r"TO VERTEX Account VALUES \((.*?)\)", mpl, re.S)
        assert header is not None and values is not None
        names = re.findall(r'"(\w+)"', header.group(1))
        self.assertEqual(tuple(names), BY_NAME["Account"].columns)
        positions = [names.index(n) for n in re.findall(r'\$"(\w+)"', values.group(1))]
        self.assertIn(
            "Account VALUES (" + ", ".join(f"${i}" for i in positions) + ")",
            loading_jobs(),
        )

    def test_schema_is_mpl_fresh_graph_ddl(self):
        self.assertEqual(
            (GSQL / "schema.gsql").read_text(), mpl_text("gsql/schema/schema.gsql")
        )

    def test_graph_from_the_ddl_passes_the_push_schema_check(self):
        schema = ddl_schema((GSQL / "schema.gsql").read_text())
        validate_schema(schema)
        self.assertEqual(
            [len(schema["VertexTypes"]), len(schema["EdgeTypes"])], [8, 19]
        )

    def test_schema_check_permits_the_mpl_scope_and_nothing_else(self):
        schema = schema_fixture()
        scope = copy.deepcopy(schema)
        scope["VertexTypes"].append({"Name": MPL_SCOPE_VERTEX, "Attributes": []})
        scope["EdgeTypes"] += [{"Name": name} for name in MPL_SCOPE_EDGES]
        validate_schema(scope)
        bad = copy.deepcopy(schema)
        bad["VertexTypes"].append({"Name": "Other", "Attributes": []})
        with self.assertRaisesRegex(RuntimeError, "vertex types"):
            validate_schema(bad)
        bad = copy.deepcopy(schema)
        bad["EdgeTypes"].append({"Name": "Other_Edge"})
        with self.assertRaisesRegex(RuntimeError, "edge types"):
            validate_schema(bad)

    def test_mpl_scope_names_match_its_scope_ddl(self):
        scope = re.sub(
            r"/\*.*?\*/", "", mpl_text("gsql/schema/scope_vertex.gsql"), flags=re.S
        )
        self.assertEqual(re.findall(r"ADD VERTEX (\w+)", scope), [MPL_SCOPE_VERTEX])
        self.assertEqual(
            tuple(
                re.findall(r"ADD DIRECTED EDGE (\w+)", scope)
                + re.findall(r'REVERSE_EDGE="(\w+)"', scope)
            ),
            MPL_SCOPE_EDGES,
        )

    def test_every_flag_loads_like_is_external(self):
        # PhantomLedger renders True and False; every BOOL takes one path.
        for d in DATASETS:
            for name, kind in d.fields:
                if kind != "BOOL":
                    continue
                self.assertIn(
                    f"lower(\"{name}\") NOT IN ('true','false')",
                    invalid_source_values(d),
                )
                self.assertIn(f'"{name}"::boolean AS "{name}"', view_query(d))
                self.assertIn(
                    f"CASE WHEN {name} THEN 'true' ELSE 'false' END", copy_query(d)
                )
        flags = [n for n, k in BY_NAME["Account"].fields if k == "BOOL"]
        self.assertEqual(flags, ["is_external", "mule_label_known", "is_mule_masked"])

    def test_only_the_label_source_may_be_null_and_loads_empty(self):
        account = BY_NAME["Account"]
        checks = invalid_source_values(account)
        for name in account.columns:
            self.assertEqual(f'"{name}" IS NULL' in checks, name != "mule_label_source")
        self.assertIn(
            'coalesce("mule_label_source", \'\')::text AS "mule_label_source"',
            view_query(account),
        )
        self.assertIn("NULL ''", copy_query(account))

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

    def test_fresh_load_refuses_a_graph_that_keeps_mpl_scope_vertices(self):
        self.counts[MPL_SCOPE_VERTEX] = 2
        with patch(
            "tf_gnn_loader.mule.tigergraph.checked_client", return_value=self.client
        ):
            with self.assertRaisesRegex(RuntimeError, MPL_SCOPE_VERTEX + "=2"):
                load(self.settings, self.directory)

    def test_export_of_another_format_version_is_refused(self):
        self.manifest["format_version"] = 4
        (self.directory / "export_manifest.json").write_text(json.dumps(self.manifest))
        with self.assertRaisesRegex(RuntimeError, "format version 4.*new, empty"):
            read_manifest(self.directory)

    def test_verify_requires_every_ring_id_to_arrive(self):
        rings = {"-1": 5, "0": 2, "3": 1}
        self.manifest["audit"].update(
            account_mule_flags={"0": 5, "1": 3}, account_mule_rings=rings
        )
        (self.directory / "export_manifest.json").write_text(json.dumps(self.manifest))
        result: dict[str, Any] = {
            "counts": {"Payment_Transaction": 1},
            "account_mule_flags": {"0": 5, "1": 3},
            "account_mule_rings": dict(rings),
            "violations": 0,
        }
        self.client.run_installed_with_timeout = Mock(return_value=[result])
        with patch(
            "tf_gnn_loader.mule.tigergraph.checked_client", return_value=self.client
        ):
            self.assertEqual(
                verify(self.settings, self.directory)["account_mule_rings"], rings
            )
            # The six-column job left every ring at its default.
            result["account_mule_rings"] = {"-1": 8}
            with self.assertRaisesRegex(RuntimeError, "account_mule_rings"):
                verify(self.settings, self.directory)
            del result["account_mule_rings"]
            with self.assertRaisesRegex(RuntimeError, "incomplete"):
                verify(self.settings, self.directory)

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
