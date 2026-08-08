import math
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from internal.executor.rpc_bridge import ERR_INVALID_PARAMS, RPCBridge
from internal.executor.runtime_contracts import (
    NODE_PROGRESS_CALLBACK_EXCLUSION,
    NodeProgressCallback,
)


class ExplodingJournal:
    def __getattr__(self, name):
        raise AssertionError(f"progress callback should not touch journal.{name}")


class NodeProgressContractTests(unittest.TestCase):
    def test_progress_callback_payload_is_log_only_no_overclaim(self):
        payload = NodeProgressCallback(
            task_id="task-a",
            node_id="node-a",
            progress=0.5,
            message="halfway",
            trace_id="trace-a",
        ).to_rpc("req-progress")

        self.assertEqual(payload["method"], "node.report_progress")
        params = payload["params"]
        self.assertNotIn("prompt_context_envelope", params)
        self.assertNotIn("runtime_event_id", params)
        self.assertNotIn("event_type", params)
        self.assertNotIn("authority_holder_node_id", params)

    def test_progress_callback_bridge_returns_explicit_exclusion(self):
        bridge = RPCBridge(executor=object(), journal=ExplodingJournal())
        response = bridge.handle_request({
            "jsonrpc": "2.0",
            "method": "node.report_progress",
            "params": {
                "task_id": " task-a ",
                "node_id": " node-a ",
                "progress": 0.5,
                "message": "halfway",
            },
            "id": "req-progress",
        })

        self.assertNotIn("error", response)
        result = response["result"]
        self.assertTrue(result["accepted"])
        self.assertEqual(result["classification"], NODE_PROGRESS_CALLBACK_EXCLUSION)
        self.assertFalse(result["classification"]["durable_runtime_event"])
        self.assertFalse(result["classification"]["prompt_context_envelope_required"])
        self.assertFalse(result["classification"]["accepted_as_agent_node_lifecycle_evidence"])

    def test_progress_callback_rejects_invalid_progress_values(self):
        bridge = RPCBridge(executor=object(), journal=ExplodingJournal())
        for bad_progress in (-0.01, 1.01, math.nan, math.inf):
            with self.subTest(bad_progress=bad_progress):
                response = bridge.handle_request({
                    "jsonrpc": "2.0",
                    "method": "node.report_progress",
                    "params": {
                        "task_id": "task-a",
                        "node_id": "node-a",
                        "progress": bad_progress,
                    },
                    "id": "req-progress",
                })
                self.assertEqual(response["error"]["code"], ERR_INVALID_PARAMS)
                self.assertIn("progress must be", response["error"]["message"])

    def test_progress_callback_rejects_type_laundering(self):
        bridge = RPCBridge(executor=object(), journal=ExplodingJournal())
        cases = [
            ("task_id", 123, "task_id must be a string"),
            ("node_id", 123, "node_id must be a string"),
            ("progress", True, "progress must be a number"),
            ("progress", "0.5", "progress must be a number"),
            ("progress", "-inf", "progress must be a number"),
        ]
        for field, value, message in cases:
            params = {"task_id": "task-a", "node_id": "node-a", "progress": 0.2}
            params[field] = value
            with self.subTest(field=field, value=value):
                response = bridge.handle_request({
                    "jsonrpc": "2.0",
                    "method": "node.report_progress",
                    "params": params,
                    "id": "req-progress",
                })
                self.assertEqual(response["error"]["code"], ERR_INVALID_PARAMS)
                self.assertIn(message, response["error"]["message"])

    def test_progress_callback_rejects_blank_identity(self):
        bridge = RPCBridge(executor=object(), journal=ExplodingJournal())
        for field in ("task_id", "node_id"):
            params = {"task_id": "task-a", "node_id": "node-a", "progress": 0.2}
            params[field] = "   "
            with self.subTest(field=field):
                response = bridge.handle_request({
                    "jsonrpc": "2.0",
                    "method": "node.report_progress",
                    "params": params,
                    "id": "req-progress",
                })
                self.assertEqual(response["error"]["code"], ERR_INVALID_PARAMS)
                self.assertIn(f"{field} is required", response["error"]["message"])

    def test_progress_callback_rejects_non_object_params_as_invalid_params(self):
        bridge = RPCBridge(executor=object(), journal=ExplodingJournal())
        response = bridge.handle_request({
            "jsonrpc": "2.0",
            "method": "node.report_progress",
            "params": ["task-a", "node-a", 0.2],
            "id": "req-progress",
        })

        self.assertEqual(response["error"]["code"], ERR_INVALID_PARAMS)
        self.assertIn("params must be an object", response["error"]["message"])


if __name__ == "__main__":
    unittest.main()
