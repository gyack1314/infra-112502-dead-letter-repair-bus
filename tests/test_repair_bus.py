"""死信修复中间件的关键因果、幂等、环和持久化测试。"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from repair_bus.engine import RepairBus
from repair_bus.model import new_state
from repair_bus.replay import replay_file
from repair_bus.simulation import LocalConsumerAdapter
from repair_bus.store import StateStore


class RepairBusTest(unittest.TestCase):
    def setUp(self) -> None:
        self.bus = RepairBus(new_state())
        self.consumer = LocalConsumerAdapter(self.bus)

    def test_normal_consumption_releases_dependency_for_named_group(self) -> None:
        self.bus.publish("stock", "take", 1, ["ledger"])
        self.bus.publish("work", "finish", 1, ["schedule"], ["ledger/take@1"])

        self.assertEqual(self.bus.delivery_view("schedule/finish@1")["phase"], "waiting")
        self.consumer.consume("ledger", "take", attempt="a1")

        released = self.bus.release(group="schedule", command="finish")
        self.assertEqual(released["released"], ["schedule/finish@1"])
        view = self.bus.delivery_view("schedule/finish@1")
        self.assertEqual(view["phase"], "ready")
        self.assertIn("ledger/take@1", view["seen_facts"])

        result = self.consumer.consume("schedule", "finish", attempt="b1")
        self.assertEqual(result["fact_status"], "applied")
        self.assertEqual(self.bus.delivery_view("schedule/finish@1")["phase"], "done")

    def test_other_group_progress_cannot_sign_for_required_group(self) -> None:
        self.bus.publish("k", "needs-stock", 1, ["schedule"], ["stock/allocate@1"])
        # schedule 组虽然也处理过同名来源，但不能代 stock/allocate 签名。
        self.bus.report_effect("schedule", "allocate", 1, "applied", attempt="x", effect_id="e-schedule")

        view = self.bus.delivery_view("schedule/needs-stock@1")
        codes = {b["code"] for b in view["blockers"]}
        self.assertIn("missing_credential", codes)
        self.assertFalse(view["can_release"])

        self.bus.report_effect("stock", "allocate", 1, "applied", attempt="y", effect_id="e-stock")
        self.assertTrue(self.bus.delivery_view("schedule/needs-stock@1")["can_release"])

    def test_unknown_result_applied_reuses_old_effect_without_second_side_effect(self) -> None:
        self.bus.publish("stock", "take", 1, ["ledger"])
        self.bus.publish("work", "finish", 1, ["schedule"], ["ledger/take@1"])
        self.consumer.consume("ledger", "take", attempt="a1", fail="unknown")
        self.assertEqual(len(self.bus.pending_receipts()), 1)

        self.bus.report_effect("ledger", "take", 1, "applied", attempt="a1", effect_id="h1")
        self.assertEqual(self.bus.pending_receipts(), [])
        view = self.bus.delivery_view("ledger/take@1")
        self.assertEqual(view["phase"], "done")
        self.assertEqual(view["fact_status"], "applied")

        duplicate = self.bus.claim("ledger", "take", attempt="a2")
        self.assertEqual(duplicate["decision"], "continue_original")
        self.assertEqual(duplicate["effect_id"], "h1")
        # 没有生成第二个外部 effect 证据。
        effect_ids = [f.get("effect_id") for f in self.bus.state["facts"].values()]
        self.assertEqual(effect_ids.count("h1"), 1)

        released = self.bus.release(command="finish")
        self.assertEqual(released["released"], ["schedule/finish@1"])

    def test_not_applied_receipt_allows_safe_new_attempt(self) -> None:
        self.bus.publish("k", "q", 1, ["g"])
        self.bus.claim("g", "q", attempt="a1")
        self.bus.mark_dead(attempt="a1", dead_reason="unknown")
        self.bus.report_effect("g", "q", 1, "not_applied", attempt="a1")

        view = self.bus.delivery_view("g/q@1")
        self.assertTrue(view["can_claim"])
        result = self.consumer.consume("g", "q", attempt="a2")
        self.assertEqual(result["fact_status"], "applied")
        self.assertEqual(len(self.bus.state["deliveries"]["g/q@1"]["attempts"]), 2)

    def test_effect_amendment_requires_compensation_then_source_retarget(self) -> None:
        self.bus.publish("stock", "take", 1, ["ledger"])
        self.bus.publish("work", "finish", 1, ["schedule"], ["ledger/take@1"])
        self.bus.claim("ledger", "take", attempt="a1")
        self.bus.mark_dead(attempt="a1", dead_reason="unknown")
        self.bus.create_repair("r1", ["take", "finish"])
        self.bus.create_repair("r2", ["take"])
        self.bus.amend("take", 2, "effect")
        self.bus.report_effect("ledger", "take", 1, "applied", attempt="a1", effect_id="h1")

        report = self.bus.repair_report("r1")
        self.assertEqual(report["decision"], "compensate")
        self.assertEqual(report["compensations"][0]["status"], "requested")
        self.assertEqual(report["compensations"][0]["repair_ids"], ["r1", "r2"])
        self.assertFalse(self.bus.delivery_view("ledger/take@2")["can_release"])

        dead_before = self.bus.dead_report(command="take")["dead_letters"][0]
        self.assertEqual(dead_before["decision"], "compensate")

        self.bus.report_compensated("h1", "undo-1", command="take")
        dead_after = self.bus.dead_report(command="take")["dead_letters"][0]
        self.assertEqual(dead_after["decision"], "source_retarget_or_withdraw")
        self.assertIn("ledger/take@2", dead_after["releasable"])

        take2 = self.bus.delivery_view("ledger/take@2")
        self.assertTrue(take2["can_release"])
        self.assertEqual(take2["next_action"], "release_and_claim")

        finish = self.bus.delivery_view("schedule/finish@1")
        codes = {b["code"] for b in finish["blockers"]}
        self.assertIn("obsolete_dependency", codes)
        self.assertIn("compensated_credential", codes)
        self.assertEqual(finish["next_action"], "source_retarget_or_withdraw")

        # 旧确认不能成为 take@2 的完成凭据。
        self.assertNotIn("ledger/take@2", self.bus.state["facts"])

        self.bus.retarget_dependency(did="schedule/finish@1", new_ref="ledger/take@2")
        self.consumer.consume("ledger", "take", rev=2, attempt="t2")
        self.bus.release(group="schedule", command="finish")
        result = self.consumer.consume("schedule", "finish", attempt="f1")
        self.assertEqual(result["fact_status"], "applied")

    def test_context_amendment_uses_new_revision_but_allows_same_external_effect_receipt(self) -> None:
        self.bus.publish("k", "move", 1, ["stock"])
        self.bus.publish("w", "after-move", 1, ["work"], ["stock/move@1"])
        self.consumer.consume("stock", "move", attempt="m1")

        self.bus.amend("move", 2, "context")
        downstream = self.bus.delivery_view("work/after-move@1")
        self.assertEqual(downstream["next_action"], "source_retarget_or_withdraw")

        self.bus.retarget_dependency(did="work/after-move@1", new_ref="stock/move@2")
        # 外部适配器按同一业务影响查到旧 effect_id，但必须针对新 rev 再出回执，
        # 系统不会直接把 @1 确认冒认为 @2 凭据。
        self.bus.report_effect("stock", "move", 2, "applied", attempt="m2-query", effect_id="move-h1")
        self.assertTrue(self.bus.delivery_view("work/after-move@1")["can_release"])

    def test_cycle_blocks_only_cycle_and_branch_but_not_unrelated_key(self) -> None:
        self.bus.publish("A", "a", 1, ["ledger"], ["schedule/b@1"])
        self.bus.publish("B", "b", 1, ["schedule"], ["ledger/a@1"])
        self.bus.publish("C", "c", 1, ["ledger"])
        self.bus.create_repair("cycle", ["a", "b", "c"])
        self.consumer.consume("ledger", "c", attempt="c1")

        report = self.bus.repair_report("cycle")
        self.assertEqual(report["cycles"], [["ledger/a@1", "schedule/b@1"]])
        self.assertIn("schedule/b@1", report["cycle_blocked_branches"] + report["cycle_nodes"])
        self.assertEqual(report["unaffected"], ["ledger/c@1"])
        self.assertIn("ledger/c@1", report["do_not_repeat"][0]["id"])
        self.assertFalse(self.bus.delivery_view("ledger/a@1")["can_release"])
        self.assertTrue(self.bus.delivery_view("ledger/c@1")["can_continue_original"])

        # 其他无关键在环修复期间仍可正常消费。
        self.bus.publish("D", "d", 1, ["ledger"])
        done = self.consumer.consume("ledger", "d", attempt="d1")
        self.assertEqual(done["fact_status"], "applied")

    def test_withdraw_or_retarget_breaks_cycle(self) -> None:
        self.bus.publish("A", "a", 1, ["ledger"], ["schedule/b@1"])
        self.bus.publish("B", "b", 1, ["schedule"], ["ledger/a@1"])
        self.bus.publish("C", "c", 1, ["ledger"])
        self.consumer.consume("ledger", "c", attempt="c1")
        self.bus.create_repair("cycle", ["a", "b", "c"])

        self.bus.withdraw(did="schedule/b@1")
        view = self.bus.delivery_view("ledger/a@1")
        self.assertNotIn("dependency_cycle", {x["code"] for x in view["blockers"]})
        self.assertEqual(view["next_action"], "source_retarget_or_withdraw")

        self.bus.retarget_dependency(did="ledger/a@1", new_ref="ledger/c@1")
        self.assertTrue(self.bus.delivery_view("ledger/a@1")["can_release"])
        result = self.consumer.consume("ledger", "a", attempt="a1")
        self.assertEqual(result["fact_status"], "applied")

    def test_overlapping_compensation_confirmation_is_idempotent(self) -> None:
        self.bus.publish("k", "fastener", 1, ["stock"])
        self.bus.create_repair("op1", ["fastener"])
        self.bus.create_repair("op2", ["fastener"])
        self.bus.report_effect("stock", "fastener", 1, "applied", attempt="a1", effect_id="h")
        self.bus.amend("fastener", 2, "effect")
        self.bus.request_compensation("h", repair_id="op1")
        self.bus.request_compensation("h", repair_id="op2")
        self.assertEqual(len(self.bus.state["compensation_actions"]), 1)

        first = self.bus.report_compensated("h", "undo-h")
        second = self.bus.report_compensated("h", "undo-h")
        self.assertNotIn("idempotent", first)
        self.assertTrue(second["idempotent"])
        with self.assertRaises(ValueError):
            self.bus.report_compensated("h", "different-payout")

    def test_new_dependent_on_unresolved_chain_waits_but_other_keys_ready(self) -> None:
        self.bus.publish("k", "root", 1, ["g"])
        self.bus.claim("g", "root", attempt="r1")
        self.bus.mark_dead(attempt="r1", dead_reason="unknown")
        self.bus.publish("k2", "waiting-new", 1, ["g2"], ["g/root@1"])
        self.bus.publish("other", "free", 1, ["g3"])

        self.assertFalse(self.bus.delivery_view("g2/waiting-new@1")["can_release"])
        self.assertTrue(self.bus.delivery_view("g3/free@1")["can_claim"])

    def test_pending_receipt_survives_restart_from_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "state.json"
            store = StateStore(path)
            bus = RepairBus(store.load())
            bus.publish("k", "q", 1, ["g"])
            bus.claim("g", "q", attempt="a1")
            bus.mark_dead(attempt="a1", dead_reason="unknown")
            store.save(bus.state)

            restarted = RepairBus(store.load())
            self.assertEqual([r["id"] for r in restarted.pending_receipts()], ["a1"])
            restarted.report_effect("g", "q", 1, "applied", attempt="a1", effect_id="h")
            store.save(restarted.state)

            twice = RepairBus(StateStore(path).load())
            self.assertEqual(twice.pending_receipts(), [])
            self.assertEqual(twice.state["facts"]["g/q@1"]["effect_id"], "h")

    def test_provided_examples_replay_and_inspect_without_errors(self) -> None:
        root = Path(__file__).resolve().parent.parent / "examples"
        with tempfile.TemporaryDirectory() as temp:
            store = StateStore(Path(temp) / "unknown.json")
            reports = replay_file(root / "unknown-effect-then-amend.jsonl", store, output=None)
            repair = next(r for r in reports if r.get("report") == "repair")
            self.assertEqual(repair["id"], "r1")
            self.assertEqual(repair["compensations"][0]["effect_id"], "h1")
            self.assertIn("r2", repair["overlapping_repairs"])

            store2 = StateStore(Path(temp) / "cycle.json")
            cycle_reports = replay_file(root / "cycle-with-unrelated-work.jsonl", store2, output=None)
            cycle = cycle_reports[0]
            self.assertEqual(cycle["cycles"], [["ledger/a@1", "schedule/b@1"]])
            self.assertEqual(cycle["unaffected"], ["ledger/c@1"])

    def test_restart_event_in_jsonl_reloads_pending_receipts(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            scenario = Path(temp) / "trace.jsonl"
            rows = [
                {"at": 0, "kind": "publish", "record": {"key": "k", "command": "q", "rev": 1, "consumers": ["g"], "needs": []}},
                {"at": 1, "kind": "claim", "record": {"group": "g", "command": "q", "attempt": "a1"}},
                {"at": 2, "kind": "dead", "record": {"group": "g", "command": "q", "attempt": "a1", "dead_reason": "unknown"}},
                {"at": 3, "kind": "restart", "record": {}},
                {"at": 4, "kind": "effect", "record": {"group": "g", "command": "q", "rev": 1, "attempt": "a1", "state": "applied", "effect_id": "h"}},
            ]
            scenario.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")
            # message_types.json is discovered next to the scenario.
            (Path(temp) / "message_types.json").write_text(
                json.dumps(["publish", "claim", "dead", "effect", "restart"]), encoding="utf-8"
            )
            store = StateStore(Path(temp) / "state.json")
            replay_file(scenario, store, output=None)
            state = store.load()
            self.assertEqual(state["facts"]["g/q@1"]["status"], "applied")
            self.assertTrue(any(e["kind"] == "restart" for e in state["audit"]))


if __name__ == "__main__":
    unittest.main()
