"""可控死信修复通道的核心状态机。

所有方法只修改普通 dict 状态；CLI/重放器负责原子持久化。这样同一套 API
既能逐行回放 JSONL，也能被测试或其他运行入口直接调用。
"""

from __future__ import annotations

from typing import Any

from .errors import BlockedOperation, RepairBusError
from .graph import cycle_index
from .model import (
    AMEND_CHANGED,
    DEAD_REASONS,
    EFFECT_STATES,
    delivery_id,
    parse_ref,
)


class RepairBus:
    def __init__(self, state: dict[str, Any]):
        self.state = state

    # ---------- 基础工具 ----------

    def audit(self, kind: str, detail: dict[str, Any], at: int | None = None) -> None:
        event_at = self.state.get("last_at", 0) if at is None else at
        self.state["last_at"] = max(self.state.get("last_at", 0), event_at)
        self.state.setdefault("audit", []).append({"at": event_at, "kind": kind, **detail})

    def _next_id(self, name: str) -> int:
        counters = self.state.setdefault("counters", {})
        counters[name] = int(counters.get(name, 0)) + 1
        return counters[name]

    def _group_view(self, group: str) -> dict[str, Any]:
        return self.state["group_views"].setdefault(group, {"seen": {}, "created_at": self.state.get("last_at", 0)})

    def _rev_record(self, command: str, rev: int) -> dict[str, Any] | None:
        entry = self.state["commands"].get(command)
        if not entry:
            return None
        return entry.get("revs", {}).get(str(rev))

    def _find_delivery_id(
        self,
        group: str | None = None,
        command: str | None = None,
        rev: int | None = None,
        attempt: str | None = None,
        delivery_ref: str | None = None,
    ) -> str:
        if attempt is not None:
            did = self.state["attempt_index"].get(attempt)
            if not did:
                raise RepairBusError(f"未知 attempt：{attempt}")
            if delivery_ref and did != delivery_ref:
                raise RepairBusError(f"attempt {attempt} 属于 {did}，不是 {delivery_ref}")
            return did

        if delivery_ref is not None:
            if delivery_ref not in self.state["deliveries"]:
                raise RepairBusError(f"未知投递：{delivery_ref}")
            return delivery_ref

        if not group or not command:
            raise RepairBusError("定位投递需要 group/command，或 attempt/delivery_ref")

        if rev is None:
            entry = self.state["commands"].get(command)
            if not entry:
                raise RepairBusError(f"未知命令：{command}")
            rev = int(entry["current_rev"])

        did = delivery_id(group, command, rev)
        if did not in self.state["deliveries"]:
            # 若调用方未指定 rev，允许在该 group/command 的所有修订中找最新存在者。
            candidates = [
                key
                for key, value in self.state["deliveries"].items()
                if value.get("group") == group and value.get("command") == command
            ]
            if not candidates:
                raise RepairBusError(f"找不到消费投递：{group}/{command}")
            did = sorted(candidates, key=lambda x: self.state["deliveries"][x]["rev"])[-1]
        return did

    # ---------- 发布与更正 ----------

    def publish(
        self,
        key: str,
        command: str,
        rev: int,
        consumers: list[str],
        needs: list[str] | None = None,
        at: int | None = None,
        **extra: Any,
    ) -> dict[str, Any]:
        if not isinstance(key, str) or not key:
            raise RepairBusError("publish 缺少非空 key")
        if not isinstance(command, str) or not command:
            raise RepairBusError("publish 缺少非空 command")
        if type(rev) is not int or rev <= 0:
            raise RepairBusError("rev 必须是正整数")
        if not isinstance(consumers, list) or not all(isinstance(x, str) and x for x in consumers):
            raise RepairBusError("consumers 必须是非空字符串数组")
        needs = list(needs or [])
        parsed_needs = [str(parse_ref(item)) for item in needs]

        entry = self.state["commands"].setdefault(
            command,
            {"key": key, "current_rev": rev, "revs": {}},
        )
        existing = entry["revs"].get(str(rev))
        core = {
            "key": key,
            "consumers": list(consumers),
            "needs": parsed_needs,
        }
        if existing:
            same = (
                existing.get("key") == key
                and list(existing.get("consumers", [])) == list(consumers)
                and list(existing.get("needs", [])) == parsed_needs
            )
            if not same:
                raise RepairBusError(f"{command}@{rev} 已存在且发布契约不一致")
            self.audit("publish_idempotent", {"ref": f"{command}@{rev}"}, at)
            return {"idempotent": True, "delivery_ids": [delivery_id(g, command, rev) for g in consumers]}

        entry["key"] = key
        entry["revs"][str(rev)] = {
            "command": command,
            "rev": rev,
            "key": key,
            "consumers": list(consumers),
            "needs": parsed_needs,
            "source": "publish",
            "changed": None,
            "correction_of": None,
            "extra": extra,
            "published_at": self.state.get("last_at", 0) if at is None else at,
        }
        entry["current_rev"] = max(int(entry.get("current_rev", rev)), rev)

        delivery_ids: list[str] = []
        for group in consumers:
            self._group_view(group)
            did = delivery_id(group, command, rev)
            if did not in self.state["deliveries"]:
                self.state["deliveries"][did] = {
                    "id": did,
                    "key": key,
                    "group": group,
                    "command": command,
                    "rev": rev,
                    "needs": parsed_needs,
                    "phase": "waiting",
                    "attempts": [],
                    "current_attempt": None,
                    "dead": None,
                    "effect_id": None,
                    "source_decisions": [],
                    "created_at": self.state.get("last_at", 0) if at is None else at,
                }
            delivery_ids.append(did)

        self.audit(
            "publish",
            {"key": key, "command": command, "rev": rev, "delivery_ids": delivery_ids},
            at,
        )
        self.refresh_phases()
        return {"delivery_ids": delivery_ids}

    def amend(
        self,
        command: str,
        rev: int,
        changed: str,
        at: int | None = None,
        **extra: Any,
    ) -> dict[str, Any]:
        if changed not in AMEND_CHANGED:
            raise RepairBusError(f"changed 必须是 {AMEND_CHANGED} 之一")
        if type(rev) is not int or rev <= 0:
            raise RepairBusError("amend rev 必须是正整数")
        entry = self.state["commands"].get(command)
        if not entry:
            raise RepairBusError(f"更正了未发布的命令：{command}")

        if str(rev) in entry["revs"]:
            record = entry["revs"][str(rev)]
            if record.get("source") != "amend" or record.get("changed") != changed:
                raise RepairBusError(f"{command}@{rev} 已存在且更正契约不一致")
            return {"idempotent": True, "rev": rev}

        lower = [int(x) for x in entry["revs"] if int(x) < rev]
        predecessor_rev = extra.pop("correction_of", None)
        if predecessor_rev is None:
            predecessor_rev = max(lower) if lower else rev - 1
        predecessor = entry["revs"].get(str(predecessor_rev))
        if predecessor is None:
            raise RepairBusError(f"{command}@{rev} 找不到前序修订 {predecessor_rev}")

        key = extra.pop("key", predecessor["key"])
        consumers = list(extra.pop("consumers", predecessor.get("consumers", [])))
        needs = [str(parse_ref(x)) for x in extra.pop("needs", predecessor.get("needs", []))]

        entry["revs"][str(rev)] = {
            "command": command,
            "rev": rev,
            "key": key,
            "consumers": consumers,
            "needs": needs,
            "source": "amend",
            "changed": changed,
            "correction_of": predecessor_rev,
            "extra": extra,
            "published_at": self.state.get("last_at", 0) if at is None else at,
        }
        entry["current_rev"] = max(int(entry.get("current_rev", rev)), rev)

        delivery_ids: list[str] = []
        for group in consumers:
            self._group_view(group)
            did = delivery_id(group, command, rev)
            if did not in self.state["deliveries"]:
                self.state["deliveries"][did] = {
                    "id": did,
                    "key": key,
                    "group": group,
                    "command": command,
                    "rev": rev,
                    "needs": needs,
                    "phase": "waiting",
                    "attempts": [],
                    "current_attempt": None,
                    "dead": None,
                    "effect_id": None,
                    "source_decisions": [],
                    "created_at": self.state.get("last_at", 0) if at is None else at,
                }
            delivery_ids.append(did)

        # 更正可能改变旧 applied 凭据的治理状态。
        for ref, fact in list(self.state["facts"].items()):
            if fact.get("command") == command:
                self._refresh_fact_status(ref)

        self.audit(
            "amend",
            {"command": command, "rev": rev, "changed": changed, "delivery_ids": delivery_ids},
            at,
        )
        self.refresh_phases()

        # 样例中的修复申请通常先于回执；若已有需要补偿的影响，立刻并入共享补偿动作。
        if changed == "effect":
            covering = [rid for rid, repair in self.state["repairs"].items() if command in repair.get("commands", [])]
            for ref, fact in self.state["facts"].items():
                if fact.get("command") == command and fact.get("status") == "compensation_required":
                    result = self.request_compensation(fact["effect_id"], repair_id=covering[0] if covering else None, at=at)
                    action = result["action"]
                    for rid in covering:
                        if rid not in action.setdefault("repair_ids", []):
                            action["repair_ids"].append(rid)
        return {"delivery_ids": delivery_ids}

    # ---------- 消费领取、死信与外部回执 ----------

    def claim(
        self,
        group: str,
        command: str,
        attempt: str | None = None,
        rev: int | None = None,
        at: int | None = None,
        **extra: Any,
    ) -> dict[str, Any]:
        if attempt and attempt in self.state["attempt_index"]:
            did = self.state["attempt_index"][attempt]
            delivery = self.state["deliveries"][did]
            prior = next((a for a in delivery["attempts"] if a["id"] == attempt), None)
            blockers = self._blockers_for_delivery(did)
            if not blockers:
                for need in delivery.get("needs", []):
                    self._grant_seen(delivery["group"], need, via="resume_claim", at=at)
                if prior is not None:
                    prior["processable"] = True
                    prior["blockers"] = []
            return {
                "idempotent": True,
                "delivery_id": did,
                "attempt": attempt,
                "attempt_record": prior,
                "can_process": not blockers,
                "blockers": blockers,
            }

        did = self._find_delivery_id(group=group, command=command, rev=rev)
        delivery = self.state["deliveries"][did]

        # 稳定修订已经查到真实 applied 影响时，绝不允许再执行一次扣件副作用。
        fact = self.state["facts"].get(did)
        pre_blockers: list[dict[str, Any]] = []
        if fact and fact.get("result_state") == "applied" and fact.get("status") == "applied":
            result = self.continue_original(did=did, at=at)
            if attempt:
                record = {
                    "id": attempt,
                    "at": self.state.get("last_at", 0) if at is None else at,
                    "state": "deduped_existing_effect",
                    "effect_id": fact.get("effect_id"),
                }
                delivery["attempts"].append(record)
                self.state["attempt_index"][attempt] = did
            return {"idempotent": True, "decision": "continue_original", **result}
        if fact and fact.get("result_state") == "applied":
            # 仍保留这次领取记录，方便回放“消息已被领走但因果链未通”的现场；
            # blockers 会明确禁止真正执行副作用。
            pre_blockers = self._blockers_for_delivery(did)

        blockers = pre_blockers or self._blockers_for_delivery(did)
        attempt_id = attempt or f"att-{self._next_id('attempt')}"
        if attempt_id in self.state["attempt_index"]:
            raise RepairBusError(f"attempt 已存在：{attempt_id}")
        now = self.state.get("last_at", 0) if at is None else at
        record = {
            "id": attempt_id,
            "at": now,
            "state": "claimed",
            "processable": not blockers,
            "blockers": blockers,
            "extra": extra,
        }
        delivery["attempts"].append(record)
        delivery["current_attempt"] = attempt_id
        delivery["phase"] = "claimed"
        self.state["attempt_index"][attempt_id] = did

        if not blockers:
            # 只有中间件核对通过并定向放行时，才把完成凭据记入该消费组自己的视图。
            for need in delivery.get("needs", []):
                self._grant_seen(group, need, via="claim", at=at)

        self.audit(
            "claim",
            {"delivery_id": did, "attempt": attempt_id, "processable": not blockers, "blocker_count": len(blockers)},
            at,
        )
        return {
            "delivery_id": did,
            "attempt": attempt_id,
            "phase": "claimed",
            "can_process": not blockers,
            "blockers": blockers,
        }

    def mark_dead(
        self,
        group: str | None = None,
        command: str | None = None,
        attempt: str | None = None,
        dead_reason: str | None = None,
        reason: str | None = None,
        at: int | None = None,
        **extra: Any,
    ) -> dict[str, Any]:
        dead_reason = dead_reason or reason
        if dead_reason not in DEAD_REASONS:
            raise RepairBusError(f"dead_reason 必须是 {DEAD_REASONS} 之一")
        if not attempt:
            raise RepairBusError("dead 事件必须带 attempt")
        if attempt in self.state["attempt_index"]:
            did = self.state["attempt_index"][attempt]
        else:
            # 即使输入中缺少前置 claim，也保留 dead 现场；按 group/command 定位当前修订。
            did = self._find_delivery_id(group=group, command=command, rev=extra.get("rev"))
        delivery = self.state["deliveries"][did]
        attempt_record = next((a for a in delivery["attempts"] if a["id"] == attempt), None)
        if attempt_record is None:
            # 允许外部系统先有 attempt 事实、但领取事件缺失的演练输入。
            attempt_record = {"id": attempt, "at": at or 0, "state": "unknown"}
            delivery["attempts"].append(attempt_record)
            self.state["attempt_index"][attempt] = did

        now = self.state.get("last_at", 0) if at is None else at
        attempt_record["state"] = "dead"
        attempt_record["dead_reason"] = dead_reason
        attempt_record["dead_at"] = now
        delivery["phase"] = "dead"
        delivery["dead"] = {"attempt": attempt, "reason": dead_reason, "at": now}

        if dead_reason == "unknown":
            receipt = self.state["receipts"].setdefault(
                attempt,
                {
                    "id": attempt,
                    "delivery_id": did,
                    "ref": did,
                    "status": "pending",
                    "resolved": False,
                    "query_count": 0,
                    "created_at": now,
                },
            )
            receipt["status"] = "pending"
            receipt["resolved"] = False
            receipt["last_dead_at"] = now

        self.audit("dead", {"delivery_id": did, "attempt": attempt, "reason": dead_reason}, at)
        return {"delivery_id": did, "dead_reason": dead_reason}

    # Backwards-friendly alias.
    dead = mark_dead

    def report_effect(
        self,
        group: str,
        command: str,
        rev: int,
        state: str,
        attempt: str | None = None,
        effect_id: str | None = None,
        at: int | None = None,
        **extra: Any,
    ) -> dict[str, Any]:
        if state not in EFFECT_STATES:
            raise RepairBusError(f"effect state 必须是 {EFFECT_STATES} 之一")
        if state == "applied" and not effect_id:
            raise RepairBusError("applied 回执必须带 effect_id 证据")
        ref = delivery_id(group, command, rev)
        did = ref
        if attempt and attempt in self.state["attempt_index"]:
            mapped = self.state["attempt_index"][attempt]
            if mapped != ref:
                # attempt 可能属于同一 group/command 的其他修订；以 attempt 映射为准，
                # 但保留回执中的业务修订用于事实键。
                did = mapped

        now = self.state.get("last_at", 0) if at is None else at

        # 仿真业务端保留的外部处理事实。真实适配器中这里会是幂等查询结果。
        if attempt:
            self.state["external_ledger"][f"attempt:{attempt}"] = {
                "attempt": attempt,
                "ref": ref,
                "state": state,
                "effect_id": effect_id,
                "observed_at": now,
            }
        if effect_id:
            self.state["external_ledger"][f"effect:{effect_id}"] = {
                "effect_id": effect_id,
                "ref": ref,
                "attempt": attempt,
                "state": state,
                "observed_at": now,
            }

        fact = self.state["facts"].get(ref) or {
            "ref": ref,
            "group": group,
            "command": command,
            "rev": rev,
            "first_seen_at": now,
        }
        fact.update(
            {
                "result_state": state,
                "effect_id": effect_id or fact.get("effect_id"),
                "attempt": attempt or fact.get("attempt"),
                "last_reported_at": now,
                "extra": extra,
            }
        )
        self.state["facts"][ref] = fact
        self._group_view(group)["seen"].setdefault(
            ref, {"first_seen_at": now, "via": "effect_receipt", "effect_id": effect_id}
        )

        if effect_id:
            refs = self.state["effect_index"].setdefault(effect_id, [])
            if ref not in refs:
                refs.append(ref)

        if attempt and attempt in self.state["attempt_index"]:
            mapped_did = self.state["attempt_index"][attempt]
            delivery = self.state["deliveries"].get(mapped_did)
            if delivery:
                record = next((a for a in delivery["attempts"] if a["id"] == attempt), None)
                if record is not None:
                    record["external_state"] = state
                    record["effect_id"] = effect_id

        receipt = self.state["receipts"].get(attempt) if attempt else None
        if receipt is None and attempt:
            receipt = {
                "id": attempt,
                "delivery_id": did,
                "ref": ref,
                "created_at": now,
            }
            self.state["receipts"][attempt] = receipt
        if receipt is not None:
            receipt["last_state"] = state
            receipt["last_report_at"] = now
            receipt["resolved"] = state in {"applied", "not_applied"}
            receipt["status"] = "resolved" if receipt["resolved"] else "unknown"
            if effect_id:
                receipt["effect_id"] = effect_id

        derived = self._refresh_fact_status(ref)

        delivery = self.state["deliveries"].get(did)
        if delivery and attempt and self.state["attempt_index"].get(attempt) == did:
            if state == "applied":
                delivery["phase"] = "done"
                delivery["effect_id"] = effect_id
            elif state == "not_applied" and delivery.get("dead", {}).get("attempt") == attempt:
                # 旧 attempt 确认无影响后，若仍是当前修订则可安全重投。
                if self._delivery_blockers(did) == []:
                    delivery["phase"] = "ready"
                else:
                    delivery["phase"] = "waiting"

        if state == "applied" and derived == "compensation_required":
            covering = [
                rid
                for rid, repair in self.state["repairs"].items()
                if command in repair.get("commands", [])
            ]
            if covering:
                self.request_compensation(effect_id, repair_id=covering[0], at=now)
                for rid in covering[1:]:
                    action = self.state["compensation_actions"].get(effect_id)
                    if action and rid not in action["repair_ids"]:
                        action["repair_ids"].append(rid)

        self.audit(
            "effect",
            {"ref": ref, "state": state, "effect_id": effect_id, "derived_status": derived},
            at,
        )
        self.refresh_phases()
        return {"ref": ref, "fact_status": derived, "effect_id": effect_id}

    def continue_original(
        self,
        did: str | None = None,
        group: str | None = None,
        command: str | None = None,
        rev: int | None = None,
        at: int | None = None,
    ) -> dict[str, Any]:
        """用旧 applied 凭据继续，不重新执行副作用。"""
        did = did or self._find_delivery_id(group=group, command=command, rev=rev)
        fact = self.state["facts"].get(did)
        if not fact or fact.get("result_state") != "applied":
            raise BlockedOperation(f"{did} 没有可继续使用的已确认影响")
        if fact.get("status") != "applied":
            raise BlockedOperation(f"{did} 凭据状态为 {fact.get('status')}，不能按原结果继续")

        delivery = self.state["deliveries"].get(did)
        if delivery:
            delivery["phase"] = "done"
            delivery["effect_id"] = fact.get("effect_id")
        self._grant_seen(fact["group"], did, via="continue_original", at=at)
        self.audit("continue_original", {"delivery_id": did, "effect_id": fact.get("effect_id")}, at)
        self.refresh_phases()
        return {"delivery_id": did, "effect_id": fact.get("effect_id"), "decision": "continue_original"}

    # ---------- 补偿 ----------

    def request_compensation(
        self,
        effect_id: str,
        repair_id: str | None = None,
        at: int | None = None,
    ) -> dict[str, Any]:
        action = self.state["compensation_actions"].get(effect_id)
        now = self.state.get("last_at", 0) if at is None else at
        if action:
            if repair_id and repair_id not in action.setdefault("repair_ids", []):
                action["repair_ids"].append(repair_id)
            self.audit("compensation_request_idempotent", {"effect_id": effect_id, "repair_id": repair_id}, at)
            return {"created": False, "action": action}

        refs = self.state["effect_index"].get(effect_id, [])
        command = self.state["facts"].get(refs[0], {}).get("command") if refs else None
        action = {
            "effect_id": effect_id,
            "command": command,
            "status": "requested",
            "compensation_id": None,
            "repair_ids": [repair_id] if repair_id else [],
            "requested_at": now,
            "confirmed_at": None,
        }
        self.state["compensation_actions"][effect_id] = action
        self.audit("compensation_requested", {"effect_id": effect_id, "repair_id": repair_id}, at)
        return {"created": True, "action": action}

    def report_compensated(
        self,
        effect_id: str,
        compensation_id: str,
        group: str | None = None,
        command: str | None = None,
        at: int | None = None,
        **extra: Any,
    ) -> dict[str, Any]:
        if not effect_id or not compensation_id:
            raise RepairBusError("compensated 需要 effect_id 和 compensation_id")
        now = self.state.get("last_at", 0) if at is None else at
        action = self.state["compensation_actions"].get(effect_id)
        if action and action.get("status") == "confirmed":
            if action.get("compensation_id") != compensation_id:
                raise RepairBusError(
                    f"effect {effect_id} 已补偿为 {action.get('compensation_id')}，"
                    f"不能再确认 {compensation_id}"
                )
            self.audit("compensated_idempotent", {"effect_id": effect_id}, at)
            return {"idempotent": True, "compensation_id": compensation_id}

        if action is None:
            action = {
                "effect_id": effect_id,
                "command": command,
                "repair_ids": [],
                "requested_at": None,
            }
            self.state["compensation_actions"][effect_id] = action

        # 重叠修复申请共享同一补偿动作；把覆盖该命令的申请都关联上。
        if command:
            for rid, repair in self.state["repairs"].items():
                if command in repair.get("commands", []) and rid not in action["repair_ids"]:
                    action["repair_ids"].append(rid)

        action.update(
            {
                "status": "confirmed",
                "compensation_id": compensation_id,
                "confirmed_at": now,
                "report_group": group,
            }
        )

        for ref in self.state["effect_index"].get(effect_id, []):
            self._refresh_fact_status(ref)
        # 即使回执先于 effect 到达，也记录索引；后续 effect 会看到 confirmed 补偿。
        self.audit("compensated", {"effect_id": effect_id, "compensation_id": compensation_id}, at)
        self.refresh_phases()
        return {"effect_id": effect_id, "compensation_id": compensation_id}

    # ---------- 修复申请 ----------

    def create_repair(self, id: str, commands: list[str], at: int | None = None, **extra: Any) -> dict[str, Any]:
        if not id:
            raise RepairBusError("repair 需要 id")
        if not isinstance(commands, list) or not commands:
            raise RepairBusError("repair commands 必须是非空数组")
        commands = [str(c) for c in commands]
        if id in self.state["repairs"]:
            prior = self.state["repairs"][id]
            if list(prior.get("commands", [])) != commands:
                raise RepairBusError(f"修复申请 {id} 已存在且 commands 不一致")
            return {"idempotent": True, "repair": prior}

        components = self._components_for_commands(commands)
        chains: list[dict[str, Any]] = []
        all_nodes: set[str] = set()
        for component in components:
            chain_id = self._chain_id_for_component(component)
            owner = self.state["chain_locks"].get(chain_id)
            if owner is None:
                self.state["chain_locks"][chain_id] = id
                owner = id
            chains.append(
                {
                    "chain_id": chain_id,
                    "nodes": sorted(component),
                    "owner": owner,
                    "overlapping": owner != id,
                }
            )
            all_nodes.update(component)

        repair = {
            "id": id,
            "commands": commands,
            "chains": chains,
            "chain_ids": [chain["chain_id"] for chain in chains],
            # 兼容单一因果链的读取方；若申请显式包含多个不相连组件，报告以 chains 为准。
            "chain_id": chains[0]["chain_id"] if len(chains) == 1 else f"repair:{id}",
            "chain_nodes": sorted(all_nodes),
            "chain_owner": chains[0]["owner"] if len(chains) == 1 else None,
            "overlapping": any(chain["overlapping"] for chain in chains),
            "created_at": self.state.get("last_at", 0) if at is None else at,
            "extra": extra,
        }
        self.state["repairs"][id] = repair
        self.state["repair_order"].append(id)

        # 已存在的待补偿影响立即生成幂等补偿动作。
        for ref, fact in self.state["facts"].items():
            if fact.get("command") in commands and fact.get("status") == "compensation_required":
                result = self.request_compensation(fact["effect_id"], repair_id=id, at=at)
                if not result["created"] and id not in result["action"]["repair_ids"]:
                    result["action"]["repair_ids"].append(id)

        self.audit("repair", {"id": id, "commands": commands, "chain_ids": repair["chain_ids"]}, at)
        return {"repair": repair}

    def _components_for_commands(self, commands: list[str]) -> list[set[str]]:
        selected = {
            did
            for did, delivery in self.state["deliveries"].items()
            if delivery.get("command") in commands
        }
        if not selected:
            return []

        # 无向依赖图用于找因果连通分量。不能把本次申请选中的多个无根节点直接拼成
        # 一个分量，否则样例中的 c 会被错误地和 a/b 环锁在一起。
        adjacency: dict[str, set[str]] = {did: set() for did in self.state["deliveries"]}
        for did, delivery in self.state["deliveries"].items():
            for need in delivery.get("needs", []):
                adjacency.setdefault(did, set()).add(need)
                adjacency.setdefault(need, set()).add(did)

        components: list[set[str]] = []
        globally_seen: set[str] = set()
        for root in sorted(selected):
            if root in globally_seen:
                continue
            component: set[str] = set()
            stack = [root]
            while stack:
                node = stack.pop()
                if node in component:
                    continue
                component.add(node)
                for nxt in adjacency.get(node, ()):  # type: ignore[arg-type]
                    if nxt not in component:
                        stack.append(nxt)
            globally_seen.update(component)
            components.append(component)
        return components

    def _chain_id_for_component(self, component: set[str]) -> str:
        raw = "chain:" + ",".join(sorted(component))
        if len(raw) <= 180:
            return raw
        import hashlib

        return "chain:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]

    def _chain_for_commands(self, commands: list[str]) -> tuple[str, set[str]]:
        components = self._components_for_commands(commands)
        if not components:
            return "commands:" + ",".join(sorted(commands)), set()
        component = components[0]
        return self._chain_id_for_component(component), component

    # ---------- 来源决策：换凭据或撤销 ----------

    def retarget_dependency(
        self,
        new_ref: str,
        old_ref: str | None = None,
        did: str | None = None,
        group: str | None = None,
        command: str | None = None,
        rev: int | None = None,
        at: int | None = None,
    ) -> dict[str, Any]:
        did = did or self._find_delivery_id(group=group, command=command, rev=rev)
        delivery = self.state["deliveries"][did]
        new_ref = str(parse_ref(new_ref))
        if old_ref is None:
            replaceable = []
            for need in delivery.get("needs", []):
                target = self.state["deliveries"].get(need)
                if self._need_is_obsolete(need) or (target and target.get("phase") == "withdrawn"):
                    replaceable.append(need)
            if len(replaceable) != 1:
                raise RepairBusError("存在多个可替换旧依赖，请用 old_ref 明确指定要替换的依赖")
            old_ref = replaceable[0]
        else:
            old_ref = str(parse_ref(old_ref))
        if old_ref not in delivery.get("needs", []):
            raise RepairBusError(f"{did} 不依赖 {old_ref}")
        if new_ref == old_ref:
            raise RepairBusError("新凭据与旧凭据相同，不能完成换版")

        needs = delivery["needs"]
        needs[needs.index(old_ref)] = new_ref
        delivery.setdefault("source_decisions", []).append(
            {"type": "retarget", "old_ref": old_ref, "new_ref": new_ref, "at": self.state.get("last_at", 0) if at is None else at}
        )
        self.audit("retarget", {"delivery_id": did, "old_ref": old_ref, "new_ref": new_ref}, at)
        self.refresh_phases()
        return self.delivery_view(did)

    def withdraw(
        self,
        did: str | None = None,
        group: str | None = None,
        command: str | None = None,
        rev: int | None = None,
        reason: str = "source_withdrew",
        at: int | None = None,
    ) -> dict[str, Any]:
        did = did or self._find_delivery_id(group=group, command=command, rev=rev)
        delivery = self.state["deliveries"][did]
        if delivery.get("phase") == "done":
            raise BlockedOperation(f"{did} 已兑现，不能撤销投递；需要走补偿/冲正")
        delivery["phase"] = "withdrawn"
        delivery.setdefault("source_decisions", []).append({"type": "withdraw", "reason": reason, "at": at or 0})
        self.audit("withdraw", {"delivery_id": did, "reason": reason}, at)
        return self.delivery_view(did)

    # ---------- 定向放行 ----------

    def release(
        self,
        group: str | None = None,
        command: str | None = None,
        repair_id: str | None = None,
        only: list[str] | None = None,
        at: int | None = None,
    ) -> dict[str, Any]:
        cycle_map = cycle_index(self.state["deliveries"])
        candidates = list(only or [])
        if not candidates:
            for did, delivery in self.state["deliveries"].items():
                if group and delivery.get("group") != group:
                    continue
                if command and delivery.get("command") != command:
                    continue
                if repair_id:
                    repair = self.state["repairs"].get(repair_id)
                    if not repair:
                        raise RepairBusError(f"未知修复申请：{repair_id}")
                    if did not in repair.get("chain_nodes", []) and delivery.get("command") not in repair.get("commands", []):
                        continue
                candidates.append(did)

        released: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        for did in sorted(set(candidates)):
            if did not in self.state["deliveries"]:
                skipped.append({"id": did, "reason": "unknown_delivery"})
                continue
            view = self.delivery_view(did, cycle_map=cycle_map)
            if not view["can_release"]:
                skipped.append({"id": did, "next_action": view["next_action"], "blockers": view["blockers"]})
                continue
            delivery = self.state["deliveries"][did]
            delivery["phase"] = "ready"
            for need in delivery.get("needs", []):
                self._grant_seen(delivery["group"], need, via="controlled_release", at=at)
            released.append(did)
            self.audit("release", {"delivery_id": did}, at)

        return {"released": released, "skipped": skipped}

    def _grant_seen(self, group: str, ref: str, via: str, at: int | None = None) -> None:
        view = self._group_view(group)
        view["seen"].setdefault(
            ref,
            {
                "first_seen_at": self.state.get("last_at", 0) if at is None else at,
                "via": via,
                "effect_id": self.state["facts"].get(ref, {}).get("effect_id"),
            },
        )

    # ---------- 状态推导 ----------

    def _refresh_fact_status(self, ref: str) -> str:
        fact = self.state["facts"][ref]
        result_state = fact.get("result_state")
        if result_state == "unknown":
            fact["status"] = "unknown"
        elif result_state == "not_applied":
            fact["status"] = "no_effect"
        elif result_state == "applied":
            effect_id = fact.get("effect_id")
            action = self.state["compensation_actions"].get(effect_id or "")
            if action and action.get("status") == "confirmed":
                fact["status"] = "compensated"
                fact["compensation_id"] = action.get("compensation_id")
            elif self._has_effect_amendment_after(fact):
                fact["status"] = "compensation_required"
            else:
                fact["status"] = "applied"
        else:
            fact["status"] = result_state or "missing"
        return fact["status"]

    def _has_effect_amendment_after(self, fact: dict[str, Any]) -> bool:
        entry = self.state["commands"].get(fact.get("command", ""))
        if not entry:
            return False
        fact_rev = int(fact.get("rev", 0))
        for rev_text, rev in entry.get("revs", {}).items():
            if int(rev_text) > fact_rev and rev.get("changed") == "effect":
                return True
        return False

    def _need_is_obsolete(self, ref: str) -> bool:
        try:
            parsed = parse_ref(ref)
        except ValueError:
            return False
        entry = self.state["commands"].get(parsed.command)
        return bool(entry and int(entry.get("current_rev", parsed.rev)) > parsed.rev)

    def _delivery_blockers(self, did: str, cycle_map: dict[str, list[str]] | None = None) -> list[dict[str, Any]]:
        return self._blockers_for_delivery(did, cycle_map)

    def _blockers_for_delivery(self, did: str, cycle_map: dict[str, list[str]] | None = None) -> list[dict[str, Any]]:
        delivery = self.state["deliveries"][did]
        blockers: list[dict[str, Any]] = []
        phase = delivery.get("phase")
        terminal = phase in {"done", "withdrawn"}
        cycle_map = cycle_map or cycle_index(self.state["deliveries"])

        if not terminal and did in cycle_map:
            blockers.append(
                {
                    "code": "dependency_cycle",
                    "message": "前置关系成环，需来源断开或改写其中一条依赖",
                    "cycle": cycle_map[did],
                }
            )

        entry = self.state["commands"].get(delivery["command"])
        current_rev = int(entry["current_rev"]) if entry else delivery["rev"]
        if not terminal and current_rev > delivery["rev"]:
            blockers.append(
                {
                    "code": "superseded_revision",
                    "message": f"{did} 已被 {delivery['group']}/{delivery['command']}@{current_rev} 取代",
                    "new_ref": f"{delivery['group']}/{delivery['command']}@{current_rev}",
                }
            )

        # 更正后的修订必须先消解前序 unknown / effect 补偿，不能用旧确认冒名新版。
        rev_record = self._rev_record(delivery["command"], delivery["rev"])
        if rev_record and rev_record.get("source") == "amend" and not terminal:
            pred = rev_record.get("correction_of")
            changed = rev_record.get("changed")
            if pred is not None:
                for group in rev_record.get("consumers", []):
                    old_ref = delivery_id(group, delivery["command"], int(pred))
                    old_fact = self.state["facts"].get(old_ref)
                    if old_fact and old_fact.get("result_state") == "unknown":
                        blockers.append({"code": "prior_result_unknown", "ref": old_ref})
                    if changed == "effect" and old_fact and old_fact.get("result_state") == "applied":
                        if old_fact.get("status") != "compensated":
                            blockers.append(
                                {
                                    "code": "await_compensation",
                                    "ref": old_ref,
                                    "effect_id": old_fact.get("effect_id"),
                                    "message": "旧影响已被 effect 类更正命中，必须先取得补偿结果",
                                }
                            )

        # 自身已经确认的影响若被 effect 更正命中，即使 phase=done 也要提示补偿。
        own_fact = self.state["facts"].get(did)
        if own_fact and own_fact.get("result_state") == "applied" and own_fact.get("status") == "compensation_required":
            blockers.append(
                {
                    "code": "compensation_required",
                    "ref": did,
                    "effect_id": own_fact.get("effect_id"),
                    "message": "已确认的旧影响被 effect 更正命中，需先补偿",
                }
            )

        # 死信自身结果不明时，先查外部系统。
        dead = delivery.get("dead")
        if dead and dead.get("reason") == "unknown" and phase == "dead":
            if not own_fact or own_fact.get("result_state") == "unknown":
                blockers.append(
                    {
                        "code": "own_result_unknown",
                        "attempt": dead.get("attempt"),
                        "message": "不知道该尝试是否已产生外部影响",
                    }
                )
        if dead and dead.get("reason") == "incomplete" and phase == "dead":
            blockers.append({"code": "incomplete_information", "message": "资料不全，需更正/补全后按新修订投递"})

        for need in delivery.get("needs", []):
            parsed = parse_ref(need)
            target_delivery = self.state["deliveries"].get(need)
            if target_delivery and target_delivery.get("phase") == "withdrawn":
                blockers.append(
                    {
                        "code": "prerequisite_withdrawn",
                        "ref": need,
                        "message": "前置处理已被来源撤销，需换凭据或撤销当前处理",
                    }
                )
                continue
            fact = self.state["facts"].get(need)
            if not fact:
                blockers.append({"code": "missing_credential", "ref": need, "message": "尚未出现作出方确认的真实副作用凭据"})
                continue
            result_state = fact.get("result_state")
            if result_state == "unknown":
                blockers.append({"code": "prerequisite_unknown", "ref": need, "attempt": fact.get("attempt")})
                continue
            if result_state == "not_applied":
                blockers.append({"code": "prerequisite_no_effect", "ref": need, "message": "该前置确认无影响，不能充当完成凭据"})
                continue
            if result_state != "applied":
                blockers.append({"code": "prerequisite_not_applied", "ref": need, "state": result_state})
                continue

            status = fact.get("status")
            if status == "compensation_required":
                blockers.append(
                    {
                        "code": "compensation_required",
                        "ref": need,
                        "effect_id": fact.get("effect_id"),
                        "message": "前置旧影响被 effect 更正命中，需先补偿",
                    }
                )
            elif status == "compensated":
                blockers.append(
                    {
                        "code": "compensated_credential",
                        "ref": need,
                        "effect_id": fact.get("effect_id"),
                        "compensation_id": fact.get("compensation_id"),
                        "message": "旧凭据已被补偿，不能继续授予新版因果链",
                    }
                )
            elif status != "applied":
                blockers.append({"code": "credential_not_active", "ref": need, "status": status})

            # 旧修订即使仍有 applied 确认，也不能在更正后自动当作新依赖放行；
            # 已补偿/待补偿时同样保留“过时依赖”位置，供来源换凭据或撤销。
            if not terminal and self._need_is_obsolete(need):
                new_ref = f"{parsed.group}/{parsed.command}@{self.state['commands'][parsed.command]['current_rev']}"
                blockers.append(
                    {
                        "code": "obsolete_dependency",
                        "ref": need,
                        "new_ref": new_ref,
                        "message": "后续仍依赖旧修订；需来源改用新凭据或撤掉本次处理",
                    }
                )

        return blockers

    def refresh_phases(self) -> None:
        """根据阻塞项刷新 waiting/ready；不覆盖 claimed/dead/done/withdrawn。"""
        cycle_map = cycle_index(self.state["deliveries"])
        for did, delivery in self.state["deliveries"].items():
            if delivery.get("phase") in {"claimed", "dead", "done", "withdrawn"}:
                continue
            blockers = self._blockers_for_delivery(did, cycle_map)
            delivery["phase"] = "ready" if not blockers else "waiting"

    def delivery_view(self, did: str, cycle_map: dict[str, list[str]] | None = None) -> dict[str, Any]:
        if did not in self.state["deliveries"]:
            raise RepairBusError(f"未知投递：{did}")
        delivery = self.state["deliveries"][did]
        cycle_map = cycle_map or cycle_index(self.state["deliveries"])
        blockers = self._blockers_for_delivery(did, cycle_map)
        phase = delivery.get("phase")
        fact = self.state["facts"].get(did, {})

        hard_block_codes = {
            "dependency_cycle",
            "superseded_revision",
            "prior_result_unknown",
            "await_compensation",
            "own_result_unknown",
            "incomplete_information",
            "missing_credential",
            "prerequisite_withdrawn",
            "prerequisite_unknown",
            "prerequisite_no_effect",
            "prerequisite_not_applied",
            "compensation_required",
            "compensated_credential",
            "credential_not_active",
            "obsolete_dependency",
        }
        blocked = any(b.get("code") in hard_block_codes for b in blockers)
        can_release = phase in {"waiting", "ready", "dead"} and not blocked
        can_claim = phase in {"ready", "dead"} and not blocked
        can_process = phase == "claimed" and not blocked
        can_continue_original = bool(fact.get("result_state") == "applied" and fact.get("status") == "applied")

        if phase == "withdrawn":
            next_action = "none_withdrawn"
        elif fact.get("result_state") == "applied" and fact.get("status") == "compensation_required":
            next_action = "compensate"
        elif any(b["code"] in {"own_result_unknown", "prerequisite_unknown", "prior_result_unknown"} for b in blockers):
            next_action = "query_external_result"
        elif any(b["code"] in {"compensation_required", "await_compensation"} for b in blockers):
            next_action = "compensate"
        elif any(b["code"] in {"obsolete_dependency", "prerequisite_withdrawn"} for b in blockers):
            next_action = "source_retarget_or_withdraw"
        elif any(b["code"] == "dependency_cycle" for b in blockers):
            next_action = "break_cycle_by_source_decision"
        elif any(b["code"] == "incomplete_information" for b in blockers):
            next_action = "await_amended_revision"
        elif any(b["code"] == "superseded_revision" for b in blockers):
            next_action = "use_amended_revision"
        elif can_process:
            next_action = "process_claimed_attempt"
        elif can_continue_original:
            next_action = "continue_original"
        elif not blocked and phase in {"ready", "waiting"}:
            next_action = "release_and_claim"
        elif not blocked and phase == "dead":
            next_action = "redeliver"
        elif phase == "done":
            next_action = "none_fulfilled"
        else:
            next_action = "wait"

        group_view = self.state["group_views"].get(delivery["group"], {}).get("seen", {})
        return {
            "id": did,
            "key": delivery.get("key"),
            "group": delivery.get("group"),
            "command": delivery.get("command"),
            "rev": delivery.get("rev"),
            "phase": phase,
            "dead": delivery.get("dead"),
            "dead_reason": (delivery.get("dead") or {}).get("reason"),
            "current_attempt": delivery.get("current_attempt"),
            "effect_id": delivery.get("effect_id") or fact.get("effect_id"),
            "fact_status": fact.get("status"),
            "needs": delivery.get("needs", []),
            "seen_facts": sorted(ref for ref in delivery.get("needs", []) if ref in group_view),
            "missing_seen_facts": sorted(ref for ref in delivery.get("needs", []) if ref not in group_view),
            "blockers": blockers,
            "can_release": can_release,
            "can_claim": can_claim,
            "can_process": can_process,
            "can_continue_original": can_continue_original,
            "next_action": next_action,
            "attempts": delivery.get("attempts", []),
            "source_decisions": delivery.get("source_decisions", []),
        }

    # ---------- 查询报告 ----------

    def pending_receipts(self) -> list[dict[str, Any]]:
        return [receipt for receipt in self.state["receipts"].values() if not receipt.get("resolved")]

    def resume_receipt_queries(self, adapter: Any | None = None) -> dict[str, Any]:
        """重启后继续未消解回执。

        本地仿真适配器可从 external_ledger 查旧 attempt；真实适配器可替换为 HTTP/RPC。
        """
        resumed: list[str] = []
        still_pending: list[str] = []
        for receipt in self.pending_receipts():
            attempt = receipt["id"]
            record = self.state["external_ledger"].get(f"attempt:{attempt}")
            receipt["query_count"] = int(receipt.get("query_count", 0)) + 1
            if not record or record.get("state") == "unknown":
                still_pending.append(attempt)
                continue
            ref = receipt.get("ref", receipt.get("delivery_id"))
            parsed = parse_ref(ref)
            self.report_effect(
                group=parsed.group,
                command=parsed.command,
                rev=parsed.rev,
                state=record["state"],
                attempt=attempt,
                effect_id=record.get("effect_id"),
            )
            resumed.append(attempt)
        self.audit("resume_receipts", {"resumed": resumed, "pending": still_pending})
        return {"resumed": resumed, "pending": still_pending}

    def record_restart(self, at: int | None = None) -> dict[str, Any]:
        pending = self.pending_receipts()
        self.audit("restart", {"pending_receipts": [r["id"] for r in pending]}, at)
        resumed = self.resume_receipt_queries()
        return {"pending_before_resume": [r["id"] for r in pending], **resumed}

    def command_report(self, command: str, rev: int | None = None, group: str | None = None) -> dict[str, Any]:
        cycle_map = cycle_index(self.state["deliveries"])
        views = [
            self.delivery_view(did, cycle_map)
            for did, d in self.state["deliveries"].items()
            if d.get("command") == command and (rev is None or d.get("rev") == rev) and (group is None or d.get("group") == group)
        ]
        views.sort(key=lambda v: (v["rev"], v["group"]))
        if not views:
            raise RepairBusError(f"找不到命令投递：{group + '/' if group else ''}{command}" + (f"@{rev}" if rev else ""))

        refs = {v["id"] for v in views}
        dependent_views = []
        for did, d in self.state["deliveries"].items():
            if any(need in refs or parse_ref(need).command == command for need in d.get("needs", [])):
                if did not in refs:
                    dependent_views.append(self.delivery_view(did, cycle_map))
        dependent_views.sort(key=lambda v: v["id"])
        return {
            "report": "command",
            "command": command,
            "rev": rev,
            "group": group,
            "current_rev": self.state["commands"].get(command, {}).get("current_rev"),
            "revisions": self.state["commands"].get(command, {}).get("revs", {}),
            "consumer_views": views,
            "dependent_views": dependent_views,
            "dead_letters": [v for v in views if v.get("dead")],
            "releasable": [v["id"] for v in views + dependent_views if v["can_release"]],
        }

    def amendment_report(self, command: str) -> dict[str, Any]:
        entry = self.state["commands"].get(command)
        if not entry:
            raise RepairBusError(f"未知命令：{command}")
        cycle_map = cycle_index(self.state["deliveries"])
        amendments: list[dict[str, Any]] = []
        for rev_text, rev_record in sorted(entry.get("revs", {}).items(), key=lambda item: int(item[0])):
            if rev_record.get("source") != "amend":
                continue
            impacts: list[dict[str, Any]] = []
            pred = rev_record.get("correction_of")
            for group in rev_record.get("consumers", []):
                old_ref = delivery_id(group, command, int(pred)) if pred is not None else None
                fact = self.state["facts"].get(old_ref) if old_ref else None
                if fact:
                    impacts.append(
                        {
                            "ref": old_ref,
                            "result_state": fact.get("result_state"),
                            "status": fact.get("status"),
                            "effect_id": fact.get("effect_id"),
                            "compensation": self.state["compensation_actions"].get(fact.get("effect_id") or ""),
                        }
                    )
            delivery_views = [
                self.delivery_view(delivery_id(group, command, int(rev_text)), cycle_map)
                for group in rev_record.get("consumers", [])
                if delivery_id(group, command, int(rev_text)) in self.state["deliveries"]
            ]
            if any(i.get("status") == "compensation_required" for i in impacts):
                status = "await_compensation"
            elif any(i.get("result_state") == "unknown" for i in impacts):
                status = "unknown_prior_result"
            elif any(v["can_release"] or v["can_claim"] for v in delivery_views):
                status = "ready_for_delivery"
            elif all(v["phase"] == "done" for v in delivery_views) and delivery_views:
                status = "delivered"
            else:
                status = "blocked"
            amendments.append(
                {
                    "command": command,
                    "rev": int(rev_text),
                    "changed": rev_record.get("changed"),
                    "correction_of": pred,
                    "status": status,
                    "impacts": impacts,
                    "delivery_ids": [v["id"] for v in delivery_views],
                    "next_actions": sorted({v["next_action"] for v in delivery_views}),
                }
            )
        return {
            "report": "amendments",
            "command": command,
            "current_rev": entry.get("current_rev"),
            "amendments": amendments,
        }

    def dead_report(self, command: str | None = None, attempt: str | None = None, rev: int | None = None) -> dict[str, Any]:
        cycle_map = cycle_index(self.state["deliveries"])
        dead_views = []
        mapped_attempt_did = self.state["attempt_index"].get(attempt) if attempt else None
        for did, d in self.state["deliveries"].items():
            if command and d.get("command") != command:
                continue
            if rev is not None and d.get("rev") != rev:
                continue
            if attempt:
                dead_attempt = (d.get("dead") or {}).get("attempt")
                if dead_attempt != attempt and mapped_attempt_did != did:
                    continue
            if d.get("dead"):
                dead_views.append(self.delivery_view(did, cycle_map))
        if mapped_attempt_did:
            did = mapped_attempt_did
            if not any(v["id"] == did for v in dead_views):
                dead_views.append(self.delivery_view(did, cycle_map))
        if not dead_views:
            target = attempt or (f"{command}@{rev}" if rev else command)
            raise RepairBusError(f"找不到死信：{target}")

        letters = []
        for view in dead_views:
            did = view["id"]
            peers = [
                self.delivery_view(other, cycle_map)
                for other, d in self.state["deliveries"].items()
                if d.get("command") == view["command"] and other != did
            ]
            downstream = [
                self.delivery_view(other, cycle_map)
                for other, d in self.state["deliveries"].items()
                if did in d.get("needs", []) or any(n == did for n in d.get("needs", []))
            ]
            # Need refs include group/cmd@rev, and did has same form.
            downstream = [
                self.delivery_view(other, cycle_map)
                for other, d in self.state["deliveries"].items()
                if other != did and did in d.get("needs", [])
            ]
            eyes = [{"perspective": "dead_letter_owner", "group": view["group"], "view": view}]
            for p in peers:
                perspective = "amended_revision" if p["rev"] != view["rev"] else "peer_consumer"
                eyes.append({"perspective": perspective, "group": p["group"], "view": p})
            eyes += [{"perspective": "downstream_consumer", "group": x["group"], "view": x} for x in downstream]
            related_views = [view] + peers + downstream
            decisions = {x["next_action"] for x in related_views}
            if "compensate" in decisions:
                decision = "compensate"
            elif "query_external_result" in decisions:
                decision = "query_external_result"
            elif "source_retarget_or_withdraw" in decisions:
                decision = "source_retarget_or_withdraw"
            elif "continue_original" in decisions or view.get("can_continue_original"):
                decision = "continue_original"
            elif any(x["can_release"] or x.get("can_process") for x in related_views):
                decision = "release_ready_successor"
            else:
                decision = view["next_action"]
            letters.append(
                {
                    "dead_letter": did,
                    "decision": decision,
                    "consumer_eyes": eyes,
                    "releasable": [x["id"] for x in related_views if x["can_release"]],
                    "processable": [x["id"] for x in related_views if x.get("can_process")],
                }
            )
        return {"report": "dead_letter", "dead_letters": letters}

    def group_report(self, group: str) -> dict[str, Any]:
        cycle_map = cycle_index(self.state["deliveries"])
        views = [self.delivery_view(did, cycle_map) for did, d in self.state["deliveries"].items() if d.get("group") == group]
        views.sort(key=lambda v: v["id"])
        return {
            "report": "group",
            "group": group,
            "seen_facts": self.state["group_views"].get(group, {}).get("seen", {}),
            "deliveries": views,
            "pending_receipts": [r for r in self.pending_receipts() if r.get("delivery_id", "").startswith(group + "/")],
            "releasable": [v["id"] for v in views if v["can_release"]],
        }

    def repair_report(self, repair_id: str) -> dict[str, Any]:
        repair = self.state["repairs"].get(repair_id)
        if not repair:
            raise RepairBusError(f"未知修复申请：{repair_id}")
        cycle_map = cycle_index(self.state["deliveries"])

        selected_ids = {
            did
            for did, d in self.state["deliveries"].items()
            if d.get("command") in repair["commands"]
        }
        # 每次查询都按当前图重算因果分量，使修复期间新发布、且依赖该链的消息
        # 自动进入值班视野；创建时落盘的 chain_ids 继续用于重叠申请判重。
        current_components = self._components_for_commands(repair["commands"])
        current_scope: set[str] = set()
        for component in current_components:
            current_scope.update(component)
        # 报告覆盖申请命令及其当前无向因果闭包中的投递。
        scope_ids = {did for did in current_scope if did in self.state["deliveries"]}
        scope_ids |= selected_ids
        views = [self.delivery_view(did, cycle_map) for did in sorted(scope_ids)]

        cycles = []
        seen_cycles = set()
        for did in scope_ids:
            component = cycle_map.get(did)
            if component:
                key = tuple(component)
                if key not in seen_cycles:
                    seen_cycles.add(key)
                    cycles.append(component)

        unknown = [
            {"id": v["id"], "blockers": [b for b in v["blockers"] if "unknown" in b["code"]]}
            for v in views
            if any("unknown" in b["code"] for b in v["blockers"])
        ]
        continuable_views = [
            v
            for v in views
            if v["can_release"] or v["can_claim"] or v.get("can_process")
        ]
        continuable = [v["id"] for v in continuable_views]
        reusable_original_results = [
            {"id": v["id"], "group": v["group"], "effect_id": v["effect_id"]}
            for v in views
            if v["can_continue_original"]
        ]
        continuable_actions = [
            {
                "id": v["id"],
                "group": v["group"],
                "command": v["command"],
                "rev": v["rev"],
                "action": v["next_action"],
            }
            for v in continuable_views
        ]
        releasable = [v["id"] for v in views if v["can_release"]]
        releasable_actions = [
            {"id": v["id"], "group": v["group"], "action": "release_and_grant_seen"}
            for v in views
            if v["can_release"]
        ]
        fulfilled = [
            {
                "id": v["id"],
                "effect_id": v["effect_id"],
                "fact_status": v["fact_status"],
                "note": "已产生外部业务影响，不能重复执行",
            }
            for v in views
            if v["phase"] == "done" or v.get("effect_id")
        ]
        do_not_repeat = fulfilled

        compensation_ids = set()
        for v in views:
            for blocker in v["blockers"]:
                if blocker.get("effect_id"):
                    compensation_ids.add(blocker["effect_id"])
            if v.get("effect_id") and v.get("fact_status") in {"compensation_required", "compensated"}:
                compensation_ids.add(v["effect_id"])
        compensations = [self.state["compensation_actions"].get(eid, {"effect_id": eid, "status": "required"}) for eid in sorted(compensation_ids)]

        overlap = []
        my_chains = set(repair.get("chain_ids") or ([repair.get("chain_id")] if repair.get("chain_id") else []))
        for rid, other in self.state["repairs"].items():
            if rid == repair_id:
                continue
            other_chains = set(other.get("chain_ids") or ([other.get("chain_id")] if other.get("chain_id") else []))
            if my_chains & other_chains:
                overlap.append(rid)

        cycle_node_set = {node for cycle in cycles for node in cycle}
        cycle_branch_set: set[str] = set()
        for cycle in cycles:
            cycle_branch_set.update(self._waiting_descendants(cycle))
        cycle_branch_set -= cycle_node_set

        # 汇总当前最高优先级决策。
        if compensations and any(c.get("status") != "confirmed" for c in compensations):
            decision = "compensate"
        elif unknown:
            decision = "query_external_result"
        elif cycles:
            decision = "blocked_cycle_but_unaffected_can_continue"
        elif releasable:
            decision = "release_ready_successors"
        else:
            decision = "wait_or_source_decision"

        unaffected = [
            v["id"]
            for v in views
            if v["id"] not in cycle_node_set
            and v["id"] not in cycle_branch_set
            and not v.get("blockers")
            and v.get("phase") != "withdrawn"
            and not v.get("dead")
            and v.get("fact_status") not in {"compensated", "compensation_required"}
        ]
        return {
            "report": "repair",
            "id": repair_id,
            "decision": decision,
            "chain_id": repair.get("chain_id"),
            "chain_ids": repair.get("chain_ids", []),
            "chains": repair.get("chains", []),
            "chain_owner": repair.get("chain_owner"),
            "overlapping_repairs": overlap,
            "cycles": cycles,
            "cycle_nodes": sorted(cycle_node_set),
            "cycle_blocked_branches": sorted(did for did in cycle_branch_set if did in scope_ids),
            "unknown": unknown,
            "continuable": continuable,
            "continuable_actions": continuable_actions,
            "reusable_original_results": reusable_original_results,
            "releasable": releasable,
            "releasable_actions": releasable_actions,
            "fulfilled": fulfilled,
            "do_not_repeat": do_not_repeat,
            "compensations": compensations,
            "unaffected": unaffected,
            "deliveries": views,
        }

    def _waiting_descendants(self, roots: list[str]) -> set[str]:
        from .graph import descendants

        return descendants(roots, self.state["deliveries"])

    def overview(self) -> dict[str, Any]:
        cycle_map = cycle_index(self.state["deliveries"])
        views = [self.delivery_view(did, cycle_map) for did in sorted(self.state["deliveries"])]
        return {
            "report": "overview",
            "dead_letters": [v["id"] for v in views if v.get("dead")],
            "pending_receipts": self.pending_receipts(),
            "releasable": [v["id"] for v in views if v["can_release"]],
            "repairs": list(self.state["repair_order"]),
            "compensation_actions": list(self.state["compensation_actions"].values()),
            "deliveries": views,
        }

    def inspect(self, repair: str | None = None, command: str | None = None, group: str | None = None, **kwargs: Any) -> dict[str, Any]:
        if repair:
            return self.repair_report(repair)
        if kwargs.get("dead"):
            return self.dead_report(command=command, attempt=kwargs.get("attempt"), rev=kwargs.get("rev"))
        if command:
            return self.command_report(command, rev=kwargs.get("rev"), group=group)
        if group:
            return self.group_report(group)
        return self.overview()
