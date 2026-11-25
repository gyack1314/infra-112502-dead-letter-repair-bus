"""本地消费端/外部业务适配器仿真。

不连接真实消息系统或账务服务：副作用保存在中间件状态的 external_ledger/facts 中，
但保留幂等键、attempt、effect_id 和重启后查询这些关键交互。
"""

from __future__ import annotations

from typing import Any

from .engine import RepairBus
from .errors import BlockedOperation


class LocalConsumerAdapter:
    def __init__(self, bus: RepairBus):
        self.bus = bus

    def idempotency_key(self, delivery: dict[str, Any]) -> str:
        return f"biz:{delivery.get('key')}:{delivery['command']}:{delivery['rev']}"

    def consume(
        self,
        group: str,
        command: str,
        rev: int | None = None,
        attempt: str | None = None,
        at: int | None = None,
        fail: str | None = None,
        effect_after_dead: bool = False,
    ) -> dict[str, Any]:
        claimed = self.bus.claim(group=group, command=command, rev=rev, attempt=attempt, at=at)
        did = claimed["delivery_id"]
        attempt_id = claimed["attempt"]
        delivery = self.bus.state["deliveries"][did]

        if claimed.get("decision") == "continue_original":
            return {"decision": "continue_original", **claimed}
        if claimed.get("can_process") is False and not fail:
            raise BlockedOperation(
                f"{did} 已被本地消费端领取，但依赖/更正状态未满足，未执行业务副作用",
                claimed.get("blockers", []),
            )

        idem_key = self.idempotency_key(delivery)
        existing = self.bus.state["external_ledger"].get(idem_key)
        if existing and existing.get("state") == "applied":
            # 稳定业务命令的同一修订重试：外部已经处理，只取回旧凭据。
            return self.bus.report_effect(
                group=delivery["group"],
                command=delivery["command"],
                rev=delivery["rev"],
                state="applied",
                attempt=attempt_id,
                effect_id=existing["effect_id"],
                at=at,
            )

        if fail:
            dead = self.bus.mark_dead(attempt=attempt_id, dead_reason=fail, at=at)
            if fail == "unknown" and effect_after_dead:
                seq = self.bus._next_id("effect")
                effect_id = f"eff-{delivery['command']}-{delivery['rev']}-{seq}"
                self.bus.state["external_ledger"][idem_key] = {
                    "state": "applied",
                    "effect_id": effect_id,
                    "attempt": attempt_id,
                }
                self.bus.report_effect(
                    group=delivery["group"],
                    command=delivery["command"],
                    rev=delivery["rev"],
                    state="applied",
                    attempt=attempt_id,
                    effect_id=effect_id,
                    at=at,
                )
            return dead

        seq = self.bus._next_id("effect")
        effect_id = f"eff-{delivery['command']}-{delivery['rev']}-{seq}"
        self.bus.state["external_ledger"][idem_key] = {
            "state": "applied",
            "effect_id": effect_id,
            "attempt": attempt_id,
        }
        return self.bus.report_effect(
            group=delivery["group"],
            command=delivery["command"],
            rev=delivery["rev"],
            state="applied",
            attempt=attempt_id,
            effect_id=effect_id,
            at=at,
        )

    def query_attempt(self, attempt: str) -> dict[str, Any] | None:
        return self.bus.state["external_ledger"].get(f"attempt:{attempt}")
