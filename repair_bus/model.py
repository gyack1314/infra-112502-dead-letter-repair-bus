"""死信修复通道的领域常量与标识工具。"""

from __future__ import annotations

import re
from typing import NamedTuple

DEAD_REASONS = ("unavailable", "incomplete", "unknown")
EFFECT_STATES = ("applied", "not_applied", "unknown")
AMEND_CHANGED = ("effect", "context")

# waiting/ready/claimed/dead/done/withdrawn 是持久化的主阶段。
# 是否真正可放行由 reports/engine 结合凭据、更正和环动态计算。
PHASES = ("waiting", "ready", "claimed", "dead", "done", "withdrawn")

REF_RE = re.compile(r"^(?P<group>[^/]+)/(?P<command>[^@]+)@(?P<rev>\d+)$")


class Ref(NamedTuple):
    group: str
    command: str
    rev: int

    def __str__(self) -> str:
        return f"{self.group}/{self.command}@{self.rev}"


def parse_ref(value: str) -> Ref:
    """解析 group/command@rev；无效引用按参数错误处理。"""
    if not isinstance(value, str):
        raise ValueError("凭据引用必须是字符串")
    match = REF_RE.fullmatch(value.strip())
    if not match:
        raise ValueError(f"凭据引用应为 group/command@rev：{value!r}")
    return Ref(match.group("group"), match.group("command"), int(match.group("rev")))


def delivery_id(group: str, command: str, rev: int) -> str:
    return f"{group}/{command}@{rev}"


def new_state() -> dict:
    """创建可 JSON 持久化的空状态。"""
    return {
        "version": 1,
        "last_at": 0,
        "commands": {},
        "deliveries": {},
        "attempt_index": {},
        "facts": {},
        "effect_index": {},
        "group_views": {},
        "receipts": {},
        "external_ledger": {},
        "repairs": {},
        "repair_order": [],
        "compensation_actions": {},
        "chain_locks": {},
        "audit": [],
    }
