"""JSONL 演练事件回放。"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Callable

from .engine import RepairBus
from .store import StateStore

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from scenario_reader import load_events  # noqa: E402


def _kinds_for(path: Path) -> set[str]:
    types_path = path.parent / "message_types.json"
    if types_path.exists():
        return set(json.loads(types_path.read_text(encoding="utf-8")))
    return {
        "publish",
        "claim",
        "dead",
        "effect",
        "amend",
        "compensated",
        "repair",
        "inspect",
        "restart",
    }


def replay_file(
    path: str | Path,
    store: StateStore,
    output: Callable[[str], None] | None = print,
) -> list[dict[str, Any]]:
    """按输入时间顺序处理文件；inspect 报告同时返回，便于测试断言。"""
    path = Path(path)
    kinds = _kinds_for(path)
    state = store.load()
    bus = RepairBus(state)
    reports: list[dict[str, Any]] = []

    def emit(text: str) -> None:
        if output is not None:
            output(text)

    for event in load_events(path, kinds):
        kind = event["kind"]
        record = dict(event["record"])
        at = event["at"]

        if kind == "publish":
            bus.publish(at=at, **record)
        elif kind == "claim":
            bus.claim(at=at, **record)
        elif kind == "dead":
            bus.mark_dead(at=at, **record)
        elif kind == "effect":
            bus.report_effect(at=at, **record)
        elif kind == "amend":
            bus.amend(at=at, **record)
        elif kind == "compensated":
            bus.report_compensated(at=at, **record)
        elif kind == "repair":
            bus.create_repair(at=at, **record)
        elif kind == "restart":
            store.save(bus.state)
            # 显式丢弃内存对象，从原子快照恢复，验证未消解回执可接续。
            restored = RepairBus(store.load())
            restart_report = restored.record_restart(at=at)
            store.save(restored.state)
            bus = restored
            emit(json.dumps({"restart": restart_report}, ensure_ascii=False, indent=2))
        elif kind == "inspect":
            report = bus.inspect(**record)
            report["at"] = at
            reports.append(report)
            store.save(bus.state)
            emit(json.dumps(report, ensure_ascii=False, indent=2))
        else:  # pragma: no cover - scenario_reader 已限制 kinds
            raise ValueError(f"未处理的事件类型：{kind}")

        if kind != "restart":
            store.save(bus.state)

    return reports
