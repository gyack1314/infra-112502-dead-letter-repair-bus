"""repair_bus 命令行运行入口。

示例：
  python3 -m repair_bus replay examples/unknown-effect-then-amend.jsonl \
      --store runtime/demo.json
  python3 -m repair_bus inspect --repair r1 --store runtime/demo.json
  python3 -m repair_bus dead-letter --command take --store runtime/demo.json

也可把全局参数放在子命令前：
  python3 -m repair_bus --state runtime/demo.json overview
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .engine import RepairBus
from .errors import RepairBusError
from .reports import render_text
from .replay import replay_file
from .simulation import LocalConsumerAdapter
from .store import StateStore


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="repair_bus", description="死信因果链修复中间件")
    parser.add_argument("--store", "--state", dest="store", default=".repair-bus-state.json", help="JSON 状态快照路径")
    parser.add_argument("--json", action="store_true", help="所有结果输出为 JSON")

    common = argparse.ArgumentParser(add_help=False)
    # SUPPRESS 使子命令后的参数覆盖主解析器默认值，同时允许全局参数放在子命令前或后。
    common.add_argument("--store", "--state-file", dest="store", default=argparse.SUPPRESS, help="JSON 状态快照路径")
    common.add_argument("--json", action="store_true", default=argparse.SUPPRESS, help="输出 JSON")

    sub = parser.add_subparsers(dest="cmd", required=True)

    def add(name: str, **kwargs: Any) -> argparse.ArgumentParser:
        aliases = kwargs.pop("aliases", [])
        return sub.add_parser(name, parents=[common], aliases=aliases, **kwargs)

    p = add("replay", help="按时间顺序回放 JSONL 演练/运营记录")
    p.add_argument("file")

    p = add("publish", help="正常发布业务命令修订")
    p.add_argument("--key", required=True)
    p.add_argument("--command", required=True)
    p.add_argument("--rev", required=True, type=int)
    p.add_argument("--consumers", nargs="+", required=True)
    p.add_argument("--needs", nargs="*", default=[])

    p = add("claim", help="消费组领取投递")
    p.add_argument("--group", required=True)
    p.add_argument("--command", required=True)
    p.add_argument("--rev", type=int)
    p.add_argument("--attempt")

    p = add("dead", help="上报死信及原因")
    p.add_argument("--group")
    p.add_argument("--command")
    p.add_argument("--attempt", required=True)
    p.add_argument("--reason", "--dead-reason", dest="reason", required=True, choices=["unavailable", "incomplete", "unknown"])

    p = add("effect", aliases=["receipt"], help="外部业务端处理回执/查询结果")
    p.add_argument("--group", required=True)
    p.add_argument("--command", required=True)
    p.add_argument("--rev", required=True, type=int)
    p.add_argument("--state", required=True, choices=["applied", "not_applied", "unknown"])
    p.add_argument("--attempt")
    p.add_argument("--effect-id")

    p = add("amend", help="发布更正/新修订")
    p.add_argument("--command", required=True)
    p.add_argument("--rev", required=True, type=int)
    p.add_argument("--changed", required=True, choices=["effect", "context"])

    p = add("amendments", help="查询命令的更正状态与补偿进度")
    p.add_argument("--command", required=True)

    p = add("compensated", help="确认原 effect_id 的补偿结果")
    p.add_argument("--effect-id", required=True)
    p.add_argument("--compensation-id", required=True)
    p.add_argument("--group")
    p.add_argument("--command")

    p = add("repair", help="提交覆盖多个命令的修复申请")
    p.add_argument("id")
    p.add_argument("commands", nargs="+")

    p = add("inspect", help="查询修复申请、命令或消费组")
    p.add_argument("--repair")
    p.add_argument("--command")
    p.add_argument("--group")
    p.add_argument("--rev", type=int)
    p.add_argument("--dead", action="store_true")
    p.add_argument("--attempt")

    p = add("dead-letter", help="查询死信在相关消费方眼中的状态")
    p.add_argument("--command")
    p.add_argument("--attempt")
    p.add_argument("--rev", type=int)

    p = add("release", help="定向放行并向指定消费组补授凭据")
    p.add_argument("--group")
    p.add_argument("--command")
    p.add_argument("--repair")

    p = add("retarget", help="来源决定把旧依赖换成新凭据")
    p.add_argument("--group")
    p.add_argument("--command")
    p.add_argument("--rev", type=int)
    p.add_argument("--delivery")
    p.add_argument("--old-ref")
    p.add_argument("--new-ref", required=True)

    p = add("withdraw", help="来源撤掉尚未兑现的投递")
    p.add_argument("--group")
    p.add_argument("--command")
    p.add_argument("--rev", type=int)
    p.add_argument("--delivery")
    p.add_argument("--reason", default="source_withdrew")

    p = add("consume", help="本地仿真：领取并执行一次消费")
    p.add_argument("--group", required=True)
    p.add_argument("--command", required=True)
    p.add_argument("--rev", type=int)
    p.add_argument("--attempt")
    p.add_argument("--fail", choices=["unavailable", "incomplete", "unknown"])
    p.add_argument("--effect-after-dead", action="store_true")

    add("pending", help="列出重启后仍需查询的外部回执")
    add("restart", help="模拟进程重启并恢复未消解回执")
    add("overview", help="输出中间件总览")
    return parser


def _print(value: Any, as_json: bool) -> None:
    if as_json:
        print(json.dumps(value, ensure_ascii=False, indent=2, default=str))
    elif isinstance(value, dict) and "report" in value:
        print(render_text(value))
    else:
        print(json.dumps(value, ensure_ascii=False, indent=2, default=str))


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    store = StateStore(args.store)

    try:
        if args.cmd == "replay":
            reports = replay_file(Path(args.file), store, output=None if args.json else print)
            if args.json:
                print(json.dumps(reports, ensure_ascii=False, indent=2))
            return 0

        state = store.load()
        bus = RepairBus(state)
        result: Any

        if args.cmd == "publish":
            result = bus.publish(
                key=args.key,
                command=args.command,
                rev=args.rev,
                consumers=args.consumers,
                needs=args.needs,
            )
        elif args.cmd == "claim":
            result = bus.claim(group=args.group, command=args.command, rev=args.rev, attempt=args.attempt)
        elif args.cmd == "dead":
            result = bus.mark_dead(
                group=args.group,
                command=args.command,
                attempt=args.attempt,
                dead_reason=args.reason,
            )
        elif args.cmd in {"effect", "receipt"}:
            result = bus.report_effect(
                group=args.group,
                command=args.command,
                rev=args.rev,
                state=args.state,
                attempt=args.attempt,
                effect_id=args.effect_id,
            )
        elif args.cmd == "amend":
            result = bus.amend(command=args.command, rev=args.rev, changed=args.changed)
        elif args.cmd == "amendments":
            result = bus.amendment_report(args.command)
        elif args.cmd == "compensated":
            result = bus.report_compensated(
                group=args.group,
                command=args.command,
                effect_id=args.effect_id,
                compensation_id=args.compensation_id,
            )
        elif args.cmd == "repair":
            result = bus.create_repair(id=args.id, commands=args.commands)
        elif args.cmd == "inspect":
            if args.repair:
                result = bus.repair_report(args.repair)
            elif args.dead:
                result = bus.dead_report(command=args.command, attempt=args.attempt, rev=args.rev)
            elif args.command or args.group:
                result = bus.inspect(command=args.command, group=args.group, rev=args.rev)
            else:
                result = bus.overview()
        elif args.cmd == "dead-letter":
            result = bus.dead_report(command=args.command, attempt=args.attempt, rev=args.rev)
        elif args.cmd == "release":
            result = bus.release(group=args.group, command=args.command, repair_id=args.repair)
        elif args.cmd == "retarget":
            did = args.delivery
            if not did and not (args.group and args.command):
                raise RepairBusError("retarget 需要 --delivery 或 --group/--command")
            result = bus.retarget_dependency(
                did=did,
                group=args.group,
                command=args.command,
                rev=args.rev,
                old_ref=args.old_ref,
                new_ref=args.new_ref,
            )
        elif args.cmd == "withdraw":
            result = bus.withdraw(
                did=args.delivery,
                group=args.group,
                command=args.command,
                rev=args.rev,
                reason=args.reason,
            )
        elif args.cmd == "consume":
            adapter = LocalConsumerAdapter(bus)
            result = adapter.consume(
                group=args.group,
                command=args.command,
                rev=args.rev,
                attempt=args.attempt,
                fail=args.fail,
                effect_after_dead=args.effect_after_dead,
            )
        elif args.cmd == "pending":
            result = {"report": "pending", "pending_receipts": bus.pending_receipts()}
        elif args.cmd == "restart":
            store.save(bus.state)
            bus = RepairBus(store.load())
            result = bus.record_restart()
        elif args.cmd == "overview":
            result = bus.overview()
        else:  # pragma: no cover
            parser.error(f"未知命令 {args.cmd}")
            return 2

        store.save(bus.state)
        _print(result, args.json)
        return 0
    except (RepairBusError, ValueError, FileNotFoundError) as error:
        print(f"错误：{error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
