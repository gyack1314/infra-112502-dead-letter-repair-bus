"""把结构化报告渲染成值班员可读的中文文本。"""

from __future__ import annotations

import json
from typing import Any


def _blocker_text(blocker: dict[str, Any]) -> str:
    code = blocker.get("code")
    if code == "missing_credential":
        return f"缺少凭据 {blocker.get('ref')}"
    if code == "prerequisite_withdrawn":
        return f"前置 {blocker.get('ref')} 已撤销，需换凭据或撤销当前处理"
    if code in {"prerequisite_unknown", "own_result_unknown", "prior_result_unknown"}:
        ref = blocker.get("ref") or blocker.get("attempt") or "当前消息"
        return f"结果不明：{ref}"
    if code == "obsolete_dependency":
        return f"依赖旧修订 {blocker.get('ref')}，可换用 {blocker.get('new_ref')} 或撤销"
    if code in {"compensation_required", "await_compensation"}:
        return f"需先补偿原影响 {blocker.get('effect_id')}（{blocker.get('ref', '')}）"
    if code == "compensated_credential":
        return f"凭据 {blocker.get('ref')} 已补偿，不能抵作新修订完成"
    if code == "dependency_cycle":
        return "依赖成环：" + " -> ".join(blocker.get("cycle", []))
    if code == "superseded_revision":
        return f"本修订已过时，新修订为 {blocker.get('new_ref')}"
    if code == "incomplete_information":
        return "资料不全，等待更正后的新修订"
    return blocker.get("message") or code or "阻塞"


def _line_for_view(view: dict[str, Any], indent: str = "  ") -> list[str]:
    dead = f" dead={view['dead_reason']}" if view.get("dead_reason") else ""
    effect = f" effect={view['effect_id']}" if view.get("effect_id") else ""
    lines = [
        f"{indent}- {view['id']} [{view['phase']}]{dead}{effect} → {view['next_action']}"
    ]
    for blocker in view.get("blockers", []):
        lines.append(f"{indent}    · {_blocker_text(blocker)}")
    if view.get("seen_facts"):
        lines.append(f"{indent}    本组已见凭据：{', '.join(view['seen_facts'])}")
    return lines


def render_text(report: dict[str, Any]) -> str:
    kind = report.get("report")
    lines: list[str] = []

    if kind == "repair":
        lines.append(f"修复申请 {report['id']}：当前决策 {report['decision']}")
        if report.get("chain_owner"):
            overlap = f"，重叠申请：{', '.join(report['overlapping_repairs'])}" if report.get("overlapping_repairs") else ""
            lines.append(f"因果链 {report['chain_id']}，锁/首单 {report['chain_owner']}{overlap}")
        if report.get("cycles"):
            lines.append("受阻环：")
            for cycle in report["cycles"]:
                lines.append("  ◯ " + " -> ".join(cycle))
        if report.get("cycle_blocked_branches"):
            lines.append("等待该环的分支：" + ", ".join(report["cycle_blocked_branches"]))
        if report.get("unknown"):
            ids = ", ".join(item["id"] for item in report["unknown"])
            lines.append(f"仍不明的前置：{ids}")
        if report.get("compensations"):
            for comp in report["compensations"]:
                cid = comp.get("compensation_id") or "待确认"
                lines.append(f"补偿动作 {comp['effect_id']}：{comp.get('status', 'required')} / {cid}")
        cont = report.get("continuable_actions") or []
        if cont:
            cont_text = "; ".join(f"{item['group']} → {item['action']} ({item['id']})" for item in cont)
        else:
            cont_text = "无"
        lines.append("可继续消费者：" + cont_text)
        reusable = report.get("reusable_original_results") or []
        if reusable:
            reusable_text = ", ".join(f"{item['id']}({item.get('effect_id')})" for item in reusable)
            lines.append("可沿用的原结果凭据：" + reusable_text)
        lines.append("可放行后续：" + (", ".join(report["releasable"]) or "无"))
        lines.append("已兑现勿重复：" + (", ".join(x["id"] + "(" + str(x.get("effect_id")) + ")" for x in report["do_not_repeat"]) or "无"))
        lines.append("不受影响：" + (", ".join(report["unaffected"]) or "无"))
        lines.append("投递明细：")
        for view in report["deliveries"]:
            lines.extend(_line_for_view(view))
        return "\n".join(lines)

    if kind == "dead_letter":
        for letter in report["dead_letters"]:
            lines.append(f"死信 {letter['dead_letter']}：当前决策 {letter['decision']}")
            lines.append(f"可放行：{', '.join(letter['releasable']) or '无'}")
            if letter.get("processable"):
                lines.append(f"已领取且现在可继续处理：{', '.join(letter['processable'])}")
            for eye in letter["consumer_eyes"]:
                labels = {
                    "dead_letter_owner": "死信所属消费方",
                    "peer_consumer": "并列消费方",
                    "amended_revision": "更正后修订的消费方",
                    "downstream_consumer": "下游消费方",
                }
                label = labels.get(eye["perspective"], eye["perspective"])
                lines.append(f"[{label} / {eye['group']}]")
                lines.extend(_line_for_view(eye["view"], "  "))
        return "\n".join(lines)

    if kind == "command":
        lines.append(f"命令 {report['command']}（当前修订 @{report.get('current_rev')}）")
        lines.append("各消费方：")
        for view in report["consumer_views"]:
            lines.extend(_line_for_view(view))
        if report.get("dependent_views"):
            lines.append("下游：")
            for view in report["dependent_views"]:
                lines.extend(_line_for_view(view))
        lines.append("可放行：" + (", ".join(report["releasable"]) or "无"))
        return "\n".join(lines)

    if kind == "group":
        lines.append(f"消费组 {report['group']}")
        lines.append("已见事实：" + (", ".join(sorted(report.get("seen_facts", {}))) or "无"))
        lines.append("未消解回执：" + (", ".join(r["id"] for r in report.get("pending_receipts", [])) or "无"))
        for view in report["deliveries"]:
            lines.extend(_line_for_view(view))
        return "\n".join(lines)

    if kind == "amendments":
        lines.append(f"命令 {report['command']} 的更正状态（当前 @{report.get('current_rev')}）")
        if not report.get("amendments"):
            lines.append("  无更正")
        for amend in report["amendments"]:
            lines.append(
                f"  - @{amend['rev']} changed={amend['changed']} "
                f"基于 @{amend['correction_of']} → {amend['status']}"
            )
            for impact in amend.get("impacts", []):
                comp = impact.get("compensation") or {}
                comp_text = comp.get("compensation_id") or comp.get("status") or "无补偿记录"
                lines.append(
                    f"      原影响 {impact['ref']}: {impact.get('status') or impact.get('result_state')} "
                    f"effect={impact.get('effect_id')}；补偿={comp_text}"
                )
            if amend.get("next_actions"):
                lines.append("      下一步：" + ", ".join(amend["next_actions"]))
        return "\n".join(lines)

    if kind == "overview":
        lines.append("中间件总览")
        lines.append("死信：" + (", ".join(report["dead_letters"]) or "无"))
        lines.append("未消解回执：" + (", ".join(r["id"] for r in report["pending_receipts"]) or "无"))
        lines.append("可放行：" + (", ".join(report["releasable"]) or "无"))
        for view in report["deliveries"]:
            lines.extend(_line_for_view(view))
        return "\n".join(lines)

    return json.dumps(report, ensure_ascii=False, indent=2)
