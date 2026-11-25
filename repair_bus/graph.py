"""因果依赖图与环检测。图节点直接使用 group/command@rev 投递/凭据标识。"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable

from .model import parse_ref


def build_graph(deliveries: dict) -> tuple[set[str], dict[str, list[str]]]:
    nodes: set[str] = set(deliveries)
    graph: dict[str, list[str]] = {node: [] for node in deliveries}
    for node, delivery in deliveries.items():
        # 来源已撤销的处理不再继续等待自己的依赖；保留节点供审计，但它没有出边，
        # 因而不会继续把一个已解除的环误报成活动环。
        if delivery.get("phase") == "withdrawn":
            continue
        for need in delivery.get("needs", []):
            # 发布时已经校验；持久化状态中的异常数据不应使检查整体崩溃。
            try:
                ref = str(parse_ref(need))
            except ValueError:
                continue
            graph[node].append(ref)
            nodes.add(ref)
            graph.setdefault(ref, [])
    return nodes, graph


def strongly_connected_cycles(deliveries: dict) -> list[list[str]]:
    """返回所有大小 >1 或存在自边的强连通分量，分量内按名称排序。"""
    nodes, graph = build_graph(deliveries)

    index = 0
    indices: dict[str, int] = {}
    low: dict[str, int] = {}
    stack: list[str] = []
    on_stack: set[str] = set()
    result: list[list[str]] = []

    # Tarjan 递归在常规演练规模下足够；输入规模很大时可再换迭代实现。
    def visit(node: str) -> None:
        nonlocal index
        indices[node] = index
        low[node] = index
        index += 1
        stack.append(node)
        on_stack.add(node)

        for nxt in graph.get(node, []):
            if nxt not in indices:
                visit(nxt)
                low[node] = min(low[node], low[nxt])
            elif nxt in on_stack:
                low[node] = min(low[node], indices[nxt])

        if low[node] == indices[node]:
            component: list[str] = []
            while True:
                cur = stack.pop()
                on_stack.remove(cur)
                component.append(cur)
                if cur == node:
                    break
            if len(component) > 1:
                result.append(sorted(component))
            elif component and node in graph.get(node, []):
                result.append(component)

    for node in sorted(nodes):
        if node not in indices:
            visit(node)
    return sorted(result, key=lambda comp: comp[0])


def cycle_index(deliveries: dict) -> dict[str, list[str]]:
    """节点 -> 所属环（同一 SCC 列表）。"""
    out: dict[str, list[str]] = {}
    for component in strongly_connected_cycles(deliveries):
        for node in component:
            out[node] = component
    return out


def descendants(roots: Iterable[str], deliveries: dict) -> set[str]:
    """返回等待 roots 中节点的业务分支（反向依赖闭包）。"""
    reverse: dict[str, set[str]] = defaultdict(set)
    for node, delivery in deliveries.items():
        for need in delivery.get("needs", []):
            reverse[need].add(node)

    seen: set[str] = set()
    queue = list(roots)
    while queue:
        node = queue.pop()
        for dependent in reverse.get(node, ()):  # type: ignore[arg-type]
            if dependent not in seen:
                seen.add(dependent)
                queue.append(dependent)
    return seen
