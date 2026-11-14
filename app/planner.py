"""滚动升级安全顺序求解。

每一方在滚动升级中经历 old -> mixed -> new 三个阶段：
- producer 在 mixed 阶段新旧实例同时运行，因此可发出 old 与 new 两类消息；
- subscriber 在 mixed 阶段新旧实例同时运行，因此两类消息都必须能被接收。

安全状态的定义：任一在运行的发布端版本可能发出的每条消息，
都必须被两个订阅端所有在运行的版本接收。
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass

from .compat import check_pair
from .models import (
    BlockedPairReport,
    BlockedTransition,
    PairCompatibility,
    PartySpec,
    Phase,
    PlanResponse,
    PlanStep,
)

PHASES: tuple[Phase, ...] = ("old", "mixed", "new")
# 各阶段处于在运行状态的契约版本
ACTIVE_VERSIONS: dict[Phase, tuple[str, ...]] = {
    "old": ("old",),
    "mixed": ("old", "new"),
    "new": ("new",),
}


@dataclass(frozen=True)
class Node:
    """BFS 节点：三方当前阶段。"""

    state: tuple[Phase, Phase, Phase]
    parent: "Node | None"
    action: tuple[int, Phase, Phase] | None  # (party_index, from, to)


def _is_safe(
    state: tuple[Phase, Phase, Phase],
    producer: PartySpec,
    subscribers: list[PartySpec],
    pair_cache: dict[tuple[str, str, str, str], PairCompatibility],
) -> bool:
    """状态是否安全：所有在运行的 (发布端版本, 订阅端版本) 组合必须兼容。"""
    prod_phase = state[0]
    for pv in ACTIVE_VERSIONS[prod_phase]:
        for idx, sub in enumerate(subscribers, start=1):
            for sv in ACTIVE_VERSIONS[state[idx]]:
                key = (producer.id, pv, sub.id, sv)
                pair = pair_cache.get(key)
                if pair is None:
                    pair = check_pair(producer, pv, sub, sv)
                    pair_cache[key] = pair
                if not pair.compatible:
                    return False
    return True


def _pair_label(pair: PairCompatibility) -> str:
    return (
        f"{pair.producer_id}:{pair.producer_version} -> "
        f"{pair.subscriber_id}:{pair.subscriber_version}"
    )


def solve(req_parties: list[PartySpec]) -> PlanResponse:
    """对 1 发布端 + 2 订阅端求解 old...old -> new...new 的安全升级顺序。"""
    producer = next(p for p in req_parties if p.role == "producer")
    subscribers = [p for p in req_parties if p.role == "subscriber"]
    parties = [producer, *subscribers]
    pair_cache: dict[tuple[str, str, str, str], PairCompatibility] = {}

    start: tuple[Phase, Phase, Phase] = ("old", "old", "old")
    goal: tuple[Phase, Phase, Phase] = ("new", "new", "new")

    initial_safe = _is_safe(start, producer, subscribers, pair_cache)

    # BFS：每一步只允许一方沿 old->mixed->new 推进一步
    queue: deque[Node] = deque([Node(start, None, None)])
    visited: set[tuple[Phase, Phase, Phase]] = {start}
    goal_node: Node | None = None
    # 记录每条被挡住的单步升级，供无解报告使用
    blocked: list[BlockedTransition] = []

    while queue:
        node = queue.popleft()
        if node.state == goal:
            goal_node = node
            break
        for idx in range(3):
            cur = node.state[idx]
            if cur == "new":
                continue
            nxt = PHASES[PHASES.index(cur) + 1]
            new_state = list(node.state)
            new_state[idx] = nxt
            new_state_t = tuple(new_state)  # type: ignore[assignment]
            if new_state_t in visited:
                continue
            if _is_safe(new_state_t, producer, subscribers, pair_cache):
                visited.add(new_state_t)
                queue.append(Node(new_state_t, node, (idx, cur, nxt)))
            else:
                # 收集该步被哪些在运行版本组合挡住（按需计算并回填缓存）
                blocking: list[str] = []
                prod_phase = new_state_t[0]
                for pv in ACTIVE_VERSIONS[prod_phase]:
                    for s_idx, sub in enumerate(subscribers, start=1):
                        for sv in ACTIVE_VERSIONS[new_state_t[s_idx]]:
                            key = (producer.id, pv, sub.id, sv)
                            pair = pair_cache.get(key)
                            if pair is None:
                                pair = check_pair(producer, pv, sub, sv)
                                pair_cache[key] = pair
                            if not pair.compatible:
                                blocking.append(_pair_label(pair))
                blocked.append(
                    BlockedTransition(
                        party_id=parties[idx].id,
                        from_phase=cur,
                        to_phase=nxt,
                        state_before={parties[i].id: node.state[i] for i in range(3)},
                        state_after={parties[i].id: new_state_t[i] for i in range(3)},
                        blocking_pairs=sorted(set(blocking)),
                        reason=(
                            f"升级 {parties[idx].id} {cur}->{nxt} 后，在运行版本组合 "
                            f"{sorted(set(blocking))} 存在拒收消息"
                        ),
                    )
                )

    if goal_node is not None:
        return _build_feasible(goal_node, parties, initial_safe)
    return _build_infeasible(producer, subscribers, pair_cache, blocked, initial_safe)


def _build_feasible(goal_node: Node, parties: list[PartySpec], initial_safe: bool) -> PlanResponse:
    # 回溯动作链
    actions: list[tuple[int, Phase, Phase]] = []
    node: Node | None = goal_node
    while node is not None and node.action is not None:
        actions.append(node.action)
        node = node.parent
    actions.reverse()

    state: list[Phase] = ["old", "old", "old"]
    steps: list[PlanStep] = []
    for order, (idx, frm, to) in enumerate(actions, start=1):
        state[idx] = to
        party = parties[idx]
        if party.role == "producer":
            note = (
                f"发布端 {frm}->{to}：此刻两个订阅端的在运行版本均能接收其全部可发消息"
            )
        else:
            note = (
                f"订阅端 {party.id} {frm}->{to}：其在运行版本均能接收发布端当前全部可发消息"
            )
        steps.append(
            PlanStep(
                order=order,
                party_id=party.id,
                role=party.role,
                from_phase=frm,
                to_phase=to,
                state_after={parties[i].id: state[i] for i in range(3)},
                note=note,
            )
        )

    order_names = " -> ".join(s.party_id for s in steps)
    summary = (
        f"存在安全升级顺序（共 {len(steps)} 步）：{order_names}。"
        "每一步后的所有在运行发布端版本 × 订阅端版本组合均通过兼容性检查。"
    )
    return PlanResponse(
        feasible=True,
        summary=summary,
        initial_state_safe=initial_safe,
        steps=steps,
    )


def _build_infeasible(
    producer: PartySpec,
    subscribers: list[PartySpec],
    pair_cache: dict[tuple[str, str, str, str], PairCompatibility],
    blocked: list[BlockedTransition],
    initial_safe: bool,
) -> PlanResponse:
    # 汇总所有不兼容的版本组合，并关联它们挡住的升级动作
    bad_pairs = [p for p in pair_cache.values() if not p.compatible]
    bad_pairs.sort(key=lambda p: _pair_label(p))

    reports: list[BlockedPairReport] = []
    for pair in bad_pairs:
        label = _pair_label(pair)
        related = [b for b in blocked if label in b.blocking_pairs]
        # 某组合可能从未出现在任何“尝试过的动作”里（例如起点即不安全），
        # 仍需在报告中保留，blocks_transitions 可为空。
        reports.append(
            BlockedPairReport(
                pair=label,
                producer_id=pair.producer_id,
                producer_version=pair.producer_version,
                subscriber_id=pair.subscriber_id,
                subscriber_version=pair.subscriber_version,
                violations=pair.violations,
                counterexample=pair.counterexample or {},
                blocks_transitions=related,
            )
        )

    pair_names = "; ".join(_pair_label(p) for p in bad_pairs)
    if not initial_safe:
        head = "初始状态（三方均为 old）本身即不安全，不存在任何升级顺序。"
    else:
        head = "状态空间中不存在从全 old 到全 new 的安全路径，滚动升级无解。"
    summary = (
        f"{head}受阻版本组合：{pair_names}。"
        "各组合的字段级违规与可发出即被拒收的示例消息见 blocked_pairs。"
    )
    return PlanResponse(
        feasible=False,
        summary=summary,
        initial_state_safe=initial_safe,
        blocked_pairs=reports,
    )
