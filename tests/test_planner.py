"""求解器测试：可解顺序、先订阅端约束、无解反例、起点不安全场景。"""

import json
from pathlib import Path

import pytest

from app.models import PlanRequest
from app.planner import solve

SAMPLES = Path(__file__).resolve().parent.parent / "samples"


def _load(name: str) -> PlanRequest:
    with (SAMPLES / name).open(encoding="utf-8") as fh:
        return PlanRequest.model_validate(json.load(fh))


def test_safe_sample_feasible_and_subscribers_finish_before_producer_new_phase():
    req = _load("safe_subscriber_first.json")
    resp = solve(req.parties)

    assert resp.feasible
    assert resp.initial_state_safe
    # 每方 old->mixed->new 各两步，共 6 步
    assert len(resp.steps) == 6
    # 状态单调推进且始终合法
    for step in resp.steps:
        assert step.from_phase != "new"
        assert set(step.state_after.values()) <= {"old", "mixed", "new"}

    # 关键顺序约束：发布端进入 new（即 mixed->new）之前，
    # 两个订阅端都必须已经完成升级（new），否则旧订阅端会收到 L3 或未知字段
    producer_id = req.producer().id
    sub_ids = [s.id for s in req.subscribers()]

    producer_new_step = next(
        s for s in resp.steps if s.party_id == producer_id and s.to_phase == "new"
    )
    for sid in sub_ids:
        sub_done_step = next(
            s for s in resp.steps if s.party_id == sid and s.to_phase == "new"
        )
        assert sub_done_step.order < producer_new_step.order

    # 发布端的第一步（old->mixed）也必须在两个订阅端都进入 mixed 之后：
    # mixed 发布端即可发出新版（含 defect_level）消息
    producer_mixed_step = next(
        s for s in resp.steps if s.party_id == producer_id and s.to_phase == "mixed"
    )
    for sid in sub_ids:
        sub_mixed_step = next(
            s for s in resp.steps if s.party_id == sid and s.to_phase == "mixed"
        )
        assert sub_mixed_step.order < producer_mixed_step.order


def test_infeasible_sample_reports_blocked_pairs_and_counterexamples():
    req = _load("infeasible_circular.json")
    resp = solve(req.parties)

    assert not resp.feasible
    assert resp.initial_state_safe  # 起点 old/old/old 是兼容的
    assert resp.blocked_pairs

    # 必须包含两个方向的死锁证据
    labels = {b.pair for b in resp.blocked_pairs}
    assert "collector:new -> qc:old" in labels       # 新枚举 L3 旧质检不认
    assert "collector:old -> qc:new" in labels       # 新质检必填 inspector_id 旧发布端不发

    enum_pair = next(b for b in resp.blocked_pairs if b.pair == "collector:new -> qc:old")
    assert any(v.kind == "enum_violation" and v.example_value == "L3" for v in enum_pair.violations)
    assert enum_pair.counterexample["defect_level"] == "L3"
    # 反例是发布端确实可发出的消息：必填字段齐全
    for required_field in ("crack_id", "car_id", "mileage_m", "length_mm", "defect_level"):
        assert required_field in enum_pair.counterexample

    missing_pair = next(b for b in resp.blocked_pairs if b.pair == "collector:old -> qc:new")
    assert any(v.kind == "required_missing" and v.field == "inspector_id" for v in missing_pair.violations)
    assert "inspector_id" not in missing_pair.counterexample

    # 每个受阻组合至少解释了一条被挡住的升级动作
    for report in resp.blocked_pairs:
        assert report.blocks_transitions


def test_unsafe_initial_state_has_no_plan():
    """旧发布端不发某字段，而旧订阅端必填：起点即不安全。"""
    payload = {
        "parties": [
            {
                "id": "p", "role": "producer",
                "old_schema": [{"name": "a", "type": "string", "required": True}],
                "new_schema": [{"name": "a", "type": "string", "required": True}],
            },
            {
                "id": "s1", "role": "subscriber",
                "old_schema": [
                    {"name": "a", "type": "string", "required": True},
                    {"name": "b", "type": "integer", "required": True},
                ],
                "new_schema": [
                    {"name": "a", "type": "string", "required": True},
                    {"name": "b", "type": "integer", "required": True},
                ],
            },
            {
                "id": "s2", "role": "subscriber",
                "old_schema": [{"name": "a", "type": "string", "required": True}],
                "new_schema": [{"name": "a", "type": "string", "required": True}],
            },
        ]
    }
    req = PlanRequest.model_validate(payload)
    resp = solve(req.parties)
    assert not resp.feasible
    assert not resp.initial_state_safe
    assert resp.steps == []


def test_compatible_identical_schemas_six_step_plan():
    schema = [
        {"name": "id", "type": "string", "required": True},
        {"name": "n", "type": "integer", "required": False},
    ]
    payload = {
        "parties": [
            {"id": "p", "role": "producer", "old_schema": schema, "new_schema": schema},
            {"id": "s1", "role": "subscriber", "old_schema": schema, "new_schema": schema},
            {"id": "s2", "role": "subscriber", "old_schema": schema, "new_schema": schema},
        ]
    }
    req = PlanRequest.model_validate(payload)
    resp = solve(req.parties)
    assert resp.feasible
    assert [s.to_phase for s in resp.steps].count("new") == 3
    assert resp.steps[-1].state_after == {"p": "new", "s1": "new", "s2": "new"}


def test_request_role_shape_validation():
    bad = {
        "parties": [
            {"id": "p", "role": "producer", "old_schema": [], "new_schema": []},
            {"id": "s1", "role": "producer", "old_schema": [], "new_schema": []},
            {"id": "s2", "role": "subscriber", "old_schema": [], "new_schema": []},
        ]
    }
    with pytest.raises(ValueError):
        PlanRequest.model_validate(bad)


def test_duplicate_field_names_rejected():
    bad = {
        "parties": [
            {
                "id": "p", "role": "producer",
                "old_schema": [
                    {"name": "a", "type": "string", "required": True},
                    {"name": "a", "type": "integer", "required": True},
                ],
                "new_schema": [],
            },
            {"id": "s1", "role": "subscriber", "old_schema": [], "new_schema": []},
            {"id": "s2", "role": "subscriber", "old_schema": [], "new_schema": []},
        ]
    }
    with pytest.raises(ValueError):
        PlanRequest.model_validate(bad)
