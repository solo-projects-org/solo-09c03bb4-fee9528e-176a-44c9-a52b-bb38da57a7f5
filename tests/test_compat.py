"""兼容性引擎单元测试：覆盖三类违规与未知字段/可选缺失语义。"""

import pytest

from app.compat import check_pair
from app.models import FieldDef, PartySpec


def _field(name, ftype, required, enum_values=None) -> FieldDef:
    return FieldDef(name=name, type=ftype, required=required, enum_values=enum_values)


def _party(pid, role, old, new) -> PartySpec:
    return PartySpec(id=pid, role=role, old_schema=old, new_schema=new)


def test_subscriber_ignores_unknown_fields():
    producer = _party("p", "producer", [], [_field("x", "integer", True)])
    sub = _party("s", "subscriber", [], [])
    pair = check_pair(producer, "new", sub, "new")
    assert pair.compatible
    assert pair.violations == []


def test_optional_publisher_field_may_be_missing_when_subscriber_optional():
    producer = _party("p", "producer", [], [_field("note", "string", False)])
    sub = _party("s", "subscriber", [], [_field("note", "string", False)])
    pair = check_pair(producer, "new", sub, "new")
    assert pair.compatible


def test_optional_publisher_field_blocks_required_subscriber_field():
    producer = _party("p", "producer", [], [_field("note", "string", False)])
    sub = _party("s", "subscriber", [], [_field("note", "string", True)])
    pair = check_pair(producer, "new", sub, "new")
    assert not pair.compatible
    assert len(pair.violations) == 1
    assert pair.violations[0].kind == "required_missing"
    assert "note" not in pair.counterexample


def test_undeclared_required_field_is_always_missing():
    producer = _party("p", "producer", [], [_field("a", "string", True)])
    sub = _party(
        "s", "subscriber", [],
        [_field("a", "string", True), _field("b", "integer", True)],
    )
    pair = check_pair(producer, "new", sub, "new")
    kinds = {v.field: v.kind for v in pair.violations}
    assert kinds == {"b": "required_missing"}
    assert pair.counterexample == {"a": "sample"}


def test_type_mismatch_integer_vs_string():
    producer = _party("p", "producer", [], [_field("v", "integer", True)])
    sub = _party("s", "subscriber", [], [_field("v", "string", True)])
    pair = check_pair(producer, "new", sub, "new")
    assert pair.violations[0].kind == "type_mismatch"
    assert pair.counterexample["v"] == 1


def test_enum_value_outside_subscriber_domain():
    producer = _party(
        "p", "producer", [],
        [_field("lvl", "enum", True, ["L1", "L2", "L3"])],
    )
    sub = _party(
        "s", "subscriber", [],
        [_field("lvl", "enum", True, ["L1", "L2"])],
    )
    pair = check_pair(producer, "new", sub, "new")
    v = pair.violations[0]
    assert v.kind == "enum_violation"
    assert v.example_value == "L3"
    assert pair.counterexample["lvl"] == "L3"


def test_unbounded_string_vs_enum_is_violation():
    producer = _party("p", "producer", [], [_field("lvl", "string", True)])
    sub = _party(
        "s", "subscriber", [],
        [_field("lvl", "enum", True, ["L1", "L2"])],
    )
    pair = check_pair(producer, "new", sub, "new")
    assert pair.violations[0].kind == "enum_violation"
    assert pair.counterexample["lvl"] not in ("L1", "L2")


def test_enum_publisher_into_plain_string_subscriber_is_safe():
    producer = _party(
        "p", "producer", [],
        [_field("lvl", "enum", True, ["L1", "L2"])],
    )
    sub = _party("s", "subscriber", [], [_field("lvl", "string", True)])
    assert check_pair(producer, "new", sub, "new").compatible


def test_enum_subset_is_safe():
    producer = _party(
        "p", "producer", [],
        [_field("lvl", "enum", True, ["L1", "L2"])],
    )
    sub = _party(
        "s", "subscriber", [],
        [_field("lvl", "enum", True, ["L1", "L2", "L3"])],
    )
    assert check_pair(producer, "new", sub, "new").compatible


def test_optional_field_type_mismatch_still_flagged():
    producer = _party("p", "producer", [], [_field("v", "integer", False)])
    sub = _party("s", "subscriber", [], [_field("v", "string", False)])
    pair = check_pair(producer, "new", sub, "new")
    assert pair.violations[0].kind == "type_mismatch"


def test_counterexample_keeps_all_required_fields():
    producer = _party(
        "p", "producer", [],
        [
            _field("id", "string", True),
            _field("n", "integer", True),
            _field("lvl", "enum", False, ["L1", "L9"]),
        ],
    )
    sub = _party(
        "s", "subscriber", [],
        [
            _field("id", "string", True),
            _field("n", "integer", True),
            _field("lvl", "enum", False, ["L1"]),
        ],
    )
    pair = check_pair(producer, "new", sub, "new")
    assert not pair.compatible
    assert pair.counterexample == {"id": "sample", "n": 1, "lvl": "L9"}


def test_invalid_enum_schema_rejected():
    with pytest.raises(ValueError):
        FieldDef(name="x", type="enum", required=True, enum_values=None)
    with pytest.raises(ValueError):
        FieldDef(name="x", type="enum", required=True, enum_values=["a", "a"])
    with pytest.raises(ValueError):
        FieldDef(name="x", type="integer", required=True, enum_values=["a"])
