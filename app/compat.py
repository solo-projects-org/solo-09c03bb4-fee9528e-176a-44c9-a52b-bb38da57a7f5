"""发布端消息空间与订阅端接收条件之间的兼容性判定。

语义约定（对应需求）：
- 契约是扁平 JSON，字段类型仅 integer / string / enum(字符串枚举)。
- 发布端 required=true 的字段一定会发出；required=false 的可选字段可能整体缺失，
  也可能携带一个声明类型/枚举内的值出现。
- 发布端 enum 字段实际可能取声明值中的任意一个。
- 订阅端忽略未知字段：发布端多发的字段不构成兼容性问题。
- 订阅端 required=true 的字段若在发布端消息里可能缺失，则该组合不安全
  （required_missing）。
- 同名字段类型不一致即 type_mismatch；订阅端 enum 只接受自己声明的枚举值，
  发布端能发出的某个值不在其中即 enum_violation。
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from .models import FieldDef, PairCompatibility, PartySpec, Violation

if TYPE_CHECKING:
    from .models import FieldType

# 反例消息里各类型使用的占位值（枚举优先使用真实枚举值）。
_INT_PLACEHOLDER = 1
_STR_PLACEHOLDER = "sample"


def _index(schema: list[FieldDef]) -> dict[str, FieldDef]:
    return {f.name: f for f in schema}


def example_scalar(field: FieldDef) -> int | str:
    """构造该发布端字段可发出的一个标量值。"""
    if field.type == "integer":
        return _INT_PLACEHOLDER
    if field.type == "enum":
        # 调用方保证 enum_values 非空
        return field.enum_values[0]  # type: ignore[index]
    return _STR_PLACEHOLDER


def _violation_for_mismatch(
    name: str, pub: FieldDef, sub: FieldDef
) -> Violation | None:
    """同名字段在发布端与订阅端之间的类型/枚举域冲突（缺失另算）。"""
    # 整数 <-> 字符串/枚举，或 字符串/枚举 <-> 整数，直接类型不符
    if pub.type == "integer" or sub.type == "integer":
        if pub.type != sub.type:
            return Violation(
                field=name,
                kind="type_mismatch",
                detail=(
                    f"字段 {name} 发布端声明为 {pub.type}，"
                    f"订阅端声明为 {sub.type}，类型不一致"
                ),
                example_value=example_scalar(pub),
            )
        return None

    # 两端均为字符串族（string / enum），字符串载荷本身可互相承载，
    # 只有订阅端用 enum 收窄取值域时才可能越界。
    if sub.type == "enum":
        if pub.type == "enum":
            offending = [v for v in (pub.enum_values or []) if v not in (sub.enum_values or [])]
            if offending:
                return Violation(
                    field=name,
                    kind="enum_violation",
                    detail=(
                        f"字段 {name} 发布端枚举值 {offending} "
                        f"不在订阅端枚举 {sub.enum_values} 内"
                    ),
                    example_value=offending[0],
                )
        else:
            # 发布端是无界 string，可发出任意字符串，必然能绕过订阅端枚举域
            outside = next(
                (v for v in (pub.enum_values or []) if v not in (sub.enum_values or [])),
                None,
            )
            sample = outside or f"{_STR_PLACEHOLDER}_{name}"
            return Violation(
                field=name,
                kind="enum_violation",
                detail=(
                    f"字段 {name} 发布端为无界 string，可发出订阅端枚举 "
                    f"{sub.enum_values} 之外的任意字符串"
                ),
                example_value=sample,
            )
    return None


def check_pair(
    producer: PartySpec,
    producer_version: str,
    subscriber: PartySpec,
    subscriber_version: str,
) -> PairCompatibility:
    """判定一个「发布端版本 × 订阅端版本」组合是否安全。

    安全 = 发布端该版本可能发出的每一条消息，都满足订阅端该版本的接收条件。
    """
    pub_schema = producer.new_schema if producer_version == "new" else producer.old_schema
    sub_schema = subscriber.new_schema if subscriber_version == "new" else subscriber.old_schema
    pub_idx = _index(pub_schema)
    sub_idx = _index(sub_schema)

    violations: list[Violation] = []

    # 1) 订阅端必填字段：发布端是否保证该字段一定出现？
    #    发布端未声明 -> 永远缺失；声明为可选 -> 可能缺失。两者都不安全。
    for sub_field in sub_schema:
        if not sub_field.required:
            continue
        pub_field = pub_idx.get(sub_field.name)
        if pub_field is None:
            violations.append(
                Violation(
                    field=sub_field.name,
                    kind="required_missing",
                    detail=(
                        f"订阅端 {subscriber.id}:{subscriber_version} 必填字段 "
                        f"{sub_field.name} 在发布端 {producer.id}:{producer_version} "
                        f"契约中不存在，消息必然缺失该字段"
                    ),
                    example_value=None,
                )
            )
        elif not pub_field.required:
            violations.append(
                Violation(
                    field=sub_field.name,
                    kind="required_missing",
                    detail=(
                        f"订阅端 {subscriber.id}:{subscriber_version} 必填字段 "
                        f"{sub_field.name} 在发布端 {producer.id}:{producer_version} "
                        f"为可选字段，消息可能缺失该字段"
                    ),
                    example_value=None,
                )
            )

    # 2) 同名字段类型 / 枚举域冲突（可选字段也检查：一旦出现就必须能被接收）
    for name, pub_field in pub_idx.items():
        sub_field = sub_idx.get(name)
        if sub_field is None:
            continue  # 订阅端忽略未知字段
        v = _violation_for_mismatch(name, pub_field, sub_field)
        if v is not None:
            violations.append(v)

    compatible = not violations
    counterexample = None
    if not compatible:
        counterexample = _build_counterexample(pub_schema, violations)

    return PairCompatibility(
        producer_id=producer.id,
        producer_version=producer_version,  # type: ignore[arg-type]
        subscriber_id=subscriber.id,
        subscriber_version=subscriber_version,  # type: ignore[arg-type]
        compatible=compatible,
        violations=violations,
        counterexample=counterexample,
    )


def _build_counterexample(
    pub_schema: list[FieldDef], violations: list[Violation]
) -> dict[str, object]:
    """构造一条发布端确实可发出、但订阅端会拒收的示例消息。

    - 必填字段全部带上（保证消息本身合法）；
    - required_missing 的字段刻意不放入消息；
    - 类型/枚举冲突字段采用违规证据里的 example_value。
    """
    missing_fields = {v.field for v in violations if v.kind == "required_missing"}
    value_overrides = {
        v.field: v.example_value
        for v in violations
        if v.kind in ("type_mismatch", "enum_violation") and v.example_value is not None
    }

    msg: dict[str, object] = {}
    for field in pub_schema:
        if field.name in missing_fields:
            continue  # 可选字段：演示缺失
        if field.name in value_overrides:
            msg[field.name] = value_overrides[field.name]
        elif field.required:
            msg[field.name] = example_scalar(field)
        # 其余可选字段保持缺省（不出现），符合「可选字段可能缺失」
    return msg


def all_pairs(producer: PartySpec, subscribers: list[PartySpec]) -> list[PairCompatibility]:
    """计算发布端新旧两版 × 每个订阅端新旧两版的全部 8 个组合。"""
    out: list[PairCompatibility] = []
    for pv in ("old", "new"):
        for sub in subscribers:
            for sv in ("old", "new"):
                out.append(check_pair(producer, pv, sub, sv))
    return out
