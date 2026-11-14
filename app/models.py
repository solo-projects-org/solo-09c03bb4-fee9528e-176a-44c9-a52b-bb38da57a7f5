"""扁平 JSON 契约的 Pydantic 模型与入参校验。"""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

FieldType = Literal["integer", "string", "enum"]
PartyRole = Literal["producer", "subscriber"]
Phase = Literal["old", "mixed", "new"]


class FieldDef(BaseModel):
    """单个扁平字段定义。

    - type=enum 时必须给出非空 enum_values；
    - enum_values 必须唯一；
    - 字段只允许整数、字符串、字符串枚举三类。
    """

    model_config = ConfigDict(extra="forbid")

    name: Annotated[str, Field(min_length=1, description="扁平 JSON 字段名")]
    type: Annotated[FieldType, Field(description="integer | string | enum")]
    required: Annotated[bool, Field(description="是否必填（发布端是否保证发出）")]
    enum_values: Annotated[
        list[str] | None,
        Field(default=None, description="type=enum 时声明的全部合法字符串值"),
    ] = None

    @field_validator("name")
    @classmethod
    def _name_not_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("字段名不能为空字符串")
        return v

    @field_validator("enum_values")
    @classmethod
    def _enum_values_rule(cls, v: list[str] | None, info) -> list[str] | None:
        ftype = info.data.get("type")
        if ftype == "enum":
            if not v:
                raise ValueError("type=enum 时 enum_values 必须为非空字符串数组")
            if any(not isinstance(x, str) or not x for x in v):
                raise ValueError("enum_values 必须全部为非空字符串")
            if len(set(v)) != len(v):
                raise ValueError("enum_values 不能有重复值")
            return list(v)
        if v is not None:
            raise ValueError("只有 type=enum 才允许声明 enum_values")
        return v


class PartySpec(BaseModel):
    """一方（发布端或某个订阅端）的新旧两版扁平字段契约。"""

    model_config = ConfigDict(extra="forbid")

    id: Annotated[str, Field(min_length=1, description="参与方标识，如 producer/qc/report")]
    role: Annotated[PartyRole, Field(description="producer=发布端，subscriber=订阅端")]
    old_schema: Annotated[list[FieldDef], Field(description="旧版字段定义")]
    new_schema: Annotated[list[FieldDef], Field(description="新版字段定义")]

    @model_validator(mode="after")
    def _unique_field_names(self) -> "PartySpec":
        for label, schema in (("old_schema", self.old_schema), ("new_schema", self.new_schema)):
            names = [f.name for f in schema]
            if len(set(names)) != len(names):
                raise ValueError(f"{self.id}.{label} 内字段名重复: {sorted(n for n in names if names.count(n) > 1)}")
        return self


class PlanRequest(BaseModel):
    """排程请求：恰好一个发布端 + 两个订阅端，各带新旧两版定义。"""

    model_config = ConfigDict(extra="forbid")

    parties: Annotated[
        list[PartySpec],
        Field(min_length=3, max_length=3, description="三方：1 个 producer + 2 个 subscriber"),
    ]

    @model_validator(mode="after")
    def _role_shape(self) -> "PlanRequest":
        roles = [p.role for p in self.parties]
        if roles.count("producer") != 1 or roles.count("subscriber") != 2:
            raise ValueError("parties 必须恰好包含 1 个 producer 和 2 个 subscriber")
        ids = [p.id for p in self.parties]
        if len(set(ids)) != 3:
            raise ValueError("三方 id 必须互不相同")
        return self

    def producer(self) -> PartySpec:
        return next(p for p in self.parties if p.role == "producer")

    def subscribers(self) -> list[PartySpec]:
        return [p for p in self.parties if p.role == "subscriber"]


class Violation(BaseModel):
    """一条不兼容证据。"""

    model_config = ConfigDict(extra="forbid")

    field: Annotated[str, Field(description="触发问题的字段名")]
    kind: Annotated[
        Literal["required_missing", "type_mismatch", "enum_violation"],
        Field(description="required_missing=必填缺失 / type_mismatch=类型不符 / enum_violation=枚举越界"),
    ]
    detail: Annotated[str, Field(description="人类可读的问题说明")]
    example_value: Annotated[
        object | None,
        Field(description="可触发该问题的字段值；缺失场景为 None"),
    ] = None


class PairCompatibility(BaseModel):
    """单个「发布端版本 × 订阅端版本」组合的兼容性结论。"""

    model_config = ConfigDict(extra="forbid")

    producer_id: str
    producer_version: Literal["old", "new"]
    subscriber_id: str
    subscriber_version: Literal["old", "new"]
    compatible: bool
    violations: list[Violation]
    counterexample: Annotated[
        dict[str, object] | None,
        Field(default=None, description="不兼容时发布端可发出、订阅端会拒收的示例消息"),
    ] = None


class BlockedTransition(BaseModel):
    """一次被安全条件挡住的单步升级。"""

    model_config = ConfigDict(extra="forbid")

    party_id: str
    from_phase: Phase
    to_phase: Phase
    state_before: dict[str, Phase]
    state_after: dict[str, Phase]
    blocking_pairs: list[str]
    reason: str


class PlanStep(BaseModel):
    """安全升级顺序中的一步。"""

    model_config = ConfigDict(extra="forbid")

    order: int
    party_id: str
    role: PartyRole
    from_phase: Phase
    to_phase: Phase
    state_after: dict[str, Phase]
    note: str


class BlockedPairReport(BaseModel):
    """无解时的受阻版本组合汇总。"""

    model_config = ConfigDict(extra="forbid")

    pair: str
    producer_id: str
    producer_version: Literal["old", "new"]
    subscriber_id: str
    subscriber_version: Literal["old", "new"]
    violations: list[Violation]
    counterexample: dict[str, object]
    blocks_transitions: list[BlockedTransition]


class PlanResponse(BaseModel):
    """排程结论。"""

    model_config = ConfigDict(extra="forbid")

    feasible: bool
    summary: str
    initial_state_safe: bool
    steps: list[PlanStep] = Field(default_factory=list)
    blocked_pairs: list[BlockedPairReport] = Field(default_factory=list)
