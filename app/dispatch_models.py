"""裂纹事件分发链路的 Pydantic 模型与纯函数。

事件模型在分发端与两个接收端之间共享（即两端的「线上契约」）：
- event_id 为全局事件号，是端到端去重键；
- 同号同内容重提：幂等接受，只保留一条；
- 同号异内容：拒绝（分发端 409，接收端同样 409）。
"""

from __future__ import annotations

import hashlib
import json
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

DeliveryState = Literal["pending", "confirmed"]
ReceiverMode = Literal["normal", "down", "ack_loss"]


class CrackEvent(BaseModel):
    """轨检采集车补传的单条裂纹事件。"""

    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "examples": [
                {
                    "event_id": "EVT-20261114-0001",
                    "car_id": "GJC-018",
                    "mileage_m": 128500,
                    "defect_level": "L2",
                }
            ]
        },
    )

    event_id: Annotated[str, Field(min_length=1, max_length=64, description="全局事件号（补传去重键）")]
    car_id: Annotated[str, Field(min_length=1, max_length=32, description="车号")]
    mileage_m: Annotated[int, Field(ge=0, description="里程（米）")]
    defect_level: Annotated[str, Field(min_length=1, max_length=16, description="缺陷等级，如 L1/L2/L3")]

    @field_validator("event_id", "car_id", "defect_level")
    @classmethod
    def _not_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("不能为空白字符串")
        return v


class EndpointDelivery(BaseModel):
    """单个接收端的投递状态。"""

    model_config = ConfigDict(extra="forbid")

    endpoint: Annotated[str, Field(description="接收端标识，如 qc / daily_report")]
    status: Annotated[DeliveryState, Field(description="pending=待送 / confirmed=已确认")]
    attempts: Annotated[int, Field(description="累计投递尝试次数")]
    last_attempt_at: Annotated[str | None, Field(description="最近一次投递尝试时间（UTC ISO8601）")] = None
    confirmed_at: Annotated[str | None, Field(description="确认时间；未确认为 null")] = None
    last_error: Annotated[str | None, Field(description="最近一次失败原因；已确认为 null")] = None


class EventRecord(BaseModel):
    """一条事件及其逐端送达情况（数据工程师核对用）。"""

    model_config = ConfigDict(extra="forbid")

    event: CrackEvent
    first_received_at: Annotated[str, Field(description="首次入库时间（UTC ISO8601）")]
    deliveries: Annotated[list[EndpointDelivery], Field(description="逐端投递状态，每个接收端一条")]


class EventIngestResponse(BaseModel):
    """事件接入应答。"""

    model_config = ConfigDict(extra="forbid")

    created: Annotated[bool, Field(description="是否首次入库")]
    deduplicated: Annotated[bool, Field(description="是否同号同内容重复提交（幂等去重，仅保留一条）")]
    message: str
    record: EventRecord


class RedeliverResponse(BaseModel):
    """手动补送应答。"""

    model_config = ConfigDict(extra="forbid")

    message: str
    attempted_endpoints: Annotated[list[str], Field(description="本次实际补送的接收端")]
    skipped_confirmed: Annotated[
        list[str],
        Field(description="已确认而跳过的接收端（确认不可撤销，也不重复投递）"),
    ]
    record: EventRecord


class EndpointInfo(BaseModel):
    """分发端配置的一个接收端。"""

    model_config = ConfigDict(extra="forbid")

    name: str
    url: str | None = None


# ---- 接收端模型 ----


class BehaviorIn(BaseModel):
    """故障注入：切换接收端行为模式。"""

    model_config = ConfigDict(extra="forbid")

    mode: Annotated[
        ReceiverMode,
        Field(
            description=(
                "normal=正常（保存成功才应答 200）；"
                "down=模拟中断（不保存，直接 503）；"
                "ack_loss=模拟回执丢失（保存成功后返回 500）"
            )
        ),
    ]


class BehaviorState(BaseModel):
    """接收端当前行为模式。"""

    model_config = ConfigDict(extra="forbid")

    receiver: str
    mode: ReceiverMode
    mode_meaning: str


class ReceiveAck(BaseModel):
    """接收端保存成功后的回执。"""

    model_config = ConfigDict(extra="forbid")

    receiver: str
    event_id: str
    result: Annotated[str, Field(description="stored=首次入库 / duplicate_ignored=重投去重，未重复计数")]
    stored_events: Annotated[int, Field(description="当前已保存的不同事件总数")]


class ReceiverStats(BaseModel):
    """接收端统计：用于核对「重投只入库一次、不重复计数」。"""

    model_config = ConfigDict(extra="forbid")

    receiver: str
    mode: ReceiverMode
    stored_events: Annotated[int, Field(description="已保存的不同事件总数（按全局事件号去重）")]
    receive_requests: Annotated[int, Field(description="累计收到的投递请求数（含重投）")]
    duplicates_suppressed: Annotated[int, Field(description="被去重的重投次数")]
    conflicts_rejected: Annotated[int, Field(description="同号异内容被拒绝的次数")]


# ---- 纯函数 ----


def content_hash(event: CrackEvent) -> str:
    """事件内容的稳定哈希，用于同号同内容判定。"""
    canonical = json.dumps(event.model_dump(), sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def diff_events(stored: CrackEvent, incoming: CrackEvent) -> dict[str, dict[str, object]]:
    """逐字段比较同号事件的内容差异（event_id 相同，不参与比较）。"""
    diffs: dict[str, dict[str, object]] = {}
    for field_name in ("car_id", "mileage_m", "defect_level"):
        old = getattr(stored, field_name)
        new = getattr(incoming, field_name)
        if old != new:
            diffs[field_name] = {"stored": old, "incoming": new}
    return diffs
