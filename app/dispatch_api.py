"""分发 API 路由：事件接入、逐端状态查询、手动补送。"""

from typing import Annotated

from fastapi import APIRouter, HTTPException, Query, Response

from .dispatch_models import (
    CrackEvent,
    EndpointInfo,
    EventIngestResponse,
    EventRecord,
    RedeliverResponse,
)
from .dispatcher import (
    DispatchService,
    EventConflictError,
    UnknownEndpointError,
    UnknownEventError,
)


def build_dispatch_router(service: DispatchService) -> APIRouter:
    router = APIRouter(prefix="/api/v1", tags=["事件分发"])
    endpoint_names = service.endpoint_names

    @router.post(
        "/events",
        response_model=EventIngestResponse,
        status_code=201,
        summary="接收采集车补传的单条裂纹事件",
        description=(
            "先持久化事件，再向质检（qc）、日报（daily_report）两个本地接收端逐端投递；"
            "每个端独立记录 pending（待送）/ confirmed（已确认）。"
            "一端失败只影响该端状态（自动重试），不妨碍另一端接收。\n\n"
            "幂等规则：同号同内容重提只保留一条（返回 200，并对未确认端补送）；"
            "同号异内容拒绝（返回 409，已入库记录不受影响）。"
        ),
        responses={
            200: {"model": EventIngestResponse, "description": "同号同内容重复提交，幂等接受"},
            409: {"description": "同号异内容，拒绝入库"},
        },
    )
    def ingest_event(event: CrackEvent, response: Response) -> EventIngestResponse:
        try:
            record, created = service.ingest(event)
        except EventConflictError as exc:
            raise HTTPException(
                status_code=409,
                detail={
                    "error": "event_conflict",
                    "message": f"事件 {exc.event_id} 已入库，本次提交内容不一致，已拒绝",
                    "fields": exc.diffs,
                },
            ) from exc
        if created:
            message = "事件已持久化，并向两个接收端投递（逐端送达情况见 record.deliveries）"
        else:
            response.status_code = 200
            message = "同号同内容重复提交：仅保留一条，未确认端已触发补送"
        return EventIngestResponse(
            created=created, deduplicated=not created, message=message, record=record
        )

    @router.get(
        "/events",
        response_model=list[EventRecord],
        summary="列出全部事件及逐端送达状态",
    )
    def list_events() -> list[EventRecord]:
        return service.list()

    @router.get(
        "/events/{event_id}",
        response_model=EventRecord,
        summary="查询单条事件的逐端送达状态（数据工程师核对用）",
        responses={404: {"description": "事件不存在"}},
    )
    def get_event(event_id: str) -> EventRecord:
        record = service.get(event_id)
        if record is None:
            raise HTTPException(status_code=404, detail=f"事件 {event_id} 不存在")
        return record

    @router.post(
        "/events/{event_id}/redeliver",
        response_model=RedeliverResponse,
        summary="手动补送未确认的端（自动重试之外的恢复手段）",
        description=(
            "仅对仍为 pending 的端补送；已确认端跳过（不重复投递、确认不可撤销）。"
            "接收端按全局事件号去重，即使重复投递也只入库一次。"
        ),
        responses={
            404: {"description": "事件不存在"},
            422: {"description": "未知接收端"},
        },
    )
    def redeliver_event(
        event_id: str,
        endpoint: Annotated[
            str | None,
            Query(description=f"只补送指定端；缺省补送全部未确认端。可选：{'、'.join(endpoint_names)}"),
        ] = None,
    ) -> RedeliverResponse:
        try:
            record, attempted, skipped = service.redeliver(event_id, endpoint)
        except UnknownEventError as exc:
            raise HTTPException(status_code=404, detail=f"事件 {event_id} 不存在") from exc
        except UnknownEndpointError as exc:
            raise HTTPException(
                status_code=422, detail=f"未知接收端 {endpoint}，可选：{endpoint_names}"
            ) from exc
        if attempted:
            message = f"已向 {attempted} 补送"
        else:
            message = "所有端均已确认，无需补送"
        return RedeliverResponse(
            message=message,
            attempted_endpoints=attempted,
            skipped_confirmed=skipped,
            record=record,
        )

    @router.get(
        "/endpoints",
        response_model=list[EndpointInfo],
        summary="查看分发端配置的两个本地接收端",
    )
    def list_endpoints() -> list[EndpointInfo]:
        return [EndpointInfo(**info) for info in service.endpoint_info()]

    return router
