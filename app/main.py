"""FastAPI 服务入口：契约排程 API + 裂纹事件分发 API。

启动：uvicorn app.main:app --host 0.0.0.0 --port 8000
文档：http://localhost:8000/docs （Swagger UI）

分发链路：接收采集车补传的裂纹事件 → 先持久化 → 向 qc / daily_report
两个本地接收端逐端投递；失败端自动重试，服务重启后从库中恢复补送。
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from pathlib import Path
from typing import AsyncIterator

from fastapi import FastAPI
from fastapi.responses import RedirectResponse

from .compat import all_pairs
from .dispatch_api import build_dispatch_router
from .dispatcher import DispatchService
from .models import PairCompatibility, PlanRequest, PlanResponse
from .planner import solve

logger = logging.getLogger("crack.main")

_SAMPLES_DIR = Path(__file__).resolve().parent.parent / "samples"


def _load_sample(name: str) -> dict:
    with (_SAMPLES_DIR / name).open(encoding="utf-8") as fh:
        return json.load(fh)


_SAFE_EXAMPLE = _load_sample("safe_subscriber_first.json")
_INFEASIBLE_EXAMPLE = _load_sample("infeasible_circular.json")


async def _retry_loop(service: DispatchService, stop: asyncio.Event) -> None:
    """后台自动补送：周期性地把仍为 pending 的投递重试一次（含重启后的恢复）。"""
    interval = service.retry_interval_seconds or 5.0
    while not stop.is_set():
        try:
            attempted = await asyncio.to_thread(service.retry_pending)
            if attempted:
                logger.info("自动补送：本次尝试 %d 条未确认投递", attempted)
        except Exception:
            logger.exception("自动补送循环异常")
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval)
        except asyncio.TimeoutError:
            pass


def create_app(dispatch_service: DispatchService | None = None) -> FastAPI:
    """构建主服务；测试可注入自定义 DispatchService。"""
    service = dispatch_service or DispatchService.from_env()

    @contextlib.asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        stop = asyncio.Event()
        task: asyncio.Task[None] | None = None
        if service.retry_interval_seconds:
            task = asyncio.create_task(_retry_loop(service, stop))
        try:
            yield
        finally:
            stop.set()
            if task is not None:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
            service.close()

    app = FastAPI(
        title="轨检裂纹事件平台：契约排程 + 事件分发 API",
        version="1.1.0",
        description=(
            "**契约排程**：提交采集端（发布端）与质检、日报（两个订阅端）新旧两版的扁平 JSON "
            "字段定义，计算滚动升级期间每一步都安全的升级顺序；无解时给出受阻的版本组合、"
            "字段级违规及发布端可发出即被拒收的示例消息。\n\n"
            "**事件分发**：接收采集车补传的单条裂纹事件（全局事件号、车号、里程、缺陷等级），"
            "先持久化，再向 qc / daily_report 两个本地接收端逐端投递并记录 "
            "pending / confirmed；一端失败不妨碍另一端，失败端自动重试，重启后补送；"
            "同号同内容重提幂等去重，同号异内容拒绝。\n\n"
            "演示：接收端容器（8100/8200 端口）的 /docs 里用 /admin/behavior 切换 "
            "down（单端故障）或 ack_loss（回执丢失），再回到本服务 /docs 观察逐端状态与补送恢复。"
        ),
        lifespan=lifespan,
    )

    @app.get("/", include_in_schema=False)
    def root() -> RedirectResponse:
        return RedirectResponse(url="/docs")

    @app.get("/health", tags=["运维"], summary="健康检查")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post(
        "/api/v1/plan",
        response_model=PlanResponse,
        tags=["排程"],
        summary="计算三方滚动升级的安全顺序",
        openapi_extra={
            "requestBody": {
                "content": {
                    "application/json": {
                        "examples": {
                            "safe_subscriber_first": {
                                "summary": "先升级订阅端才能新增缺陷等级（有安全顺序）",
                                "value": _SAFE_EXAMPLE,
                            },
                            "infeasible_circular": {
                                "summary": "新增枚举值 + 订阅端要求新必填字段，循环依赖无解",
                                "value": _INFEASIBLE_EXAMPLE,
                            },
                        }
                    }
                }
            }
        },
    )
    def plan(request: PlanRequest) -> PlanResponse:
        return solve(request.parties)

    @app.post(
        "/api/v1/compatibility",
        response_model=list[PairCompatibility],
        tags=["排程"],
        summary="查看全部 8 个发布端版本 × 订阅端版本组合的兼容性明细",
    )
    def compatibility(request: PlanRequest) -> list[PairCompatibility]:
        return all_pairs(request.producer(), request.subscribers())

    app.include_router(build_dispatch_router(service))
    return app


app = create_app()
