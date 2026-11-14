"""FastAPI 契约排程服务入口。

启动：uvicorn app.main:app --host 0.0.0.0 --port 8000
文档：http://localhost:8000/docs （Swagger UI）
"""

from __future__ import annotations

import json
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import RedirectResponse

from .compat import all_pairs
from .models import PlanRequest, PlanResponse, PairCompatibility
from .planner import solve

_SAMPLES_DIR = Path(__file__).resolve().parent.parent / "samples"


def _load_sample(name: str) -> dict:
    with (_SAMPLES_DIR / name).open(encoding="utf-8") as fh:
        return json.load(fh)


_SAFE_EXAMPLE = _load_sample("safe_subscriber_first.json")
_INFEASIBLE_EXAMPLE = _load_sample("infeasible_circular.json")

app = FastAPI(
    title="裂纹事件流契约排程 API",
    version="1.0.0",
    description=(
        "提交采集端（发布端）与质检、日报（两个订阅端）新旧两版的扁平 JSON 字段定义，"
        "计算滚动升级期间每一步都安全的升级顺序；无解时给出受阻的版本组合、"
        "字段级违规及发布端可发出即被拒收的示例消息。\n\n"
        "兼容规则：字段限 integer/string/enum；订阅端忽略未知字段；"
        "发布端可选字段可能缺失；枚举可能取声明的任一值。"
    ),
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
