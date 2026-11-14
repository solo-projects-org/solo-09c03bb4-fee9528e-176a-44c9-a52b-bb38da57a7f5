"""本地接收端（质检 qc / 日报 daily_report）演示服务。

同一镜像以不同环境变量启动两个实例：

    RECEIVER_NAME=qc           DATA_DIR=/data/qc     uvicorn app.receiver:app --port 8100
    RECEIVER_NAME=daily_report DATA_DIR=/data/report uvicorn app.receiver:app --port 8200

行为约定：
- 保存成功才应答 200（先落库、后回执）；
- 重投只入库一次：同号同内容返回 duplicate_ignored，不重复计数；
- 同号异内容返回 409；
- 故障注入（供 Swagger 演示）：down=不保存直接 503；ack_loss=保存后返回 500，
  模拟「已写入但回执丢失」。
"""

from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from fastapi import FastAPI, HTTPException
from fastapi.responses import RedirectResponse

from .dispatch_models import (
    BehaviorIn,
    BehaviorState,
    CrackEvent,
    ReceiveAck,
    ReceiverStats,
    content_hash,
    diff_events,
)
from .store import utcnow

_SCHEMA = """
CREATE TABLE IF NOT EXISTS received_events (
    event_id     TEXT PRIMARY KEY,
    car_id       TEXT NOT NULL,
    mileage_m    INTEGER NOT NULL,
    defect_level TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    stored_at    TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value INTEGER NOT NULL
);
"""

_COUNTERS = ("receive_requests", "duplicates_suppressed", "conflicts_rejected")

_MODE_MEANING = {
    "normal": "正常：保存成功才应答 200",
    "down": "模拟中断：不保存，直接 503",
    "ack_loss": "模拟回执丢失：保存成功后返回 500（分发端视为失败会重投，重投去重不重复计数）",
}


class ReceiverConflictError(Exception):
    """同号异内容。"""

    def __init__(self, event_id: str, diffs: dict[str, dict[str, object]]):
        self.event_id = event_id
        self.diffs = diffs
        super().__init__(f"事件 {event_id} 已保存且内容不一致")


class ReceiverStore:
    """接收端本地库：保存成功才应答；重投按全局事件号去重，只入库一次。"""

    def __init__(self, db_path: str | Path):
        self._path = str(db_path)
        Path(self._path).parent.mkdir(parents=True, exist_ok=True)
        with self._tx() as conn:
            conn.executescript(_SCHEMA)
            conn.execute("PRAGMA journal_mode=WAL")
            for key in _COUNTERS:
                conn.execute("INSERT OR IGNORE INTO meta (key, value) VALUES (?, 0)", (key,))

    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self._path, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA synchronous=FULL")
        try:
            yield conn
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()

    def save(self, event: CrackEvent) -> str:
        """落库并返回 stored / duplicate_ignored；同号异内容抛 ReceiverConflictError。"""
        outcome, diffs = self._save(event)
        if outcome == "conflict":
            raise ReceiverConflictError(event.event_id, diffs or {})
        return outcome

    def _save(self, event: CrackEvent) -> tuple[str, dict[str, dict[str, object]] | None]:
        # 注意：不在事务内抛异常，否则统计计数会随回滚丢失。
        with self._tx() as conn:
            self._bump(conn, "receive_requests")
            cur = conn.execute(
                "INSERT OR IGNORE INTO received_events"
                " (event_id, car_id, mileage_m, defect_level, content_hash, stored_at)"
                " VALUES (?, ?, ?, ?, ?, ?)",
                (
                    event.event_id,
                    event.car_id,
                    event.mileage_m,
                    event.defect_level,
                    content_hash(event),
                    utcnow(),
                ),
            )
            if cur.rowcount == 1:
                return "stored", None
            row = conn.execute(
                "SELECT * FROM received_events WHERE event_id = ?", (event.event_id,)
            ).fetchone()
            if row["content_hash"] == content_hash(event):
                self._bump(conn, "duplicates_suppressed")
                return "duplicate_ignored", None
            self._bump(conn, "conflicts_rejected")
            stored = CrackEvent(
                event_id=row["event_id"],
                car_id=row["car_id"],
                mileage_m=row["mileage_m"],
                defect_level=row["defect_level"],
            )
            return "conflict", diff_events(stored, event)

    def list_events(self) -> list[dict[str, object]]:
        with self._tx() as conn:
            rows = conn.execute(
                "SELECT event_id, car_id, mileage_m, defect_level, stored_at"
                " FROM received_events ORDER BY rowid"
            ).fetchall()
        return [dict(r) for r in rows]

    def count(self) -> int:
        with self._tx() as conn:
            row = conn.execute("SELECT COUNT(*) AS n FROM received_events").fetchone()
        return int(row["n"])

    def stats(self) -> dict[str, int]:
        with self._tx() as conn:
            rows = conn.execute("SELECT key, value FROM meta").fetchall()
        counters = {r["key"]: int(r["value"]) for r in rows}
        counters["stored_events"] = self.count()
        return counters

    @staticmethod
    def _bump(conn: sqlite3.Connection, key: str) -> None:
        conn.execute("UPDATE meta SET value = value + 1 WHERE key = ?", (key,))


def create_receiver_app(name: str, data_dir: str | Path) -> FastAPI:
    """构建一个本地接收端实例（qc 或 daily_report）。"""
    store = ReceiverStore(Path(data_dir) / "received.db")
    behavior = {"mode": "normal"}

    app = FastAPI(
        title=f"裂纹事件本地接收端 · {name}",
        version="1.1.0",
        description=(
            f"轨检裂纹事件的本地接收端（{name}）。保存成功才应答；重投按全局事件号去重，"
            "只入库一次。\n\n故障注入（/admin/behavior）可演示：单端中断（down）、"
            "已写入但回执丢失（ack_loss）。"
        ),
    )

    @app.get("/", include_in_schema=False)
    def root() -> RedirectResponse:
        return RedirectResponse(url="/docs")

    @app.get("/health", tags=["运维"], summary="健康检查")
    def health() -> dict[str, str]:
        return {"status": "ok", "receiver": name}

    @app.post(
        "/receive",
        response_model=ReceiveAck,
        tags=["接收"],
        summary="接收一条裂纹事件（保存成功才应答）",
        responses={
            409: {"description": "同号异内容，拒绝"},
            500: {"description": "ack_loss 模式：已保存但回执丢失（模拟）"},
            503: {"description": "down 模式：接收端中断（模拟），事件未保存"},
        },
    )
    def receive(event: CrackEvent) -> ReceiveAck:
        mode = behavior["mode"]
        if mode == "down":
            raise HTTPException(status_code=503, detail=f"接收端 {name} 模拟中断：事件未保存")
        try:
            result = store.save(event)
        except ReceiverConflictError as exc:
            raise HTTPException(
                status_code=409,
                detail={
                    "error": "event_conflict",
                    "message": f"事件 {exc.event_id} 已保存，本次投递内容不一致，已拒绝",
                    "fields": exc.diffs,
                },
            ) from exc
        if mode == "ack_loss":
            # 已落库，但模拟回执丢失：分发端视为失败，稍后会重投；重投按 event_id 去重。
            raise HTTPException(
                status_code=500,
                detail=f"接收端 {name} 已保存事件 {event.event_id}，但回执丢失（模拟）",
            )
        return ReceiveAck(
            receiver=name, event_id=event.event_id, result=result, stored_events=store.count()
        )

    @app.get("/events", tags=["接收"], summary="查看已保存的事件（重投只入库一次）")
    def list_events() -> dict[str, object]:
        return {"receiver": name, "stored_events": store.count(), "events": store.list_events()}

    @app.get("/stats", response_model=ReceiverStats, tags=["接收"], summary="接收端统计（核对不重复计数）")
    def stats() -> ReceiverStats:
        return ReceiverStats(receiver=name, mode=behavior["mode"], **store.stats())

    @app.post(
        "/admin/behavior",
        response_model=BehaviorState,
        tags=["故障注入"],
        summary="切换接收端行为模式（演示单端故障 / 回执丢失）",
    )
    def set_behavior(body: BehaviorIn) -> BehaviorState:
        behavior["mode"] = body.mode
        return BehaviorState(receiver=name, mode=body.mode, mode_meaning=_MODE_MEANING[body.mode])

    @app.get(
        "/admin/behavior",
        response_model=BehaviorState,
        tags=["故障注入"],
        summary="查看当前行为模式",
    )
    def get_behavior() -> BehaviorState:
        return BehaviorState(
            receiver=name, mode=behavior["mode"], mode_meaning=_MODE_MEANING[behavior["mode"]]
        )

    return app


# uvicorn app.receiver:app 入口；实例身份由环境变量决定。
app = create_receiver_app(
    name=os.environ.get("RECEIVER_NAME", "qc"),
    data_dir=os.environ.get("DATA_DIR", "./data/receiver"),
)
