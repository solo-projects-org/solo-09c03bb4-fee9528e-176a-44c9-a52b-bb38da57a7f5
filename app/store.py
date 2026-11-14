"""分发端 SQLite 持久化：事件表 + 逐端投递状态表。

可靠性设计：
- 先持久化再投递：事件与两个接收端的 pending 行在同一事务落库后才外发；
- 每次状态变更立即提交（synchronous=FULL），进程崩溃、容器重启后
  pending 行仍在，启动后的补送不会漏送；
- 状态更新全部带 ``status='pending'`` 前置条件：已确认的行永不被改写，
  重试、重启补送都不可能撤销另一端的确认。
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

from .dispatch_models import CrackEvent, content_hash

_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    event_id          TEXT PRIMARY KEY,
    car_id            TEXT NOT NULL,
    mileage_m         INTEGER NOT NULL,
    defect_level      TEXT NOT NULL,
    content_hash      TEXT NOT NULL,
    first_received_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS deliveries (
    event_id        TEXT NOT NULL REFERENCES events(event_id),
    endpoint        TEXT NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('pending', 'confirmed')),
    attempts        INTEGER NOT NULL DEFAULT 0,
    last_attempt_at TEXT,
    confirmed_at    TEXT,
    last_error      TEXT,
    PRIMARY KEY (event_id, endpoint)
);
"""


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


@dataclass(frozen=True)
class StoredEvent:
    event: CrackEvent
    content_hash: str
    first_received_at: str


@dataclass(frozen=True)
class DeliveryRow:
    event_id: str
    endpoint: str
    status: str
    attempts: int
    last_attempt_at: str | None
    confirmed_at: str | None
    last_error: str | None


class EventStore:
    """分发端事件库（SQLite，WAL + FULL 同步）。"""

    def __init__(self, db_path: str | Path):
        self._path = str(db_path)
        Path(self._path).parent.mkdir(parents=True, exist_ok=True)
        with self._tx() as conn:
            conn.executescript(_SCHEMA)
            conn.execute("PRAGMA journal_mode=WAL")

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

    # ---- 事件 ----

    def insert_event(self, event: CrackEvent, endpoints: list[str]) -> bool:
        """事件 + 各端 pending 行同一事务落库；已存在返回 False（不重复建单）。"""
        with self._tx() as conn:
            cur = conn.execute(
                "INSERT OR IGNORE INTO events"
                " (event_id, car_id, mileage_m, defect_level, content_hash, first_received_at)"
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
            if cur.rowcount == 0:
                return False
            conn.executemany(
                "INSERT INTO deliveries (event_id, endpoint, status, attempts)"
                " VALUES (?, ?, 'pending', 0)",
                [(event.event_id, ep) for ep in endpoints],
            )
            return True

    def get_event(self, event_id: str) -> StoredEvent | None:
        with self._tx() as conn:
            row = conn.execute("SELECT * FROM events WHERE event_id = ?", (event_id,)).fetchone()
        return None if row is None else self._to_stored(row)

    def list_events(self) -> list[StoredEvent]:
        with self._tx() as conn:
            rows = conn.execute("SELECT * FROM events ORDER BY rowid").fetchall()
        return [self._to_stored(r) for r in rows]

    # ---- 逐端投递状态 ----

    def deliveries_for(self, event_id: str) -> list[DeliveryRow]:
        with self._tx() as conn:
            rows = conn.execute(
                "SELECT * FROM deliveries WHERE event_id = ? ORDER BY endpoint", (event_id,)
            ).fetchall()
        return [self._to_delivery(r) for r in rows]

    def pending_deliveries(self) -> list[tuple[CrackEvent, str]]:
        """全部待送的 (事件, 接收端)，供自动重试与重启补送使用。"""
        with self._tx() as conn:
            rows = conn.execute(
                "SELECT e.*, d.endpoint FROM deliveries d"
                " JOIN events e ON e.event_id = d.event_id"
                " WHERE d.status = 'pending' ORDER BY d.rowid"
            ).fetchall()
        return [(self._to_stored(r).event, r["endpoint"]) for r in rows]

    def record_delivery_result(self, event_id: str, endpoint: str, ok: bool, message: str) -> None:
        """记录一次投递结果。

        两条 UPDATE 都带 ``status='pending'`` 前置条件：已确认的行永不被改写，
        确认不会被撤销，也不会被重复计数。
        """
        now = utcnow()
        with self._tx() as conn:
            if ok:
                conn.execute(
                    "UPDATE deliveries SET status='confirmed', attempts=attempts+1,"
                    " last_attempt_at=?, confirmed_at=?, last_error=NULL"
                    " WHERE event_id=? AND endpoint=? AND status='pending'",
                    (now, now, event_id, endpoint),
                )
            else:
                conn.execute(
                    "UPDATE deliveries SET attempts=attempts+1, last_attempt_at=?, last_error=?"
                    " WHERE event_id=? AND endpoint=? AND status='pending'",
                    (now, message, event_id, endpoint),
                )

    # ---- 行转换 ----

    @staticmethod
    def _to_stored(row: sqlite3.Row) -> StoredEvent:
        return StoredEvent(
            event=CrackEvent(
                event_id=row["event_id"],
                car_id=row["car_id"],
                mileage_m=row["mileage_m"],
                defect_level=row["defect_level"],
            ),
            content_hash=row["content_hash"],
            first_received_at=row["first_received_at"],
        )

    @staticmethod
    def _to_delivery(row: sqlite3.Row) -> DeliveryRow:
        return DeliveryRow(
            event_id=row["event_id"],
            endpoint=row["endpoint"],
            status=row["status"],
            attempts=row["attempts"],
            last_attempt_at=row["last_attempt_at"],
            confirmed_at=row["confirmed_at"],
            last_error=row["last_error"],
        )
