"""事件分发服务：先持久化、再逐端投递、失败重试、重启补送。

可靠性语义：
- 单端故障隔离：向某个接收端投递失败只把该端记为 pending，另一端照常确认；
- 接收端保存成功才应答 200，分发端只有拿到 200 才把该端标记为 confirmed；
- 回执丢失（接收端已保存但响应未到达）：该端保持 pending，重试时接收端
  按全局事件号去重，只入库一次、不重复计数；
- 重试与重启补送只处理 pending 行，已确认端永不再投、确认不可撤销。
"""

from __future__ import annotations

import logging
import os
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import httpx

from .dispatch_models import (
    CrackEvent,
    EndpointDelivery,
    EventRecord,
    content_hash,
    diff_events,
)
from .store import EventStore

logger = logging.getLogger("crack.dispatch")

DEFAULT_ENDPOINT_URLS = {
    "qc": "http://127.0.0.1:8100",
    "daily_report": "http://127.0.0.1:8200",
}


@dataclass(frozen=True)
class SendResult:
    ok: bool
    message: str


Sender = Callable[[dict], SendResult]


class HttpSender:
    """通过 HTTP 向某个本地接收端投递；仅 200 视为确认。"""

    def __init__(
        self,
        endpoint: str,
        base_url: str,
        timeout: float = 3.0,
        client: httpx.Client | None = None,
    ):
        self.endpoint = endpoint
        self.base_url = base_url
        self._timeout = timeout
        self._client = client

    def _post(self, payload: dict) -> httpx.Response:
        if self._client is None:
            self._client = httpx.Client(base_url=self.base_url, timeout=self._timeout)
        return self._client.post("/receive", json=payload)

    def __call__(self, payload: dict) -> SendResult:
        try:
            resp = self._post(payload)
        except httpx.TimeoutException:
            return SendResult(False, f"投递 {self.endpoint} 超时，将自动重试")
        except httpx.HTTPError as exc:
            return SendResult(False, f"投递 {self.endpoint} 连接失败（{exc.__class__.__name__}），将自动重试")
        if resp.status_code == 200:
            return SendResult(True, "接收端已保存并确认")
        return SendResult(False, f"接收端 {self.endpoint} 返回 {resp.status_code}：{resp.text[:150]}")

    def describe(self) -> dict[str, str]:
        return {"name": self.endpoint, "url": self.base_url}

    def close(self) -> None:
        if self._client is not None:
            self._client.close()


class EventConflictError(Exception):
    """同号异内容：事件已入库且内容不一致。"""

    def __init__(self, event_id: str, diffs: dict[str, dict[str, object]]):
        self.event_id = event_id
        self.diffs = diffs
        super().__init__(f"事件 {event_id} 已入库，本次提交内容不一致")


class UnknownEventError(KeyError):
    """事件不存在。"""


class UnknownEndpointError(ValueError):
    """接收端不存在。"""


class DispatchService:
    """裂纹事件分发服务（先持久化，再向两个本地接收端逐端投递）。"""

    def __init__(
        self,
        store: EventStore,
        senders: dict[str, Sender],
        retry_interval_seconds: float | None = 5.0,
    ):
        if not senders:
            raise ValueError("至少配置一个接收端")
        self._store = store
        self._senders = dict(senders)
        self.retry_interval_seconds = retry_interval_seconds
        self._send_lock = threading.Lock()

    @classmethod
    def from_env(cls) -> "DispatchService":
        data_dir = Path(os.environ.get("DATA_DIR", "./data/distributor"))
        timeout = float(os.environ.get("DELIVERY_TIMEOUT_SECONDS", "3"))
        interval = float(os.environ.get("RETRY_INTERVAL_SECONDS", "5"))
        urls = {
            "qc": os.environ.get("QC_RECEIVER_URL", DEFAULT_ENDPOINT_URLS["qc"]),
            "daily_report": os.environ.get("REPORT_RECEIVER_URL", DEFAULT_ENDPOINT_URLS["daily_report"]),
        }
        senders = {name: HttpSender(name, url, timeout=timeout) for name, url in urls.items()}
        return cls(
            EventStore(data_dir / "events.db"),
            senders,
            retry_interval_seconds=interval if interval > 0 else None,
        )

    # ---- 配置信息 ----

    @property
    def endpoint_names(self) -> list[str]:
        return list(self._senders)

    def endpoint_info(self) -> list[dict[str, str | None]]:
        info: list[dict[str, str | None]] = []
        for name, sender in self._senders.items():
            describe = getattr(sender, "describe", None)
            info.append(describe() if callable(describe) else {"name": name, "url": None})
        return info

    # ---- 接入 ----

    def ingest(self, event: CrackEvent) -> tuple[EventRecord, bool]:
        """先持久化，再向未确认的端投递。返回 (记录, 是否首次入库)。

        同号同内容重提：幂等接受（不重复建单），并对未确认端补送；
        同号异内容：抛 EventConflictError。
        """
        created = self._store.insert_event(event, self.endpoint_names)
        if not created:
            stored = self._store.get_event(event.event_id)
            assert stored is not None
            if stored.content_hash != content_hash(event):
                raise EventConflictError(event.event_id, diff_events(stored.event, event))
        record = self._deliver_pending(event.event_id)
        return record, created

    # ---- 补送 ----

    def redeliver(self, event_id: str, endpoint: str | None = None) -> tuple[EventRecord, list[str], list[str]]:
        """手动补送未确认的端；已确认端跳过（不重复投递、确认不可撤销）。"""
        stored = self._store.get_event(event_id)
        if stored is None:
            raise UnknownEventError(event_id)
        if endpoint is not None and endpoint not in self._senders:
            raise UnknownEndpointError(endpoint)
        attempted: list[str] = []
        skipped: list[str] = []
        for row in self._store.deliveries_for(event_id):
            if endpoint is not None and row.endpoint != endpoint:
                continue
            if row.status == "confirmed":
                skipped.append(row.endpoint)
                continue
            self._attempt(stored.event, row.endpoint)
            attempted.append(row.endpoint)
        return self._record(event_id), attempted, skipped

    def retry_pending(self) -> int:
        """自动重试/重启补送：对所有待送的 (事件, 端) 各投递一次。返回尝试次数。"""
        attempted = 0
        for event, endpoint in self._store.pending_deliveries():
            self._attempt(event, endpoint)
            attempted += 1
        return attempted

    # ---- 查询 ----

    def get(self, event_id: str) -> EventRecord | None:
        if self._store.get_event(event_id) is None:
            return None
        return self._record(event_id)

    def list(self) -> list[EventRecord]:
        return [self._record(e.event.event_id) for e in self._store.list_events()]

    def close(self) -> None:
        for sender in self._senders.values():
            close = getattr(sender, "close", None)
            if callable(close):
                close()

    # ---- 内部 ----

    def _deliver_pending(self, event_id: str) -> EventRecord:
        stored = self._store.get_event(event_id)
        assert stored is not None
        for row in self._store.deliveries_for(event_id):
            if row.status == "confirmed":
                continue  # 已确认端不再投递
            self._attempt(stored.event, row.endpoint)
        return self._record(event_id)

    def _attempt(self, event: CrackEvent, endpoint: str) -> SendResult:
        sender = self._senders[endpoint]
        with self._send_lock:
            try:
                result = sender(event.model_dump())
            except Exception as exc:  # 发送器自身异常也记为待送，不影响其他端
                logger.exception("投递 %s 时发送器异常", endpoint)
                result = SendResult(False, f"投递 {endpoint} 异常（{exc.__class__.__name__}），将自动重试")
            self._store.record_delivery_result(event.event_id, endpoint, result.ok, result.message)
        if not result.ok:
            logger.warning("事件 %s 投递 %s 失败：%s", event.event_id, endpoint, result.message)
        return result

    def _record(self, event_id: str) -> EventRecord:
        stored = self._store.get_event(event_id)
        assert stored is not None
        deliveries = [
            EndpointDelivery(
                endpoint=row.endpoint,
                status=row.status,  # type: ignore[arg-type]
                attempts=row.attempts,
                last_attempt_at=row.last_attempt_at,
                confirmed_at=row.confirmed_at,
                last_error=row.last_error,
            )
            for row in self._store.deliveries_for(event_id)
        ]
        return EventRecord(
            event=stored.event,
            first_received_at=stored.first_received_at,
            deliveries=deliveries,
        )
