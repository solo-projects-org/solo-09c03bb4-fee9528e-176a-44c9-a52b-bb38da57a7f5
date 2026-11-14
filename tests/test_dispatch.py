"""分发链路：先持久化再投递、单端故障隔离、幂等重提、回执丢失恢复、重启补送。"""

import asyncio
import json
import time
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from app.dispatcher import DispatchService, HttpSender
from app.main import create_app
from app.receiver import create_receiver_app
from app.store import EventStore

SAMPLES = Path(__file__).resolve().parent.parent / "samples"

EVENT = {
    "event_id": "EVT-20261114-0001",
    "car_id": "GJC-018",
    "mileage_m": 128500,
    "defect_level": "L2",
}


class _AsgiHttpSender(HttpSender):
    """把同步投递调用桥接到进程内接收端 ASGI 应用（真实 HTTP 语义，不起端口）。"""

    def __init__(self, name: str, app):
        super().__init__(name, f"http://{name}")
        self._app = app

    def _post(self, payload: dict) -> httpx.Response:
        async def _send() -> httpx.Response:
            transport = httpx.ASGITransport(app=self._app)
            async with httpx.AsyncClient(
                transport=transport, base_url=self.base_url
            ) as client:
                return await client.post("/receive", json=payload)

        return asyncio.run(_send())


def _asgi_sender(name: str, app) -> HttpSender:
    return _AsgiHttpSender(name, app)


def _build_service(db_dir: Path, qc_app, report_app, retry_interval=None) -> DispatchService:
    return DispatchService(
        EventStore(db_dir / "events.db"),
        {
            "qc": _asgi_sender("qc", qc_app),
            "daily_report": _asgi_sender("daily_report", report_app),
        },
        retry_interval_seconds=retry_interval,
    )


class Rig:
    """一套分发链路试验台：主服务客户端 + 两个接收端客户端 + 数据目录。"""

    def __init__(self, client, qc, report, service, db_dir, qc_app, report_app):
        self.client = client
        self.qc = qc
        self.report = report
        self.service = service
        self.db_dir = db_dir
        self.qc_app = qc_app
        self.report_app = report_app

    def states(self, event_id=EVENT["event_id"]):
        body = self.client.get(f"/api/v1/events/{event_id}").json()
        return {d["endpoint"]: d for d in body["deliveries"]}


@pytest.fixture()
def rig(tmp_path):
    qc_app = create_receiver_app("qc", tmp_path / "qc")
    report_app = create_receiver_app("daily_report", tmp_path / "report")
    db_dir = tmp_path / "distributor"
    service = _build_service(db_dir, qc_app, report_app)
    with TestClient(create_app(service)) as client:
        yield Rig(
            client=client,
            qc=TestClient(qc_app),
            report=TestClient(report_app),
            service=service,
            db_dir=db_dir,
            qc_app=qc_app,
            report_app=report_app,
        )


def test_ingest_persists_then_delivers_to_both(rig):
    resp = rig.client.post("/api/v1/events", json=EVENT)
    assert resp.status_code == 201
    body = resp.json()
    assert body["created"] is True
    assert body["deduplicated"] is False
    states = {d["endpoint"]: d["status"] for d in body["record"]["deliveries"]}
    assert states == {"qc": "confirmed", "daily_report": "confirmed"}
    assert rig.qc.get("/stats").json()["stored_events"] == 1
    assert rig.report.get("/stats").json()["stored_events"] == 1


def test_single_endpoint_failure_does_not_block_other(rig):
    rig.qc.post("/admin/behavior", json={"mode": "down"})
    resp = rig.client.post("/api/v1/events", json=EVENT)
    assert resp.status_code == 201  # 一端失败不妨碍事件接入与另一端接收
    deliveries = {d["endpoint"]: d for d in resp.json()["record"]["deliveries"]}
    assert deliveries["qc"]["status"] == "pending"
    assert deliveries["qc"]["last_error"]
    assert deliveries["daily_report"]["status"] == "confirmed"
    assert rig.qc.get("/stats").json()["stored_events"] == 0

    # 质检端恢复后手动补送
    rig.qc.post("/admin/behavior", json={"mode": "normal"})
    redo = rig.client.post(f"/api/v1/events/{EVENT['event_id']}/redeliver")
    assert redo.status_code == 200
    body = redo.json()
    assert body["attempted_endpoints"] == ["qc"]
    assert body["skipped_confirmed"] == ["daily_report"]  # 已确认端不重复投递
    states = {d["endpoint"]: d["status"] for d in body["record"]["deliveries"]}
    assert states == {"qc": "confirmed", "daily_report": "confirmed"}
    assert rig.qc.get("/stats").json()["stored_events"] == 1


def test_duplicate_resubmit_keeps_single_record(rig):
    rig.client.post("/api/v1/events", json=EVENT)
    again = rig.client.post("/api/v1/events", json=EVENT)
    assert again.status_code == 200
    assert again.json()["created"] is False
    assert again.json()["deduplicated"] is True
    assert len(rig.client.get("/api/v1/events").json()) == 1
    # 已确认端未被重复投递
    assert rig.qc.get("/stats").json()["receive_requests"] == 1
    assert rig.report.get("/stats").json()["receive_requests"] == 1


def test_same_id_different_content_rejected(rig):
    rig.client.post("/api/v1/events", json=EVENT)
    resp = rig.client.post("/api/v1/events", json={**EVENT, "defect_level": "L3"})
    assert resp.status_code == 409
    detail = resp.json()["detail"]
    assert detail["error"] == "event_conflict"
    assert detail["fields"]["defect_level"] == {"stored": "L2", "incoming": "L3"}
    # 已入库记录不受影响
    assert len(rig.client.get("/api/v1/events").json()) == 1
    assert rig.states()["qc"]["status"] == "confirmed"


def test_ack_loss_recovery_no_double_count_no_revoke(rig):
    rig.report.post("/admin/behavior", json={"mode": "ack_loss"})
    resp = rig.client.post("/api/v1/events", json=EVENT)
    deliveries = {d["endpoint"]: d for d in resp.json()["record"]["deliveries"]}
    # 回执丢失：分发端记为待送，但接收端实际已保存
    assert deliveries["daily_report"]["status"] == "pending"
    assert deliveries["qc"]["status"] == "confirmed"
    assert rig.report.get("/stats").json()["stored_events"] == 1
    qc_confirmed_at = deliveries["qc"]["confirmed_at"]

    rig.report.post("/admin/behavior", json={"mode": "normal"})
    redo = rig.client.post(
        f"/api/v1/events/{EVENT['event_id']}/redeliver",
        params={"endpoint": "daily_report"},
    )
    assert redo.status_code == 200
    states = {d["endpoint"]: d for d in redo.json()["record"]["deliveries"]}
    assert states["daily_report"]["status"] == "confirmed"
    # 重投只入库一次：不重复计数
    stats = rig.report.get("/stats").json()
    assert stats["stored_events"] == 1
    assert stats["duplicates_suppressed"] == 1
    # 另一端的确认不被撤销、不被重投
    assert states["qc"]["status"] == "confirmed"
    assert states["qc"]["confirmed_at"] == qc_confirmed_at
    assert states["qc"]["attempts"] == 1


def test_restart_redelivers_pending_without_duplicates(rig):
    rig.qc.post("/admin/behavior", json={"mode": "down"})
    rig.client.post("/api/v1/events", json=EVENT)
    assert rig.states()["qc"]["status"] == "pending"
    rig.service.close()

    # 模拟服务重启：同一数据目录重建 service 与 app
    service2 = _build_service(rig.db_dir, rig.qc_app, rig.report_app)
    rig.qc.post("/admin/behavior", json={"mode": "normal"})
    with TestClient(create_app(service2)) as client2:
        # 重启后待送状态仍在（先持久化的结果），已确认端不受影响
        body = client2.get(f"/api/v1/events/{EVENT['event_id']}").json()
        states = {d["endpoint"]: d["status"] for d in body["deliveries"]}
        assert states == {"qc": "pending", "daily_report": "confirmed"}
        # 补送成功且两端都不重复计数
        client2.post(f"/api/v1/events/{EVENT['event_id']}/redeliver")
        final = {
            d["endpoint"]: d["status"]
            for d in client2.get(f"/api/v1/events/{EVENT['event_id']}").json()["deliveries"]
        }
        assert final == {"qc": "confirmed", "daily_report": "confirmed"}
    assert rig.qc.get("/stats").json()["stored_events"] == 1
    assert rig.report.get("/stats").json()["stored_events"] == 1


def test_background_retry_auto_recovers_after_restart(rig):
    rig.qc.post("/admin/behavior", json={"mode": "down"})
    rig.client.post("/api/v1/events", json=EVENT)
    rig.service.close()

    # 重启后自动重试循环从库中恢复 pending 并补送
    service2 = _build_service(rig.db_dir, rig.qc_app, rig.report_app, retry_interval=0.05)
    rig.qc.post("/admin/behavior", json={"mode": "normal"})
    with TestClient(create_app(service2)) as client2:
        states = {}
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            body = client2.get(f"/api/v1/events/{EVENT['event_id']}").json()
            states = {d["endpoint"]: d["status"] for d in body["deliveries"]}
            if states["qc"] == "confirmed":
                break
            time.sleep(0.05)
        assert states == {"qc": "confirmed", "daily_report": "confirmed"}
    assert rig.qc.get("/stats").json()["stored_events"] == 1


def test_per_endpoint_status_query(rig):
    rig.client.post("/api/v1/events", json=EVENT)
    resp = rig.client.get(f"/api/v1/events/{EVENT['event_id']}")
    assert resp.status_code == 200
    deliveries = resp.json()["deliveries"]
    assert {d["endpoint"] for d in deliveries} == {"qc", "daily_report"}
    for d in deliveries:
        assert d["status"] == "confirmed"
        assert d["attempts"] == 1
        assert d["confirmed_at"]
        assert d["last_error"] is None
    assert rig.client.get("/api/v1/events/NOPE").status_code == 404


def test_redeliver_errors(rig):
    assert rig.client.post("/api/v1/events/NOPE/redeliver").status_code == 404
    rig.client.post("/api/v1/events", json=EVENT)
    resp = rig.client.post(
        f"/api/v1/events/{EVENT['event_id']}/redeliver", params={"endpoint": "bogus"}
    )
    assert resp.status_code == 422


def test_endpoints_listing(rig):
    body = rig.client.get("/api/v1/endpoints").json()
    assert {e["name"] for e in body} == {"qc", "daily_report"}


def test_event_validation(rig):
    url = "/api/v1/events"
    assert rig.client.post(url, json={"event_id": "X"}).status_code == 422
    assert rig.client.post(url, json={**EVENT, "event_id": "  "}).status_code == 422
    assert rig.client.post(url, json={**EVENT, "mileage_m": -1}).status_code == 422
    assert rig.client.post(url, json={**EVENT, "extra_field": 1}).status_code == 422


def test_planner_api_preserved(rig):
    payload = json.loads((SAMPLES / "safe_subscriber_first.json").read_text(encoding="utf-8"))
    resp = rig.client.post("/api/v1/plan", json=payload)
    assert resp.status_code == 200
    assert resp.json()["feasible"] is True
    paths = rig.client.get("/openapi.json").json()["paths"]
    assert "/api/v1/plan" in paths
    assert "/api/v1/events" in paths
