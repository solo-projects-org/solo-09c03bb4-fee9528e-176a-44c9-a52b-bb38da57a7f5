"""接收端：保存成功才应答、重投只入库一次、故障注入。"""

import pytest
from fastapi.testclient import TestClient

from app.receiver import create_receiver_app

EVENT = {"event_id": "EVT-1", "car_id": "GJC-01", "mileage_m": 100, "defect_level": "L1"}


@pytest.fixture()
def client(tmp_path):
    app = create_receiver_app("qc", tmp_path / "qc")
    with TestClient(app) as c:
        yield c


def test_receive_stores_then_acks(client):
    resp = client.post("/receive", json=EVENT)
    assert resp.status_code == 200
    body = resp.json()
    assert body["result"] == "stored"
    assert body["stored_events"] == 1
    # 确实已落库
    stored = client.get("/events").json()
    assert stored["stored_events"] == 1
    assert stored["events"][0]["event_id"] == "EVT-1"


def test_redelivery_stored_only_once(client):
    assert client.post("/receive", json=EVENT).json()["result"] == "stored"
    again = client.post("/receive", json=EVENT)
    assert again.status_code == 200
    assert again.json()["result"] == "duplicate_ignored"
    stats = client.get("/stats").json()
    assert stats["stored_events"] == 1  # 重投不重复计数
    assert stats["receive_requests"] == 2
    assert stats["duplicates_suppressed"] == 1


def test_conflicting_content_rejected(client):
    client.post("/receive", json=EVENT)
    resp = client.post("/receive", json={**EVENT, "mileage_m": 999})
    assert resp.status_code == 409
    assert "mileage_m" in resp.json()["detail"]["fields"]
    stats = client.get("/stats").json()
    assert stats["stored_events"] == 1
    assert stats["conflicts_rejected"] == 1


def test_down_mode_saves_nothing(client):
    assert client.post("/admin/behavior", json={"mode": "down"}).status_code == 200
    resp = client.post("/receive", json=EVENT)
    assert resp.status_code == 503
    assert client.get("/stats").json()["stored_events"] == 0


def test_ack_loss_stores_but_receipt_missing(client):
    client.post("/admin/behavior", json={"mode": "ack_loss"})
    resp = client.post("/receive", json=EVENT)
    assert resp.status_code == 500  # 回执丢失：分发端视为失败
    assert client.get("/stats").json()["stored_events"] == 1  # 实际已保存

    # 回执丢失后的重投：去重，不重复计数
    client.post("/admin/behavior", json={"mode": "normal"})
    again = client.post("/receive", json=EVENT)
    assert again.status_code == 200
    assert again.json()["result"] == "duplicate_ignored"
    stats = client.get("/stats").json()
    assert stats["stored_events"] == 1
    assert stats["duplicates_suppressed"] == 1


def test_store_survives_restart(tmp_path):
    app1 = create_receiver_app("qc", tmp_path / "qc")
    with TestClient(app1) as c1:
        assert c1.post("/receive", json=EVENT).status_code == 200
    # 同一数据目录重建（模拟重启）：已存事件仍在，重投仍去重
    app2 = create_receiver_app("qc", tmp_path / "qc")
    with TestClient(app2) as c2:
        assert c2.get("/stats").json()["stored_events"] == 1
        assert c2.post("/receive", json=EVENT).json()["result"] == "duplicate_ignored"


def test_invalid_mode_rejected(client):
    assert client.post("/admin/behavior", json={"mode": "bogus"}).status_code == 422
