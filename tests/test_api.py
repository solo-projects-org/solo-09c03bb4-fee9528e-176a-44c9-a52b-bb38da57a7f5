"""HTTP 层测试：Swagger、健康检查、两个样例端到端。"""

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.main import app

SAMPLES = Path(__file__).resolve().parent.parent / "samples"


@pytest.fixture(scope="module")
def client():
    with TestClient(app) as c:
        yield c


def test_health_and_docs(client):
    assert client.get("/health").json() == {"status": "ok"}
    docs = client.get("/openapi.json")
    assert docs.status_code == 200
    paths = docs.json()["paths"]
    assert "/api/v1/plan" in paths
    assert "/api/v1/compatibility" in paths


def test_plan_safe_sample_end_to_end(client):
    payload = json.loads((SAMPLES / "safe_subscriber_first.json").read_text(encoding="utf-8"))
    resp = client.post("/api/v1/plan", json=payload)
    assert resp.status_code == 200
    body = resp.json()
    assert body["feasible"] is True
    assert len(body["steps"]) == 6
    # 最终状态三方均为 new
    assert body["steps"][-1]["state_after"] == {
        "collector": "new", "qc": "new", "daily_report": "new",
    }


def test_plan_infeasible_sample_end_to_end(client):
    payload = json.loads((SAMPLES / "infeasible_circular.json").read_text(encoding="utf-8"))
    resp = client.post("/api/v1/plan", json=payload)
    assert resp.status_code == 200
    body = resp.json()
    assert body["feasible"] is False
    assert body["blocked_pairs"]
    for report in body["blocked_pairs"]:
        assert report["counterexample"]
        assert report["violations"]


def test_compatibility_endpoint_returns_eight_pairs(client):
    payload = json.loads((SAMPLES / "safe_subscriber_first.json").read_text(encoding="utf-8"))
    resp = client.post("/api/v1/compatibility", json=payload)
    assert resp.status_code == 200
    pairs = resp.json()
    assert len(pairs) == 8  # 2 发布端版本 × 2 订阅端 × 2 订阅端版本


def test_bad_enum_payload_rejected(client):
    bad = {
        "parties": [
            {
                "id": "p", "role": "producer",
                "old_schema": [{"name": "x", "type": "enum", "required": True, "enum_values": []}],
                "new_schema": [],
            },
            {"id": "s1", "role": "subscriber", "old_schema": [], "new_schema": []},
            {"id": "s2", "role": "subscriber", "old_schema": [], "new_schema": []},
        ]
    }
    resp = client.post("/api/v1/plan", json=bad)
    assert resp.status_code == 422
