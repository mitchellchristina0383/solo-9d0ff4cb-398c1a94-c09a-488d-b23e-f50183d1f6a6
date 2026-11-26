"""端到端 / 用例层测试：样例、成功交接、标签冲突、交接后旧计划退回。"""

from __future__ import annotations

import pytest

from app import create_app
from app.db import (
    CONFLICT_APP_RS,
    CONFLICT_OVERLAP_RS,
    RS_NEW,
    RS_OLD,
    UID_V1_NOTREADY_3,
    UID_V1_READY_1,
    UID_V1_READY_2,
    UID_V2_READY_1,
    UID_V2_READY_2,
)
from app.domain import matches


@pytest.fixture()
def app(tmp_path):
    application = create_app(db_path=tmp_path / "drill.db")
    application.config.update(TESTING=True)
    yield application


@pytest.fixture()
def client(app):
    return app.test_client()


def get_state(client):
    resp = client.get("/api/state")
    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload["ok"] is True
    return payload["state"]


def pod_by_uid(state, uid):
    return next(p for p in state["pods"] if p["uid"] == uid)


def rs_by_id(state, rs_id):
    return next(r for r in state["replicasets"] if r["id"] == rs_id)


def signature(state):
    """用于断言“办不到时保留原状”的最小状态签名。"""
    return (
        sorted(
            (p["uid"], p["rs_id"], p["ready"], tuple(sorted(p["labels"].items())))
            for p in state["pods"]
        ),
        sorted((r["id"], r["desired"]) for r in state["replicasets"]),
    )


# ---------------------------------------------------------------------------
# 样例数据
# ---------------------------------------------------------------------------

def test_seed_has_two_rs_five_pods_one_service_and_ready_endpoints(client):
    state = get_state(client)
    assert len(state["replicasets"]) == 2
    assert len(state["pods"]) == 5
    assert len(state["services"]) == 1

    old = rs_by_id(state, RS_OLD)
    new = rs_by_id(state, RS_NEW)
    assert old["desired"] == 4 and old["owned_count"] == 3 and old["ready_count"] == 2
    assert new["desired"] == 2 and new["owned_count"] == 2 and new["ready_count"] == 2

    svc = state["services"][0]
    assert set(svc["endpoint_uids"]) == {
        UID_V1_READY_1, UID_V1_READY_2, UID_V2_READY_1, UID_V2_READY_2,
    }
    assert UID_V1_NOTREADY_3 not in svc["endpoint_uids"]
    assert len(svc["not_ready_matching"]) == 1


def test_home_page_renders(client):
    html = client.get("/").get_data(as_text=True)
    assert "ReplicaSet 换组交接演练台" in html
    assert "shop-svc" in html
    assert "就绪名单" in html
    assert "web-v1" in html and "web-v2" in html


# ---------------------------------------------------------------------------
# 成功交接
# ---------------------------------------------------------------------------

def test_ready_handover_keeps_uid_and_ready_preserves_service_and_moves_selector_only(client):
    before = get_state(client)
    endpoints_before = set(before["services"][0]["endpoint_uids"])

    resp = client.post("/api/handover", json={
        "pod_uid": UID_V1_READY_1, "target_rs_id": RS_NEW,
    })
    assert resp.status_code == 200, resp.get_json()
    result = resp.get_json()["result"]
    assert result["source_desired"] == 3
    assert result["target_desired"] == 3

    state = get_state(client)
    pod = pod_by_uid(state, UID_V1_READY_1)

    # UID 与就绪状态保留（同一行被 UPDATE，而不是删除重建）
    assert pod["uid"] == UID_V1_READY_1
    assert pod["ready"] is True

    # 归属改变
    assert pod["rs_id"] == RS_NEW

    # 标签：判别标签切换，无关标签 zone 保留
    assert pod["labels"]["release"] == "green"
    assert pod["labels"]["app"] == "shop"
    assert pod["labels"]["zone"] == "a"

    # 期望数联动：旧组 -1，新组 +1
    assert rs_by_id(state, RS_OLD)["desired"] == 3
    assert rs_by_id(state, RS_NEW)["desired"] == 3

    # 仍命中原 Service，就绪名单不变（同一个 UID 仍在）
    svc = state["services"][0]
    assert UID_V1_READY_1 in svc["endpoint_uids"]
    assert set(svc["endpoint_uids"]) == endpoints_before

    # 选择器层面只命中新 ReplicaSet
    selectors = {r["id"]: r["selector"] for r in state["replicasets"]}
    assert matches(pod["labels"], selectors[RS_NEW])
    assert not matches(pod["labels"], selectors[RS_OLD])
    assert pod["matched_rs_ids"] == [RS_NEW]

    # 有交接记录
    assert len(state["handovers"]) == 1
    assert state["handovers"][0]["pod_uid"] == UID_V1_READY_1


def test_non_ready_pod_handover_refused_and_state_unchanged(client):
    before = get_state(client)
    resp = client.post("/api/handover", json={
        "pod_uid": UID_V1_NOTREADY_3, "target_rs_id": RS_NEW,
    })
    assert resp.status_code == 409
    body = resp.get_json()
    assert body["ok"] is False
    assert body["error"]["code"] == "pod_not_ready"
    assert "未就绪" in body["error"]["message"]

    after = get_state(client)
    assert signature(after) == signature(before)


def test_same_replicaset_handover_refused(client):
    resp = client.post("/api/handover", json={
        "pod_uid": UID_V1_READY_1, "target_rs_id": RS_OLD,
    })
    assert resp.status_code == 409
    assert resp.get_json()["error"]["code"] == "same_replicaset"


# ---------------------------------------------------------------------------
# 标签冲突：必须拒绝且保留原状
# ---------------------------------------------------------------------------

def test_label_value_conflict_with_service_blocks_and_preserves_state(client):
    made = client.post("/api/drill/conflict", json={"kind": "app_key"})
    assert made.status_code == 200

    before = get_state(client)
    resp = client.post("/api/handover", json={
        "pod_uid": UID_V1_READY_1, "target_rs_id": CONFLICT_APP_RS,
    })
    assert resp.status_code == 409
    error = resp.get_json()["error"]
    assert error["code"] == "label_conflict"
    assert "app" in error["message"] and "Service" in error["message"]

    after = get_state(client)
    assert signature(after) == signature(before)
    pod = pod_by_uid(after, UID_V1_READY_1)
    assert pod["rs_id"] == RS_OLD
    assert pod["labels"]["release"] == "blue"
    assert rs_by_id(after, RS_OLD)["desired"] == 4


def test_selector_overlap_conflict_blocks_and_preserves_state(client):
    made = client.post("/api/drill/conflict", json={"kind": "overlap"})
    assert made.status_code == 200

    before = get_state(client)
    resp = client.post("/api/handover", json={
        "pod_uid": UID_V1_READY_2, "target_rs_id": CONFLICT_OVERLAP_RS,
    })
    assert resp.status_code == 409
    error = resp.get_json()["error"]
    assert error["code"] == "selector_overlap"
    assert "只命中新 ReplicaSet" in error["message"]

    after = get_state(client)
    assert signature(after) == signature(before)
    assert pod_by_uid(after, UID_V1_READY_2)["rs_id"] == RS_OLD


# ---------------------------------------------------------------------------
# 计划：生成 -> 交接 -> 旧计划执行被退回 -> 重算后可执行
# ---------------------------------------------------------------------------

def test_stale_add_plan_is_returned_after_handover_then_replan_executes(client):
    # 初始 web-v1 desired=4 / owned=3 -> +1 计划
    resp = client.post("/api/plans", json={"rs_id": RS_OLD})
    assert resp.status_code == 200
    plan1 = resp.get_json()["result"]
    assert plan1["add_count"] == 1
    plan1_id = plan1["plan_id"]

    # 交接一个就绪 Pod：web-v1 desired 4→3，归属 3→2
    assert client.post("/api/handover", json={
        "pod_uid": UID_V1_READY_1, "target_rs_id": RS_NEW,
    }).status_code == 200

    # 执行交接前生成的旧计划：必须 409 + stale_plan，并被标记为 returned
    resp = client.post(f"/api/plans/{plan1_id}/execute")
    assert resp.status_code == 409
    error = resp.get_json()["error"]
    assert error["code"] == "stale_plan"
    assert "重新生成" in error["message"]

    state = get_state(client)
    old_plan = next(p for p in state["plans"] if p["id"] == plan1_id)
    assert old_plan["status"] == "returned"
    assert "重新生成" in old_plan["stale_reason"]

    # 旧计划没有产生任何副本变动：仍然是 5 个 Pod，旧组归属 2
    assert len(state["pods"]) == 5
    assert rs_by_id(state, RS_OLD)["owned_count"] == 2

    # 已退回计划不能再次执行
    again = client.post(f"/api/plans/{plan1_id}/execute")
    assert again.status_code == 409
    assert again.get_json()["error"]["code"] == "plan_returned"

    # 按当前 desired=3 / owned=2 重算：仍然是 +1（但依据已更新），执行成功
    resp = client.post("/api/plans", json={"rs_id": RS_OLD})
    plan2_id = resp.get_json()["result"]["plan_id"]
    assert resp.get_json()["result"]["add_count"] == 1

    done = client.post(f"/api/plans/{plan2_id}/execute")
    assert done.status_code == 200, done.get_json()

    state = get_state(client)
    assert len(state["pods"]) == 6
    old_rs = rs_by_id(state, RS_OLD)
    assert old_rs["owned_count"] == 3

    executed = next(p for p in state["plans"] if p["id"] == plan2_id)
    assert executed["status"] == "executed"

    # 新 Pod 标签来自 RS 等值选择器，因此命中 web-v1
    new_pod = next(p for p in state["pods"] if p["uid"] not in {
        UID_V1_READY_1, UID_V1_READY_2, UID_V1_NOTREADY_3,
        UID_V2_READY_1, UID_V2_READY_2,
    })
    assert new_pod["labels"]["release"] == "blue"
    assert new_pod["ready"] is True
    assert new_pod["rs_id"] == RS_OLD

    # 已执行计划不能重复执行
    repeat = client.post(f"/api/plans/{plan2_id}/execute")
    assert repeat.status_code == 409
    assert repeat.get_json()["error"]["code"] == "plan_already_executed"


def test_handover_makes_target_group_plan_stale_too(client):
    # 新组初始 desired=2 / owned=2，生成一个空操作计划
    plan = client.post("/api/plans", json={"rs_id": RS_NEW}).get_json()["result"]
    assert plan["add_count"] == 0

    assert client.post("/api/handover", json={
        "pod_uid": UID_V1_READY_1, "target_rs_id": RS_NEW,
    }).status_code == 200

    resp = client.post(f"/api/plans/{plan['plan_id']}/execute")
    assert resp.status_code == 409
    error = resp.get_json()["error"]
    assert error["code"] == "stale_plan"
    assert "期望副本数 2 → 3" in error["message"]


def test_stale_delete_plan_is_returned_after_handover(client):
    # 把 web-v1 期望数调到 2：owned=3 -> 删除 1，未就绪的 UID3 应排在首位
    assert client.post(f"/api/rs/{RS_OLD}/desired", json={"desired": 2}).status_code == 200
    plan = client.post("/api/plans", json={"rs_id": RS_OLD}).get_json()["result"]
    assert plan["add_count"] == 0
    assert plan["delete_uids"] == [UID_V1_NOTREADY_3]
    plan_id = plan["plan_id"]

    # 交接走一个就绪 Pod：归属集合变化 + desired 2→1
    assert client.post("/api/handover", json={
        "pod_uid": UID_V1_READY_2, "target_rs_id": RS_NEW,
    }).status_code == 200

    resp = client.post(f"/api/plans/{plan_id}/execute")
    assert resp.status_code == 409
    assert resp.get_json()["error"]["code"] == "stale_plan"

    state = get_state(client)
    # 一个都没删
    assert pod_by_uid(state, UID_V1_NOTREADY_3) is not None
    assert len(state["pods"]) == 5
    assert rs_by_id(state, RS_OLD)["desired"] == 1

    # 重算：desired=1 / owned=2 仍需删 1，仍优先删未就绪 UID3；执行后删除
    plan2 = client.post("/api/plans", json={"rs_id": RS_OLD}).get_json()["result"]
    assert plan2["delete_uids"] == [UID_V1_NOTREADY_3]
    assert client.post(f"/api/plans/{plan2['plan_id']}/execute").status_code == 200

    state = get_state(client)
    assert all(p["uid"] != UID_V1_NOTREADY_3 for p in state["pods"])
    assert rs_by_id(state, RS_OLD)["owned_count"] == 1


# ---------------------------------------------------------------------------
# 表单流程（验证网页闪现“已退回 / 重新生成”）
# ---------------------------------------------------------------------------

def test_form_flow_execute_old_plan_shows_return_and_recalc_prompt(client):
    client.post("/plans/generate", data={"rs_id": RS_OLD})
    state = get_state(client)
    plan_id = state["plans"][0]["id"]

    client.post("/handover", data={
        "pod_uid": UID_V1_READY_1, "target_rs_id": RS_NEW,
    })

    resp = client.post(f"/plans/{plan_id}/execute", follow_redirects=True)
    assert resp.status_code == 200
    html = resp.get_data(as_text=True)
    assert "旧计划已退回" in html or "已退回" in html
    assert "重新生成" in html
    assert "情况已变化" in html


def test_reset_restores_seed(client):
    client.post("/api/drill/conflict", json={"kind": "app_key"})
    client.post("/api/handover", json={
        "pod_uid": UID_V1_READY_1, "target_rs_id": RS_NEW,
    })
    assert client.post("/api/reset").status_code == 200

    state = get_state(client)
    assert len(state["replicasets"]) == 2
    assert len(state["pods"]) == 5
    assert state["plans"] == []
    assert rs_by_id(state, RS_OLD)["desired"] == 4
    assert pod_by_uid(state, UID_V1_READY_1)["rs_id"] == RS_OLD
