"""并发交接 / 并发计划执行的专项测试。

覆盖题目要求的三类实测：
1. 同一 Pod 同时交给同一目标 RS：只有一笔生效，期望数只联动一次，只有一条成功记录；
2. 两个不同 Pod 并发交接：按写锁串行后的实际生效顺序逐笔核对，符合条件的两笔都完成，
   且每笔记录两组期望数的实际前后值（链式衔接）；
3. 交接与缩容的两种生效顺序：
   - 缩容先提交并删掉 Pod：交接报错、两组期望数不变、无成功交接记录；
   - 交接先提交：已通过预检的旧缩容计划必须在写事务复核时被退回（returned），不删任何 Pod。

另含多轮真实屏障并发的“竞速不变量”测试（不预设谁赢，两种结果都必须自洽）。
"""

from __future__ import annotations

import threading

import pytest

from app import create_app, services
from app.db import (
    RS_NEW,
    RS_OLD,
    UID_V1_READY_1,
    UID_V1_READY_2,
    connect,
    reset_to_seed,
)
from app.services import UseCaseError


@pytest.fixture()
def app(tmp_path):
    application = create_app(db_path=tmp_path / "concurrency.db")
    application.config.update(TESTING=True)
    yield application


@pytest.fixture()
def db_path(app):
    return app.config["DB_PATH"]


@pytest.fixture()
def client(app):
    return app.test_client()


def _rs_desired(db_path, rs_id):
    db = connect(db_path)
    try:
        return db.execute("SELECT desired FROM replicasets WHERE id=?", (rs_id,)).fetchone()[0]
    finally:
        db.close()


def _handover_count(db_path):
    db = connect(db_path)
    try:
        return db.execute("SELECT COUNT(*) AS c FROM handovers").fetchone()["c"]
    finally:
        db.close()


# ---------------------------------------------------------------------------
# 1. 同一 Pod 重复交接：只能一笔生效
# ---------------------------------------------------------------------------

def test_same_pod_concurrent_handover_single_effect(db_path):
    barrier = threading.Barrier(2)
    successes: list[dict] = []
    errors: list[UseCaseError] = []

    def worker():
        db = connect(db_path)
        barrier.wait(timeout=10)
        try:
            successes.append(services.handover(db, UID_V1_READY_1, RS_NEW))
        except UseCaseError as exc:
            errors.append(exc)
        finally:
            db.close()

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=20)
        assert not t.is_alive()

    assert len(successes) == 1
    assert len(errors) == 1
    assert errors[0].code == "same_replicaset"
    assert errors[0].status == 409

    # 期望数只联动一次：4-1=3 / 2+1=3，而不是各改两次变成 2 / 4。
    assert _rs_desired(db_path, RS_OLD) == 3
    assert _rs_desired(db_path, RS_NEW) == 3
    assert _handover_count(db_path) == 1

    db = connect(db_path)
    try:
        row = db.execute(
            "SELECT * FROM handovers WHERE pod_uid=?", (UID_V1_READY_1,)
        ).fetchone()
        assert row is not None
        assert (row["source_desired_before"], row["source_desired_after"]) == (4, 3)
        assert (row["target_desired_before"], row["target_desired_after"]) == (2, 3)
        assert "4→3" in row["desired_change"] and "2→3" in row["desired_change"]
        pod = db.execute("SELECT ready, rs_id FROM pods WHERE uid=?", (UID_V1_READY_1,)).fetchone()
        assert pod["ready"] == 1 and pod["rs_id"] == RS_NEW
    finally:
        db.close()


def test_same_pod_concurrent_handover_over_http(app):
    """整条 Flask/JSON 链路（每请求独立连接）下同样只能一笔生效。"""
    barrier = threading.Barrier(2)
    statuses: list[int] = []
    codes: list[str] = []

    def worker():
        client = app.test_client()
        barrier.wait(timeout=10)
        resp = client.post("/api/handover", json={
            "pod_uid": UID_V1_READY_1, "target_rs_id": RS_NEW,
        })
        statuses.append(resp.status_code)
        if resp.status_code != 200:
            codes.append(resp.get_json()["error"]["code"])

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=20)

    assert sorted(statuses) == [200, 409]
    assert codes == ["same_replicaset"]
    assert _rs_desired(app.config["DB_PATH"], RS_OLD) == 3
    assert _rs_desired(app.config["DB_PATH"], RS_NEW) == 3
    assert _handover_count(app.config["DB_PATH"]) == 1


# ---------------------------------------------------------------------------
# 2. 两个不同 Pod 并发交接：都应完成，前后值按生效顺序链式衔接
# ---------------------------------------------------------------------------

def test_two_different_pods_concurrent_handovers_both_commit(db_path):
    barrier = threading.Barrier(2)
    outcomes: list[tuple[str, object]] = []

    def worker(uid):
        db = connect(db_path)
        barrier.wait(timeout=10)
        try:
            outcomes.append(("ok", services.handover(db, uid, RS_NEW)))
        except UseCaseError as exc:
            outcomes.append(("err", exc.code))
        finally:
            db.close()

    threads = [
        threading.Thread(target=worker, args=(UID_V1_READY_1,)),
        threading.Thread(target=worker, args=(UID_V1_READY_2,)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=20)

    assert [kind for kind, _ in outcomes].count("ok") == 2
    assert not [o for o in outcomes if o[0] == "err"]

    assert _rs_desired(db_path, RS_OLD) == 2   # 4 - 2
    assert _rs_desired(db_path, RS_NEW) == 4   # 2 + 2
    assert _handover_count(db_path) == 2

    db = connect(db_path)
    try:
        rows = db.execute("SELECT * FROM handovers ORDER BY id").fetchall()
        src_pairs = [(r["source_desired_before"], r["source_desired_after"]) for r in rows]
        tgt_pairs = [(r["target_desired_before"], r["target_desired_after"]) for r in rows]
        # 不预设谁先谁后，但两笔必须按实际生效顺序链式衔接。
        assert sorted(src_pairs) == [(3, 2), (4, 3)]
        assert sorted(tgt_pairs) == [(2, 3), (3, 4)]
        for r in rows:
            assert r["source_desired_after"] == r["source_desired_before"] - 1
            assert r["target_desired_after"] == r["target_desired_before"] + 1

        # UID / Ready 保留，且都归属新组
        for uid in (UID_V1_READY_1, UID_V1_READY_2):
            pod = db.execute("SELECT ready, rs_id FROM pods WHERE uid=?", (uid,)).fetchone()
            assert pod["ready"] == 1 and pod["rs_id"] == RS_NEW
    finally:
        db.close()


# ---------------------------------------------------------------------------
# 3a. 缩容先生效（删掉 Pod）：交接必须报错且不留任何副作用
# ---------------------------------------------------------------------------

def test_scale_delete_commits_first_then_inflight_handover_fails_without_side_effects(
    db_path, monkeypatch
):
    setup = connect(db_path)
    services.set_desired(setup, RS_OLD, 1)
    plan = services.generate_plan(setup, RS_OLD)
    plan_id = plan["plan_id"]
    # desired=1 / owned=3：未就绪 UID3 先删，其次是就绪的 UID1
    assert plan["delete_uids"][-1] == UID_V1_READY_1
    setup.close()

    entered = threading.Event()
    release = threading.Event()

    def hook(point):
        if point == "handover:before_begin":
            entered.set()
            assert release.wait(timeout=10)

    monkeypatch.setattr(services, "_CONCURRENCY_HOOK", hook)

    handover_outcome: dict[str, object] = {}

    def handover_worker():
        db = connect(db_path)
        try:
            handover_outcome["result"] = services.handover(db, UID_V1_READY_1, RS_NEW)
        except UseCaseError as exc:
            handover_outcome["error"] = exc
        finally:
            db.close()

    t = threading.Thread(target=handover_worker)
    t.start()
    assert entered.wait(timeout=5)

    # 交接已读到 Pod、但尚未取写锁写入；此时缩容计划抢先提交删除。
    scale_db = connect(db_path)
    scale_result = services.execute_plan(scale_db, plan_id)
    scale_db.close()
    assert UID_V1_READY_1 in scale_result["deleted_uids"]

    release.set()
    t.join(timeout=20)
    assert not t.is_alive()

    err = handover_outcome.get("error")
    assert isinstance(err, UseCaseError)
    assert err.code == "pod_not_found"
    assert handover_outcome.get("result") is None

    db = connect(db_path)
    try:
        # 两组期望数保持交接前的原值：v1=1（没有被再 -1 成 0），v2=2（没有 +1 成 3）
        assert db.execute("SELECT desired FROM replicasets WHERE id=?", (RS_OLD,)).fetchone()[0] == 1
        assert db.execute("SELECT desired FROM replicasets WHERE id=?", (RS_NEW,)).fetchone()[0] == 2
        # 不能留下成功交接记录
        assert db.execute("SELECT COUNT(*) AS c FROM handovers").fetchone()["c"] == 0
        # Pod 确实已被缩容删除
        assert db.execute("SELECT 1 FROM pods WHERE uid=?", (UID_V1_READY_1,)).fetchone() is None
    finally:
        db.close()


# ---------------------------------------------------------------------------
# 3b. 交接先生效：预检已过的缩容计划必须退回，且一个 Pod 都不删
# ---------------------------------------------------------------------------

def test_handover_commits_first_then_scale_plan_is_returned_and_keeps_all_pods(
    db_path, monkeypatch
):
    setup = connect(db_path)
    services.set_desired(setup, RS_OLD, 1)
    plan_id = services.generate_plan(setup, RS_OLD)["plan_id"]
    setup.close()

    entered = threading.Event()
    release = threading.Event()

    def hook(point):
        if point == "plan:after_precheck":
            entered.set()
            assert release.wait(timeout=10)

    monkeypatch.setattr(services, "_CONCURRENCY_HOOK", hook)

    plan_outcome: dict[str, object] = {}

    def plan_worker():
        db = connect(db_path)
        try:
            plan_outcome["result"] = services.execute_plan(db, plan_id)
        except UseCaseError as exc:
            plan_outcome["error"] = exc
        finally:
            db.close()

    t = threading.Thread(target=plan_worker)
    t.start()
    assert entered.wait(timeout=5)

    # 计划已按旧依据通过预检；交接在此期间完整提交。
    handover_db = connect(db_path)
    hr = services.handover(handover_db, UID_V1_READY_1, RS_NEW)
    handover_db.close()
    assert hr["source_desired_after"] == 0
    assert hr["target_desired_after"] == 3

    release.set()
    t.join(timeout=20)
    assert not t.is_alive()

    err = plan_outcome.get("error")
    assert isinstance(err, UseCaseError)
    assert err.code == "stale_plan"

    db = connect(db_path)
    try:
        assert db.execute("SELECT status FROM plans WHERE id=?", (plan_id,)).fetchone()[0] == "returned"
        # 所有 Pod 保留（初始 5 个，一个都没被删）
        assert db.execute("SELECT COUNT(*) AS c FROM pods").fetchone()["c"] == 5
        assert db.execute("SELECT rs_id FROM pods WHERE uid=?", (UID_V1_READY_1,)).fetchone()[0] == RS_NEW
        assert db.execute("SELECT desired FROM replicasets WHERE id=?", (RS_OLD,)).fetchone()[0] == 0
        assert db.execute("SELECT desired FROM replicasets WHERE id=?", (RS_NEW,)).fetchone()[0] == 3
    finally:
        db.close()


# ---------------------------------------------------------------------------
# 4. 多轮无预设竞速：无论谁先生效，全局不变量都必须成立
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("round_idx", range(6))
def test_parallel_handover_and_scale_race_invariants(db_path, round_idx):
    admin = connect(db_path)
    reset_to_seed(admin)
    services.set_desired(admin, RS_OLD, 1)
    plan_id = services.generate_plan(admin, RS_OLD)["plan_id"]
    admin.close()

    barrier = threading.Barrier(2)
    handover_out: dict[str, object] = {}
    plan_out: dict[str, object] = {}

    def handover_worker():
        db = connect(db_path)
        barrier.wait(timeout=10)
        try:
            handover_out["ok"] = services.handover(db, UID_V1_READY_1, RS_NEW)
        except UseCaseError as exc:
            handover_out["err"] = exc.code
        finally:
            db.close()

    def plan_worker():
        db = connect(db_path)
        barrier.wait(timeout=10)
        try:
            plan_out["ok"] = services.execute_plan(db, plan_id)
        except UseCaseError as exc:
            plan_out["err"] = exc.code
        finally:
            db.close()

    t1 = threading.Thread(target=handover_worker)
    t2 = threading.Thread(target=plan_worker)
    t1.start(); t2.start()
    t1.join(timeout=20); t2.join(timeout=20)
    assert not t1.is_alive() and not t2.is_alive()

    h_ok = "ok" in handover_out
    p_ok = "ok" in plan_out
    # 绝不能两笔都“成功”：交接移动/改动与按旧名单删除同一 Pod 冲突。
    assert not (h_ok and p_ok), (handover_out, plan_out)

    db = connect(db_path)
    try:
        plan_status = db.execute("SELECT status FROM plans WHERE id=?", (plan_id,)).fetchone()[0]
        total_pods = db.execute("SELECT COUNT(*) AS c FROM pods").fetchone()["c"]
        v1 = db.execute("SELECT desired FROM replicasets WHERE id=?", (RS_OLD,)).fetchone()[0]
        v2 = db.execute("SELECT desired FROM replicasets WHERE id=?", (RS_NEW,)).fetchone()[0]
        handover_rows = db.execute("SELECT COUNT(*) AS c FROM handovers").fetchone()["c"]
        uid1 = db.execute("SELECT rs_id FROM pods WHERE uid=?", (UID_V1_READY_1,)).fetchone()

        if h_ok:
            # 交接先生效：计划必须 stale 并 returned，5 个 Pod 全保留。
            assert plan_out.get("err") == "stale_plan", plan_out
            assert plan_status == "returned"
            assert total_pods == 5
            assert uid1 is not None and uid1[0] == RS_NEW
            assert (v1, v2) == (0, 3)
            assert handover_rows == 1
        else:
            # 缩容先生效：交接必须失败且无成功记录，期望数不被交接联动。
            assert handover_out.get("err") in {"pod_not_found", "same_replicaset", "handover_conflict"}
            assert handover_rows == 0
            assert (v1, v2) == (1, 2)
            assert uid1 is None
            assert p_ok and plan_status == "executed"
    finally:
        db.close()


# ---------------------------------------------------------------------------
# 5. 页面 / API 能看到逐笔实际前后值
# ---------------------------------------------------------------------------

def test_state_and_page_render_actual_before_after(client):
    resp = client.post("/api/handover", json={
        "pod_uid": UID_V1_READY_1, "target_rs_id": RS_NEW,
    })
    assert resp.status_code == 200

    state = client.get("/api/state").get_json()["state"]
    record = state["handovers"][0]
    assert record["source_desired_before"] == 4
    assert record["source_desired_after"] == 3
    assert record["target_desired_before"] == 2
    assert record["target_desired_after"] == 3
    assert record["desired_pairs"][0]["before"] == 4
    assert record["desired_pairs"][0]["after"] == 3

    html = client.get("/").get_data(as_text=True)
    assert "两组期望数实际前后值" in html
    assert "web-v1" in html and "4" in html and "3" in html
