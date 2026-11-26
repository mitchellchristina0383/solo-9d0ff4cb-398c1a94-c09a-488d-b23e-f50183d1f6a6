"""用例层：交接、期望数调整、副本计划生成/执行、页面快照。

Web 路由和 JSON API 都调用这里的函数，保证两条入口行为一致。
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from typing import Any, Callable

from .db import (
    CONFLICT_APP_RS,
    CONFLICT_OVERLAP_RS,
    immediate_transaction,
)
from .domain import (
    HandoverImpossible,
    choose_delete_uids,
    matches,
    plan_handover_labels,
    plan_is_stale,
    selector_text,
    short_uid,
)


class UseCaseError(Exception):
    def __init__(self, code: str, message: str, status: int = 409, extra: dict | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status
        self.extra = extra or {}


# ---------------------------------------------------------------------------
# 并发实测卡点（生产运行时为 None，行为完全不变）
#
# 测试 / 实测脚本可注入 ``_CONCURRENCY_HOOK``，在关键时序点阻塞，以确定性地
# 复现“读到旧状态后、提交前”的交错：
#   - "handover:before_begin"：交接已做完事务前读取，尚未 BEGIN IMMEDIATE
#   - "plan:after_precheck"  ：计划已按旧快照通过过期预检，尚未进入写事务复核
# ---------------------------------------------------------------------------

_CONCURRENCY_HOOK: Callable[[str], None] | None = None


def _hook(point: str) -> None:
    hook = _CONCURRENCY_HOOK
    if hook is not None:
        hook(point)


# ---------------------------------------------------------------------------
# 行 -> dict
# ---------------------------------------------------------------------------

def _rs_dict(row: sqlite3.Row) -> dict[str, Any]:
    data = dict(row)
    data["selector"] = json.loads(data.pop("selector_json"))
    data["desired"] = int(data["desired"])
    data["drill"] = bool(data.get("drill", 0))
    return data


def _pod_dict(row: sqlite3.Row) -> dict[str, Any]:
    data = dict(row)
    data["labels"] = json.loads(data.pop("labels_json"))
    data["ready"] = bool(data["ready"])
    return data


def _svc_dict(row: sqlite3.Row) -> dict[str, Any]:
    data = dict(row)
    data["selector"] = json.loads(data.pop("selector_json"))
    return data


def _audit(db: sqlite3.Connection, ok: bool, kind: str, message: str) -> None:
    db.execute(
        "INSERT INTO audit_log(ok, kind, message) VALUES (?,?,?)",
        (1 if ok else 0, kind, message),
    )


# ---------------------------------------------------------------------------
# 快照（首页 + /api/state 共用）
# ---------------------------------------------------------------------------

def build_snapshot(db: sqlite3.Connection) -> dict[str, Any]:
    rs_rows = db.execute("SELECT * FROM replicasets ORDER BY rowid").fetchall()
    rs_list = [_rs_dict(r) for r in rs_rows]
    rs_by_id = {r["id"]: r for r in rs_list}

    pod_rows = db.execute("SELECT p.*, p.rowid AS seq FROM pods p ORDER BY p.rowid").fetchall()
    pods = [_pod_dict(p) for p in pod_rows]

    svc_rows = db.execute("SELECT * FROM services ORDER BY rowid").fetchall()
    services = [_svc_dict(s) for s in svc_rows]

    for pod in pods:
        pod["owner_name"] = rs_by_id.get(pod["rs_id"], {}).get("name", pod["rs_id"])
        pod["matched_rs_ids"] = [
            rs["id"] for rs in rs_list if matches(pod["labels"], rs["selector"])
        ]
        pod["matched_rs_names"] = [
            rs["name"] for rs in rs_list if rs["id"] in pod["matched_rs_ids"]
        ]
        pod["matched_service_ids"] = [
            svc["id"] for svc in services if matches(pod["labels"], svc["selector"])
        ]
        pod["short_uid"] = short_uid(pod["uid"], 13)

    owned_uids: dict[str, list[str]] = {rs["id"]: [] for rs in rs_list}
    owned_pods: dict[str, list[dict]] = {rs["id"]: [] for rs in rs_list}
    for pod in pods:
        owned_uids.setdefault(pod["rs_id"], []).append(pod["uid"])
        owned_pods.setdefault(pod["rs_id"], []).append(pod)

    for rs in rs_list:
        owned = owned_pods.get(rs["id"], [])
        rs["owned_count"] = len(owned)
        rs["ready_count"] = sum(1 for p in owned if p["ready"])
        rs["owned_uids"] = owned_uids.get(rs["id"], [])
        rs["selector_text"] = selector_text(rs["selector"])

    for svc in services:
        eps = [p for p in pods if p["ready"] and matches(p["labels"], svc["selector"])]
        matching_all = [p for p in pods if matches(p["labels"], svc["selector"])]
        svc["endpoints"] = eps
        svc["endpoint_uids"] = [p["uid"] for p in eps]
        svc["not_ready_matching"] = [p for p in matching_all if not p["ready"]]
        svc["selector_text"] = selector_text(svc["selector"])

    # 计划 + 实时过期判定
    plan_rows = db.execute("SELECT * FROM plans ORDER BY id DESC").fetchall()
    plans: list[dict[str, Any]] = []
    plans_by_rs: dict[str, list[dict]] = {rs["id"]: [] for rs in rs_list}
    for row in plan_rows:
        plan = dict(row)
        plan["basis_uids"] = json.loads(plan.pop("basis_uids_json"))
        plan["delete_uids"] = json.loads(plan.pop("delete_uids_json"))
        plan["delete_pods"] = [
            next((p for p in pods if p["uid"] == uid), {"uid": uid, "name": uid, "ready": False})
            for uid in plan["delete_uids"]
        ]
        plan["stale"] = False
        plan["stale_changes"] = []
        rs = rs_by_id.get(plan["rs_id"])
        if rs is not None and plan["status"] == "pending":
            stale, changes = plan_is_stale(
                basis_desired=plan["basis_desired"],
                basis_uids=plan["basis_uids"],
                current_desired=rs["desired"],
                current_uids=rs["owned_uids"],
            )
            plan["stale"] = stale
            plan["stale_changes"] = changes
        plans.append(plan)
        plans_by_rs.setdefault(plan["rs_id"], []).append(plan)

    for rs in rs_list:
        rs["plans"] = plans_by_rs.get(rs["id"], [])

    handover_rows = db.execute("SELECT * FROM handovers ORDER BY id DESC LIMIT 10").fetchall()
    handovers = []
    for row in handover_rows:
        item = dict(row)
        item["old_labels"] = json.loads(item.pop("old_labels_json"))
        item["new_labels"] = json.loads(item.pop("new_labels_json"))
        sb = item.get("source_desired_before")
        sa = item.get("source_desired_after")
        tb = item.get("target_desired_before")
        ta = item.get("target_desired_after")
        if sb is not None and sa is not None and tb is not None and ta is not None:
            # 逐笔两组期望数的“实际前→后”，模板与 API 共用。
            item["desired_pairs"] = [
                {
                    "rs_id": item.get("from_rs_id"),
                    "rs_name": item.get("from_rs_name"),
                    "before": sb,
                    "after": sa,
                },
                {
                    "rs_id": item.get("to_rs_id"),
                    "rs_name": item.get("to_rs_name"),
                    "before": tb,
                    "after": ta,
                },
            ]
            item["desired_before_after"] = {
                item.get("from_rs_id"): {"name": item.get("from_rs_name"), "before": sb, "after": sa},
                item.get("to_rs_id"): {"name": item.get("to_rs_name"), "before": tb, "after": ta},
            }
            # 常见命名别名，方便脚本/测试按不同习惯取值。
            item["desired_before"] = {item.get("from_rs_id"): sb, item.get("to_rs_id"): tb}
            item["desired_after"] = {item.get("from_rs_id"): sa, item.get("to_rs_id"): ta}
            item["old_source_desired"] = sb
            item["new_source_desired"] = sa
            item["old_target_desired"] = tb
            item["new_target_desired"] = ta
            # 与旧实现局部变量命名对齐的别名
            item["old_desired_source"] = sb
            item["new_desired_source"] = sa
            item["old_desired_target"] = tb
            item["new_desired_target"] = ta
        else:
            item["desired_pairs"] = []
        handovers.append(item)

    audit_rows = db.execute("SELECT * FROM audit_log ORDER BY id DESC LIMIT 15").fetchall()
    audit = [dict(r) for r in audit_rows]

    return {
        "replicasets": rs_list,
        "pods": pods,
        "services": services,
        "plans": plans,
        "handovers": handovers,
        "audit": audit,
        "counts": {
            "pods": len(pods),
            "ready": sum(1 for p in pods if p["ready"]),
            "service_endpoints": sum(len(s["endpoints"]) for s in services),
        },
    }


# ---------------------------------------------------------------------------
# Pod 交接
# ---------------------------------------------------------------------------

def handover(db: sqlite3.Connection, pod_uid: str, target_rs_id: str) -> dict[str, Any]:
    # 事务前读取只负责尽早给出 404，以及保留“请求已看到 Pod”的并发时序窗口；
    # 一切写入依据都必须在 BEGIN IMMEDIATE 之后重新读取、逐笔核对。
    pod_row = db.execute(
        "SELECT p.*, p.rowid AS seq FROM pods p WHERE uid = ?", (pod_uid,)
    ).fetchone()
    if pod_row is None:
        raise UseCaseError("pod_not_found", f"找不到 Pod：{pod_uid}", 404)

    target_row = db.execute("SELECT * FROM replicasets WHERE id = ?", (target_rs_id,)).fetchone()
    if target_row is None:
        raise UseCaseError("rs_not_found", f"找不到目标 ReplicaSet：{target_rs_id}", 404)

    pre_read_owner = pod_row["rs_id"]

    # 并发实测卡点（生产为 no-op）：此处已读到 Pod，但尚未取写锁/写入。
    _hook("handover:before_begin")

    denial: UseCaseError | None = None
    result: dict[str, Any] | None = None

    # 写锁串行化：同一时刻只有一笔交接（或计划执行）能进入提交段，
    # 后到者拿锁后读到的一定是前一笔已提交的最新状态。
    with immediate_transaction(db):
        fresh_pod_row = db.execute(
            "SELECT p.*, p.rowid AS seq FROM pods p WHERE uid = ?", (pod_uid,)
        ).fetchone()

        # 缩容计划抢先提交并删除了该 Pod：本笔交接必须整体不生效。
        if fresh_pod_row is None:
            message = (
                f"Pod {pod_row['name']}（{pod_uid}）已被并发执行的缩容计划删除，"
                "交接失去依据，已拒绝：两组期望副本数保持原值，"
                "也不会写入成功交接记录。"
            )
            _audit(db, False, "handover_denied", f"拒绝交接 {pod_row['name']}：Pod 已被缩容删除")
            denial = UseCaseError(
                "pod_not_found",
                message,
                409,
                {"concurrent": True, "reason": "deleted_by_scale_down", "pod_uid": pod_uid},
            )
        else:
            pod = _pod_dict(fresh_pod_row)

            target_row = db.execute(
                "SELECT * FROM replicasets WHERE id = ?", (target_rs_id,)
            ).fetchone()
            if target_row is None:
                denial = UseCaseError(
                    "rs_not_found", f"找不到目标 ReplicaSet：{target_rs_id}", 404
                )
            else:
                target = _rs_dict(target_row)
                source_row = db.execute(
                    "SELECT * FROM replicasets WHERE id = ?", (pod["rs_id"],)
                ).fetchone()
                if source_row is None:
                    raise UseCaseError(
                        "rs_not_found", f"Pod 当前归属的 ReplicaSet 不存在：{pod['rs_id']}", 409
                    )
                source = _rs_dict(source_row)

                # 规则：只能选就绪 Pod 交接（未就绪 Pod 不在 Service 名单上）。
                if not pod["ready"]:
                    message = (
                        f"Pod {pod['name']} 当前未就绪，不能作为“正在接流量的 Pod”交接。"
                        "UID、标签、归属和期望副本数均未改动。"
                    )
                    _audit(db, False, "handover_denied", f"拒绝交接 {pod['name']}：Pod 未就绪")
                    denial = UseCaseError("pod_not_ready", message, 409)

                elif source["id"] == target["id"]:
                    # 并发重复交接的落败者会在这里看到：同一 Pod 已被前一笔交给同一目标组。
                    concurrent = pre_read_owner != target["id"]
                    if concurrent:
                        message = (
                            f"Pod {pod['name']} 已在另一标签页/请求的并发交接中交给 "
                            f"{target['name']}；同一 Pod 同时交给同一目标组只能有一笔生效，"
                            "故本次重复交接被拒绝：期望数不再变动，也不新增成功交接记录。"
                        )
                        audit_msg = f"并发重复交接落败：{pod['name']} 已归属 {target['name']}"
                    else:
                        message = (
                            f"Pod {pod['name']} 已归属 {target['name']}，无需交接；状态未改动。"
                        )
                        audit_msg = message
                    _audit(db, False, "handover_denied", audit_msg)
                    denial = UseCaseError(
                        "same_replicaset",
                        message,
                        409,
                        {"concurrent": concurrent, "reason": "duplicate_handover" if concurrent else "same_replicaset"},
                    )

                # 旧组期望数要 -1；已经是 0 时不能变成负数，按“办不到”保留原状。
                elif source["desired"] <= 0:
                    message = (
                        f"旧 ReplicaSet {source['name']} 的期望副本数已经是 0，"
                        "无法按“旧组减一、新组加一”联动变更，故保留原状并拒绝交接。"
                    )
                    _audit(db, False, "handover_denied", message)
                    denial = UseCaseError("desired_floor", message, 409)

                else:
                    all_rs = [
                        _rs_dict(r)
                        for r in db.execute("SELECT * FROM replicasets").fetchall()
                    ]
                    all_services = [
                        _svc_dict(r)
                        for r in db.execute("SELECT * FROM services").fetchall()
                    ]

                    # 纯计算：无解时抛 HandoverImpossible，此时本事务尚未做任何写入。
                    try:
                        new_labels = plan_handover_labels(
                            pod["labels"],
                            source=source,
                            target=target,
                            replicasets=all_rs,
                            services=all_services,
                        )
                    except HandoverImpossible as exc:
                        _audit(
                            db, False, "handover_conflict",
                            f"标签冲突，交接 {pod['name']} -> {target['name']} 未执行："
                            f"{exc.code} {exc.message}",
                        )
                        denial = UseCaseError(exc.code, exc.message, 409)
                    else:
                        # 本笔事务内、持锁状态下读到的前后值才是“实际前后值”。
                        src_before = int(source["desired"])
                        tgt_before = int(target["desired"])

                        # 条件更新再兜底一次归属：必须仍属于本事务读到的 source。
                        cur = db.execute(
                            "UPDATE pods SET labels_json = ?, rs_id = ?"
                            " WHERE uid = ? AND rs_id = ?",
                            (
                                json.dumps(new_labels, ensure_ascii=False),
                                target["id"],
                                pod["uid"],
                                source["id"],
                            ),
                        )
                        if cur.rowcount != 1:
                            # 理论上写锁串行后不会发生；发生则整体回滚，绝不报成功。
                            raise UseCaseError(
                                "handover_conflict",
                                "Pod 归属在提交瞬间已被其它事务改变，本笔交接回滚，请刷新后重试。",
                                409,
                            )

                        cur = db.execute(
                            "UPDATE replicasets SET desired = desired - 1"
                            " WHERE id = ? AND desired > 0",
                            (source["id"],),
                        )
                        if cur.rowcount != 1:
                            raise UseCaseError(
                                "desired_floor",
                                "旧组期望数扣减失败（可能已被并发修改），已回滚。",
                                409,
                            )
                        db.execute(
                            "UPDATE replicasets SET desired = desired + 1 WHERE id = ?",
                            (target["id"],),
                        )

                        # 写后在同一事务内回读，确保记录的是实际提交值而非推算值。
                        src_after = int(db.execute(
                            "SELECT desired FROM replicasets WHERE id = ?", (source["id"],)
                        ).fetchone()["desired"])
                        tgt_after = int(db.execute(
                            "SELECT desired FROM replicasets WHERE id = ?", (target["id"],)
                        ).fetchone()["desired"])

                        change_text = (
                            f"{source['name']} desired {src_before}→{src_after}；"
                            f"{target['name']} desired {tgt_before}→{tgt_after}"
                        )
                        db.execute(
                            "INSERT INTO handovers(pod_uid, pod_name, from_rs_id, from_rs_name,"
                            " to_rs_id, to_rs_name, old_labels_json, new_labels_json,"
                            " desired_change, source_desired_before, source_desired_after,"
                            " target_desired_before, target_desired_after)"
                            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                            (
                                pod["uid"], pod["name"], source["id"], source["name"],
                                target["id"], target["name"],
                                json.dumps(pod["labels"], ensure_ascii=False),
                                json.dumps(new_labels, ensure_ascii=False),
                                change_text,
                                src_before, src_after, tgt_before, tgt_after,
                            ),
                        )
                        _audit(
                            db, True, "handover_ok",
                            f"交接成功：{pod['name']}（UID {pod['uid']}，就绪状态保留）"
                            f"{source['name']} → {target['name']}；实际期望数变化 {change_text}",
                        )

                        result = {
                            "pod_uid": pod["uid"],
                            "pod_name": pod["name"],
                            "from_rs_id": source["id"],
                            "to_rs_id": target["id"],
                            "old_labels": pod["labels"],
                            "new_labels": new_labels,
                            # 兼容旧字段：两组的“后值”
                            "source_desired": src_after,
                            "target_desired": tgt_after,
                            # 逐笔实际前后值
                            "source_desired_before": src_before,
                            "source_desired_after": src_after,
                            "target_desired_before": tgt_before,
                            "target_desired_after": tgt_after,
                            "desired_before_after": {
                                source["id"]: {
                                    "name": source["name"],
                                    "before": src_before,
                                    "after": src_after,
                                },
                                target["id"]: {
                                    "name": target["name"],
                                    "before": tgt_before,
                                    "after": tgt_after,
                                },
                            },
                            "desired_before": {
                                source["id"]: src_before, target["id"]: tgt_before,
                            },
                            "desired_after": {
                                source["id"]: src_after, target["id"]: tgt_after,
                            },
                            "old_desired_source": src_before,
                            "new_desired_source": src_after,
                            "old_desired_target": tgt_before,
                            "new_desired_target": tgt_after,
                        }

    # 正常退出 with 块时，拒绝审计（如有）随事务提交；denial 在提交后抛出。
    if denial is not None:
        raise denial
    assert result is not None
    return result


# ---------------------------------------------------------------------------
# 期望副本数
# ---------------------------------------------------------------------------

def set_desired(db: sqlite3.Connection, rs_id: str, desired: Any) -> dict[str, Any]:
    try:
        desired_int = int(desired)
    except (TypeError, ValueError):
        raise UseCaseError("bad_desired", "期望副本数必须是 0~50 的整数。", 400)
    if not 0 <= desired_int <= 50:
        raise UseCaseError("bad_desired", "期望副本数必须在 0~50 之间。", 400)

    row = db.execute("SELECT * FROM replicasets WHERE id = ?", (rs_id,)).fetchone()
    if row is None:
        raise UseCaseError("rs_not_found", f"找不到 ReplicaSet：{rs_id}", 404)
    rs = _rs_dict(row)

    with db:
        db.execute("UPDATE replicasets SET desired = ? WHERE id = ?", (desired_int, rs_id))
        _audit(
            db, True, "desired_change",
            f"{rs['name']} 期望副本数 {rs['desired']}→{desired_int}"
            "（此前生成的计划若依据已变，执行时会被退回）",
        )
    return {"rs_id": rs_id, "desired": desired_int}


# ---------------------------------------------------------------------------
# 副本增删计划
# ---------------------------------------------------------------------------

def _owned_pods(db: sqlite3.Connection, rs_id: str) -> list[dict[str, Any]]:
    rows = db.execute(
        "SELECT p.*, p.rowid AS seq FROM pods p WHERE rs_id = ? ORDER BY p.rowid",
        (rs_id,),
    ).fetchall()
    return [_pod_dict(r) for r in rows]


def generate_plan(db: sqlite3.Connection, rs_id: str) -> dict[str, Any]:
    row = db.execute("SELECT * FROM replicasets WHERE id = ?", (rs_id,)).fetchone()
    if row is None:
        raise UseCaseError("rs_not_found", f"找不到 ReplicaSet：{rs_id}", 404)
    rs = _rs_dict(row)
    owned = _owned_pods(db, rs_id)

    add_count = max(0, rs["desired"] - len(owned))
    delete_uids = choose_delete_uids(owned, max(0, len(owned) - rs["desired"]))
    delete_pods = [p for p in owned if p["uid"] in set(delete_uids)]
    delete_names = [p["name"] for p in delete_pods]

    if add_count == 0 and not delete_uids:
        summary = f"无需增删：期望 {rs['desired']} = 现有归属 {len(owned)}"
    else:
        bits = []
        if add_count:
            bits.append(f"新增 {add_count} 个 Pod（标签 {selector_text(rs['selector'])}，就绪）")
        if delete_uids:
            bits.append("删除 " + "、".join(delete_names))
        summary = "；".join(bits)

    cur = db.execute(
        "INSERT INTO plans(rs_id, status, basis_desired, basis_uids_json, add_count,"
        " delete_uids_json, summary) VALUES (?, 'pending', ?, ?, ?, ?, ?)",
        (
            rs_id,
            rs["desired"],
            json.dumps(sorted(p["uid"] for p in owned)),
            add_count,
            json.dumps(delete_uids),
            summary,
        ),
    )
    plan_id = cur.lastrowid
    _audit(
        db, True, "plan_generated",
        f"为 {rs['name']} 生成计划 #{plan_id}：{summary}"
        f"（依据：期望 {rs['desired']}，归属 {len(owned)} 个 UID）",
    )
    db.commit()
    return {"plan_id": plan_id, "summary": summary, "add_count": add_count,
            "delete_uids": delete_uids}


def execute_plan(db: sqlite3.Connection, plan_id: int) -> dict[str, Any]:
    row = db.execute("SELECT * FROM plans WHERE id = ?", (plan_id,)).fetchone()
    if row is None:
        raise UseCaseError("plan_not_found", f"找不到计划 #{plan_id}", 404)
    plan = dict(row)

    if plan["status"] == "executed":
        raise UseCaseError(
            "plan_already_executed", f"计划 #{plan_id} 已经执行过，不能重复执行。", 409
        )
    if plan["status"] == "returned":
        raise UseCaseError(
            "plan_returned",
            f"计划 #{plan_id} 已被退回，请按当前期望副本数和现有归属重新生成计划。",
            409,
        )

    def _current_basis() -> tuple[dict[str, Any], list[dict[str, Any]], list[str], bool, list[str]]:
        rs_row = db.execute("SELECT * FROM replicasets WHERE id = ?", (plan["rs_id"],)).fetchone()
        if rs_row is None:
            raise UseCaseError("rs_not_found", f"找不到 ReplicaSet：{plan['rs_id']}", 404)
        rs = _rs_dict(rs_row)
        owned = _owned_pods(db, rs["id"])
        current_uids = sorted(p["uid"] for p in owned)
        basis_uids = sorted(json.loads(plan["basis_uids_json"]))
        stale, changes = plan_is_stale(
            basis_desired=plan["basis_desired"],
            basis_uids=basis_uids,
            current_desired=rs["desired"],
            current_uids=current_uids,
        )
        return rs, owned, basis_uids, stale, changes

    # 事务前预检：多数“页面上已经能看出过期”的请求在这里直接退回。
    rs, _owned, _basis_uids, stale, changes = _current_basis()
    if stale:
        reason = (
            f"计划 #{plan_id} 生成后，{rs['name']} 的期望副本数或 Pod 归属已改变，"
            f"情况已变化（{'；'.join(changes)}）。为避免误增删，旧计划已退回，"
            "请按当前期望数和现有归属重新生成计划后再执行。"
        )
        with immediate_transaction(db):
            db.execute(
                "UPDATE plans SET status = 'returned', stale_reason = ?,"
                " executed_at = datetime('now') WHERE id = ? AND status = 'pending'",
                (reason, plan_id),
            )
            _audit(db, False, "plan_returned", reason)
        raise UseCaseError("stale_plan", reason, 409, {"plan_id": plan_id, "changes": changes})

    # 并发实测卡点：预检已通过、写事务尚未开始。此处若放行一个交接，
    # 必须靠事务内复核把计划退回，而不能继续按旧名单删除。
    _hook("plan:after_precheck")

    delete_uids = json.loads(plan["delete_uids_json"])
    add_count = int(plan["add_count"])

    stale_error: UseCaseError | None = None
    done_result: dict[str, Any] | None = None

    with immediate_transaction(db):
        # 拿写锁后重新读取计划状态与全部依据，按实际生效顺序做最终核对。
        fresh_plan_row = db.execute("SELECT * FROM plans WHERE id = ?", (plan_id,)).fetchone()
        if fresh_plan_row is None:
            raise UseCaseError("plan_not_found", f"找不到计划 #{plan_id}", 404)
        fresh_status = fresh_plan_row["status"]
        if fresh_status == "executed":
            raise UseCaseError(
                "plan_already_executed", f"计划 #{plan_id} 已经执行过，不能重复执行。", 409
            )
        if fresh_status == "returned":
            raise UseCaseError(
                "plan_returned",
                f"计划 #{plan_id} 已被退回，请按当前期望副本数和现有归属重新生成计划。",
                409,
            )

        rs, _owned, _basis_uids, stale, changes = _current_basis()
        if stale:
            # 典型交错：预检后交接先提交。失配计划在同一事务内置 returned，
            # 不增删任何 Pod，提交后抛出 stale_plan。
            reason = (
                f"计划 #{plan_id} 生成后，{rs['name']} 的期望副本数或 Pod 归属已改变，"
                f"情况已变化（{'；'.join(changes)}）。为避免误增删，旧计划已退回，"
                "请按当前期望数和现有归属重新生成计划后再执行。"
            )
            db.execute(
                "UPDATE plans SET status = 'returned', stale_reason = ?,"
                " executed_at = datetime('now') WHERE id = ?",
                (reason, plan_id),
            )
            _audit(db, False, "plan_returned", reason)
            stale_error = UseCaseError(
                "stale_plan", reason, 409, {"plan_id": plan_id, "changes": changes}
            )
        else:
            created_names: list[str] = []

            # 扩容：按 RS 选择器生成最小标签集的新 Pod（演练中直接置为就绪）。
            for _ in range(add_count):
                uid = str(uuid.uuid4())
                name = f"{rs['name']}-{uuid.uuid4().hex[:5]}"
                db.execute(
                    "INSERT INTO pods(uid, name, labels_json, ready, rs_id)"
                    " VALUES (?,?,?,1,?)",
                    (uid, name, json.dumps(rs["selector"], ensure_ascii=False), rs["id"]),
                )
                created_names.append(name)

            for uid in delete_uids:
                cur = db.execute(
                    "DELETE FROM pods WHERE uid = ? AND rs_id = ?", (uid, rs["id"])
                )
                if cur.rowcount != 1:
                    # 依据复核通过且写锁串行，正常不会走到；走到就整体回滚。
                    raise UseCaseError(
                        "plan_delete_failed",
                        f"计划中的 Pod {uid} 已不属于 {rs['name']}，执行中止，事务回滚。",
                        409,
                    )

            result_parts = []
            if created_names:
                result_parts.append("已新增 " + "、".join(created_names))
            if delete_uids:
                result_parts.append(f"已删除 {len(delete_uids)} 个 Pod")
            if not result_parts:
                result_parts.append("计划为空操作，副本数已一致")
            result = "；".join(result_parts)

            db.execute(
                "UPDATE plans SET status = 'executed', result = ?, executed_at = datetime('now')"
                " WHERE id = ?",
                (result, plan_id),
            )
            _audit(db, True, "plan_executed", f"计划 #{plan_id}（{rs['name']}）执行完成：{result}")
            done_result = {
                "plan_id": plan_id,
                "result": result,
                "created_names": created_names,
                "deleted_uids": delete_uids,
            }

    if stale_error is not None:
        raise stale_error
    assert done_result is not None
    return done_result


def discard_plan(db: sqlite3.Connection, plan_id: int) -> None:
    row = db.execute("SELECT * FROM plans WHERE id = ?", (plan_id,)).fetchone()
    if row is None:
        raise UseCaseError("plan_not_found", f"找不到计划 #{plan_id}", 404)
    with db:
        db.execute("DELETE FROM plans WHERE id = ? AND status = 'pending'", (plan_id,))
        _audit(db, True, "plan_discarded", f"待执行计划 #{plan_id} 已丢弃")


# ---------------------------------------------------------------------------
# 冲突演练用 RS
# ---------------------------------------------------------------------------

CONFLICT_PRESETS: dict[str, dict[str, str]] = {
    "app_key": {
        "id": CONFLICT_APP_RS,
        "name": "web-canary-conflict",
        "selector_json": json.dumps({"app": "canary"}, ensure_ascii=False),
        "note": "演练用标签冲突：选择器 app=canary 与 shop-svc 的 app=shop 无法同时满足",
    },
    "overlap": {
        "id": CONFLICT_OVERLAP_RS,
        "name": "web-v1-copy",
        "selector_json": json.dumps({"app": "shop", "release": "blue"}, ensure_ascii=False),
        "note": "演练用选择器重叠：与 web-v1 完全相同，交接后无法做到只命中新组",
    },
}


def create_conflict_rs(db: sqlite3.Connection, kind: str) -> dict[str, str]:
    preset = CONFLICT_PRESETS.get(kind)
    if preset is None:
        raise UseCaseError("bad_conflict_kind", f"未知冲突演练类型：{kind}", 400)
    db.execute(
        "INSERT OR IGNORE INTO replicasets(id, name, selector_json, desired, drill, note)"
        " VALUES (?,?,?,0,1,?)",
        (preset["id"], preset["name"], preset["selector_json"], preset["note"]),
    )
    db.commit()
    _audit(db, True, "drill_setup", f"已构造冲突演练 ReplicaSet：{preset['name']}")
    db.commit()
    return {"id": preset["id"], "name": preset["name"]}


def remove_drill_rs(db: sqlite3.Connection, rs_id: str) -> None:
    row = db.execute("SELECT * FROM replicasets WHERE id = ?", (rs_id,)).fetchone()
    if row is None:
        raise UseCaseError("rs_not_found", f"找不到 ReplicaSet：{rs_id}", 404)
    rs = _rs_dict(row)
    if not rs["drill"]:
        raise UseCaseError("not_drill_rs", "只能移除演练用 ReplicaSet。", 400)
    count = db.execute(
        "SELECT COUNT(*) AS c FROM pods WHERE rs_id = ?", (rs_id,)
    ).fetchone()["c"]
    if count:
        raise UseCaseError(
            "drill_rs_not_empty",
            f"{rs['name']} 仍有 {count} 个归属 Pod，请先处理后再移除（或直接重置样例）。",
            409,
        )
    with db:
        db.execute("DELETE FROM plans WHERE rs_id = ?", (rs_id,))
        db.execute("DELETE FROM replicasets WHERE id = ?", (rs_id,))
        _audit(db, True, "drill_teardown", f"已移除演练 ReplicaSet：{rs['name']}")
