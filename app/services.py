"""用例层：交接、期望数调整、副本计划生成/执行、页面快照。

Web 路由和 JSON API 都调用这里的函数，保证两条入口行为一致。
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from typing import Any

from .db import (
    CONFLICT_APP_RS,
    CONFLICT_OVERLAP_RS,
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
    pod_row = db.execute(
        "SELECT p.*, p.rowid AS seq FROM pods p WHERE uid = ?", (pod_uid,)
    ).fetchone()
    if pod_row is None:
        raise UseCaseError("pod_not_found", f"找不到 Pod：{pod_uid}", 404)
    pod = _pod_dict(pod_row)

    target_row = db.execute("SELECT * FROM replicasets WHERE id = ?", (target_rs_id,)).fetchone()
    if target_row is None:
        raise UseCaseError("rs_not_found", f"找不到目标 ReplicaSet：{target_rs_id}", 404)
    target = _rs_dict(target_row)

    source_row = db.execute(
        "SELECT * FROM replicasets WHERE id = ?", (pod["rs_id"],)
    ).fetchone()
    source = _rs_dict(source_row)

    # 规则：只能选就绪 Pod 交接（未就绪 Pod 不在 Service 名单上，也不符合演练前提）。
    if not pod["ready"]:
        message = (
            f"Pod {pod['name']} 当前未就绪，不能作为“正在接流量的 Pod”交接。"
            "UID、标签、归属和期望副本数均未改动。"
        )
        _audit(db, False, "handover_denied", f"拒绝交接 {pod['name']}：Pod 未就绪")
        db.commit()
        raise UseCaseError("pod_not_ready", message, 409)

    if source["id"] == target["id"]:
        message = f"Pod {pod['name']} 已归属 {target['name']}，无需交接；状态未改动。"
        _audit(db, False, "handover_denied", message)
        db.commit()
        raise UseCaseError("same_replicaset", message, 409)

    # 旧组期望数要 -1；已经是 0 时不能变成负数，按“办不到”保留原状。
    if source["desired"] <= 0:
        message = (
            f"旧 ReplicaSet {source['name']} 的期望副本数已经是 0，"
            "无法按“旧组减一、新组加一”联动变更，故保留原状并拒绝交接。"
        )
        _audit(db, False, "handover_denied", message)
        db.commit()
        raise UseCaseError("desired_floor", message, 409)

    all_rs = [_rs_dict(r) for r in db.execute("SELECT * FROM replicasets").fetchall()]
    all_services = [_svc_dict(r) for r in db.execute("SELECT * FROM services").fetchall()]

    # 纯计算：无解时抛 HandoverImpossible，此时尚未做任何写入。
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
            f"标签冲突，交接 {pod['name']} -> {target['name']} 未执行：{exc.code} {exc.message}",
        )
        db.commit()
        raise UseCaseError(exc.code, exc.message, 409) from exc

    old_desired_source = source["desired"]
    old_desired_target = target["desired"]

    # 归属 + 两边期望数 + 标签在同一个事务里联动提交。UID/ready 两列完全不更新。
    with db:
        db.execute(
            "UPDATE pods SET labels_json = ?, rs_id = ? WHERE uid = ?",
            (json.dumps(new_labels, ensure_ascii=False), target["id"], pod["uid"]),
        )
        cur = db.execute(
            "UPDATE replicasets SET desired = desired - 1 WHERE id = ? AND desired > 0",
            (source["id"],),
        )
        if cur.rowcount != 1:
            raise UseCaseError("desired_floor", "旧组期望数扣减失败（可能已被并发修改），已回滚。", 409)
        db.execute(
            "UPDATE replicasets SET desired = desired + 1 WHERE id = ?",
            (target["id"],),
        )
        change_text = (
            f"{source['name']} desired {old_desired_source}→{old_desired_source - 1}；"
            f"{target['name']} desired {old_desired_target}→{old_desired_target + 1}"
        )
        db.execute(
            "INSERT INTO handovers(pod_uid, pod_name, from_rs_id, from_rs_name,"
            " to_rs_id, to_rs_name, old_labels_json, new_labels_json, desired_change)"
            " VALUES (?,?,?,?,?,?,?,?,?)",
            (
                pod["uid"], pod["name"], source["id"], source["name"],
                target["id"], target["name"],
                json.dumps(pod["labels"], ensure_ascii=False),
                json.dumps(new_labels, ensure_ascii=False),
                change_text,
            ),
        )
        _audit(
            db, True, "handover_ok",
            f"交接成功：{pod['name']}（UID {pod['uid']}，就绪状态保留）"
            f"{source['name']} → {target['name']}；{change_text}",
        )

    return {
        "pod_uid": pod["uid"],
        "pod_name": pod["name"],
        "from_rs_id": source["id"],
        "to_rs_id": target["id"],
        "old_labels": pod["labels"],
        "new_labels": new_labels,
        "source_desired": old_desired_source - 1,
        "target_desired": old_desired_target + 1,
    }


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

    rs_row = db.execute("SELECT * FROM replicasets WHERE id = ?", (plan["rs_id"],)).fetchone()
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
    if stale:
        reason = (
            f"计划 #{plan_id} 生成后，{rs['name']} 的期望副本数或 Pod 归属已改变，"
            f"情况已变化（{'；'.join(changes)}）。为避免误增删，旧计划已退回，"
            "请按当前期望数和现有归属重新生成计划后再执行。"
        )
        with db:
            db.execute(
                "UPDATE plans SET status = 'returned', stale_reason = ?, executed_at = datetime('now')"
                " WHERE id = ?",
                (reason, plan_id),
            )
            _audit(db, False, "plan_returned", reason)
        raise UseCaseError("stale_plan", reason, 409, {"plan_id": plan_id, "changes": changes})

    delete_uids = json.loads(plan["delete_uids_json"])
    add_count = int(plan["add_count"])
    created_names: list[str] = []

    with db:
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

    return {"plan_id": plan_id, "result": result,
            "created_names": created_names, "deleted_uids": delete_uids}


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
