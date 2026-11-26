"""纯领域逻辑：等值标签选择器、命中判定、交接标签规划、副本计划核对。

本模块不依赖 Flask / SQLite，方便单独测试。
这里的选择器只支持 Kubernetes 的 equality-based（等值）要求：
``{"app": "shop", "release": "blue"}`` 表示 ``app=shop AND release=blue``。
"""

from __future__ import annotations

from typing import Any, Mapping, Sequence

Labels = dict[str, str]
Selector = dict[str, str]


class HandoverImpossible(Exception):
    """标签层面无法完成“保 Service、只命中新 RS”的交接。"""

    def __init__(self, code: str, message: str):
        self.code = code
        self.message = message
        super().__init__(message)


def matches(labels: Mapping[str, str] | None, selector: Mapping[str, str] | None) -> bool:
    """Pod 标签是否满足等值选择器（所有 key=value 同时成立）。"""
    labels = labels or {}
    selector = selector or {}
    return all(labels.get(key) == value for key, value in selector.items())


def selector_text(selector: Mapping[str, str] | None) -> str:
    if not selector:
        return "（空选择器，命中全部 Pod）"
    return ", ".join(f"{key}={value}" for key, value in selector.items())


def plan_handover_labels(
    pod_labels: Mapping[str, str],
    *,
    source: Mapping[str, Any],
    target: Mapping[str, Any],
    replicasets: Sequence[Mapping[str, Any]],
    services: Sequence[Mapping[str, Any]],
) -> Labels:
    """计算交接后的新标签。

    约束（按演练要求）：
    1. 新标签必须命中 ``target`` ReplicaSet；
    2. 交接前命中的每一个 Service（“原 Service”）交接后仍须命中；
    3. 在所有 ReplicaSet 中只命中 ``target``。

    做法：先写入目标选择器要求的键值；Service 选择器固定的键不允许被改坏；
    随后逐个解除对其它 ReplicaSet 的命中，优先删除未被目标/Service 锁定的键。
    如果某个多余命中的选择器键全部被锁定、无法解除，则判定为标签冲突并报错，
    调用方必须保证此时不写入任何变更。
    """
    if source["id"] == target["id"]:
        raise HandoverImpossible("same_replicaset", "该 Pod 已归属目标 ReplicaSet，无需交接。")

    labels: Labels = dict(pod_labels)

    # 交接前该 Pod 命中的 Service，交接后必须继续命中（保留流量）。
    kept_services = [svc for svc in services if matches(labels, svc["selector"])]

    # 1) 满足目标 ReplicaSet 的等值选择器（直接对齐键值）。
    for key, value in target["selector"].items():
        labels[key] = value

    # 目标选择器与 Service 选择器对同一个键给出不同值 => 无解的标签冲突。
    for svc in kept_services:
        for key, wanted in svc["selector"].items():
            if labels.get(key) != wanted:
                target_value = target["selector"].get(key)
                if target_value is not None and target_value != wanted:
                    detail = (
                        f"Service {svc['name']} 要求 {key}={wanted!r}，"
                        f"目标 ReplicaSet {target['name']} 要求 {key}={target_value!r}"
                    )
                else:
                    detail = f"调整标签后无法继续满足 Service {svc['name']} 的 {key}={wanted!r}"
                raise HandoverImpossible(
                    "label_conflict",
                    "标签冲突："
                    + detail
                    + "，两组等值要求无法同时成立。"
                    "若强行对齐新控制器，Pod 会掉出原 Service 名单；因此保留原状。",
                )

    # 被目标选择器或必须保留的 Service 锁定的键，不能删除/改值。
    pinned: set[str] = set(target["selector"].keys())
    for svc in kept_services:
        pinned.update(svc["selector"].keys())

    # 2) 解除对其它 ReplicaSet 的命中，确保“只命中新 ReplicaSet”。
    others = [rs for rs in replicasets if rs["id"] != target["id"]]
    # 先处理旧组，报错信息更贴近值班场景。
    others.sort(key=lambda rs: (rs["id"] != source["id"], rs.get("name", "")))

    while True:
        hit = next((rs for rs in others if matches(labels, rs["selector"])), None)
        if hit is None:
            break

        breakable = [key for key in hit["selector"].keys() if key not in pinned]
        if not breakable:
            raise HandoverImpossible(
                "selector_overlap",
                "标签冲突：对齐目标选择器后，Pod 仍会命中 ReplicaSet "
                f"{hit['name']}（其选择器 {selector_text(hit['selector'])}），"
                "而该选择器的每个标签键都被目标选择器或原 Service 选择器锁定，"
                "无法在不破坏“命中新 ReplicaSet / 留在原 Service”的前提下解除命中。"
                "因此无法做到只命中新 ReplicaSet，已保留原状。",
            )

        # 命中中的选择器，删掉其中一个未锁定的键即可解除命中。
        labels.pop(breakable[0], None)

    # 3) 最终校验（防御性，正常路径下一定通过）。
    if not matches(labels, target["selector"]):
        raise HandoverImpossible(
            "label_conflict", "内部校验失败：新标签未命中目标 ReplicaSet，已保留原状。"
        )
    extra_rs = [
        rs["name"]
        for rs in replicasets
        if rs["id"] != target["id"] and matches(labels, rs["selector"])
    ]
    if extra_rs:
        raise HandoverImpossible(
            "selector_overlap",
            "标签冲突：交接后仍会命中其它 ReplicaSet：" + "、".join(extra_rs) + "。已保留原状。",
        )
    lost_svc = [svc["name"] for svc in kept_services if not matches(labels, svc["selector"])]
    if lost_svc:
        raise HandoverImpossible(
            "label_conflict",
            "标签冲突：交接后 Pod 会掉出原 Service：" + "、".join(lost_svc) + "。已保留原状。",
        )

    return labels


def choose_delete_uids(owned_pods: Sequence[Mapping[str, Any]], delete_count: int) -> list[str]:
    """缩容时给出确定的删除顺序：未就绪优先，其次按入库顺序（rowid/seq）。"""
    ordered = sorted(
        owned_pods,
        key=lambda pod: (1 if pod["ready"] else 0, pod.get("seq", 0), pod["name"]),
    )
    return [pod["uid"] for pod in ordered[: max(0, delete_count)]]


def plan_is_stale(
    *,
    basis_desired: int,
    basis_uids: Sequence[str],
    current_desired: int,
    current_uids: Sequence[str],
) -> tuple[bool, list[str]]:
    """计划依据（期望数 + 生成时的归属 UID 集合）是否已变化。

    返回 (是否过期, 变化说明列表)。这是“交接后旧计划必须退回重算”的判定核心。
    """
    changes: list[str] = []
    if int(basis_desired) != int(current_desired):
        changes.append(f"期望副本数 {basis_desired} → {current_desired}")

    old_set = set(basis_uids)
    new_set = set(current_uids)
    if old_set != new_set:
        removed = sorted(old_set - new_set)
        added = sorted(new_set - old_set)
        parts = []
        if removed:
            parts.append("移走 " + "、".join(short_uid(u) for u in removed))
        if added:
            parts.append("新增 " + "、".join(short_uid(u) for u in added))
        changes.append("Pod 归属集合变化（" + "；".join(parts) + "）")

    return bool(changes), changes


def short_uid(uid: str, length: int = 8) -> str:
    return uid if len(uid) <= length else uid[:length] + "…"
