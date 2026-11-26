"""SQLite 连接、建表与样例数据。"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS replicasets (
    id            TEXT PRIMARY KEY,
    name          TEXT NOT NULL UNIQUE,
    selector_json TEXT NOT NULL,
    desired       INTEGER NOT NULL CHECK (desired >= 0),
    drill         INTEGER NOT NULL DEFAULT 0,
    note          TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS pods (
    uid         TEXT PRIMARY KEY,          -- 交接前后 UID 不变
    name        TEXT NOT NULL UNIQUE,
    labels_json TEXT NOT NULL,
    ready       INTEGER NOT NULL,          -- 就绪状态，交接时保留
    rs_id       TEXT NOT NULL REFERENCES replicasets(id) ON DELETE RESTRICT,
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS services (
    id            TEXT PRIMARY KEY,
    name          TEXT NOT NULL UNIQUE,
    selector_json TEXT NOT NULL,
    note          TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS handovers (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    pod_uid          TEXT NOT NULL,
    pod_name         TEXT NOT NULL,
    from_rs_id       TEXT,
    from_rs_name     TEXT NOT NULL DEFAULT '',
    to_rs_id         TEXT,
    to_rs_name       TEXT NOT NULL DEFAULT '',
    old_labels_json  TEXT NOT NULL DEFAULT '{}',
    new_labels_json  TEXT NOT NULL DEFAULT '{}',
    desired_change   TEXT NOT NULL DEFAULT '',
    created_at       TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS plans (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    rs_id            TEXT NOT NULL REFERENCES replicasets(id) ON DELETE CASCADE,
    status           TEXT NOT NULL DEFAULT 'pending', -- pending | executed | returned
    basis_desired    INTEGER NOT NULL,
    basis_uids_json  TEXT NOT NULL DEFAULT '[]',
    add_count        INTEGER NOT NULL DEFAULT 0,
    delete_uids_json TEXT NOT NULL DEFAULT '[]',
    summary          TEXT NOT NULL DEFAULT '',
    stale_reason     TEXT NOT NULL DEFAULT '',
    result           TEXT NOT NULL DEFAULT '',
    created_at       TEXT NOT NULL DEFAULT (datetime('now')),
    executed_at      TEXT
);

CREATE TABLE IF NOT EXISTS audit_log (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    ok         INTEGER NOT NULL DEFAULT 1,
    kind       TEXT NOT NULL,
    message    TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
"""

# --- 固定 ID，方便测试与演练脚本引用 ---------------------------------------

RS_OLD = "rs-v1"
RS_NEW = "rs-v2"
SVC_SHOP = "svc-shop"

UID_V1_READY_1 = "10000000-0000-4000-8000-000000000001"
UID_V1_READY_2 = "10000000-0000-4000-8000-000000000002"
UID_V1_NOTREADY_3 = "10000000-0000-4000-8000-000000000003"
UID_V2_READY_1 = "20000000-0000-4000-8000-000000000004"
UID_V2_READY_2 = "20000000-0000-4000-8000-000000000005"

CONFLICT_APP_RS = "rs-conflict-app"
CONFLICT_OVERLAP_RS = "rs-conflict-overlap"


def connect(db_path: str | Path) -> sqlite3.Connection:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(str(path))
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA foreign_keys = ON")
    return db


def init_schema(db: sqlite3.Connection) -> None:
    db.executescript(SCHEMA)
    db.commit()


def _seed_data(db: sqlite3.Connection) -> None:
    """写入固定样例。

    web-v1 故意设成期望 4 / 现有归属 3，使“生成增容计划”一步即可看到动作；
    交接 1 个就绪 Pod 后变成期望 3 / 归属 2，旧计划依据失效，必须重算。
    """
    replicasets = [
        (
            RS_OLD,
            "web-v1",
            {"app": "shop", "release": "blue"},
            4,
            0,
            "旧 ReplicaSet（蓝组），值班员要把正在接流量的 Pod 移交给新控制器",
        ),
        (
            RS_NEW,
            "web-v2",
            {"app": "shop", "release": "green"},
            2,
            0,
            "新 ReplicaSet（绿组），交接目标",
        ),
    ]
    db.executemany(
        "INSERT INTO replicasets(id, name, selector_json, desired, drill, note)"
        " VALUES (?,?,?,?,?,?)",
        [(rid, name, json.dumps(sel, ensure_ascii=False), desired, drill, note)
         for rid, name, sel, desired, drill, note in replicasets],
    )

    pods = [
        ("web-v1-a1", UID_V1_READY_1,
         {"app": "shop", "release": "blue", "zone": "a"}, 1, RS_OLD),
        ("web-v1-b2", UID_V1_READY_2,
         {"app": "shop", "release": "blue", "zone": "b"}, 1, RS_OLD),
        ("web-v1-c3", UID_V1_NOTREADY_3,
         {"app": "shop", "release": "blue", "zone": "a"}, 0, RS_OLD),
        ("web-v2-d4", UID_V2_READY_1,
         {"app": "shop", "release": "green", "zone": "b"}, 1, RS_NEW),
        ("web-v2-e5", UID_V2_READY_2,
         {"app": "shop", "release": "green", "zone": "a"}, 1, RS_NEW),
    ]
    db.executemany(
        "INSERT INTO pods(name, uid, labels_json, ready, rs_id) VALUES (?,?,?,?,?)",
        [(name, uid, json.dumps(labels, ensure_ascii=False), ready, rs_id)
         for name, uid, labels, ready, rs_id in pods],
    )

    db.execute(
        "INSERT INTO services(id, name, selector_json, note) VALUES (?,?,?,?)",
        (
            SVC_SHOP,
            "shop-svc",
            json.dumps({"app": "shop"}, ensure_ascii=False),
            "原 Service：只用等值标签 app=shop 选择 Pod，就绪 Pod 才进入 endpoints 名单",
        ),
    )

    db.execute(
        "INSERT INTO audit_log(ok, kind, message) VALUES (1, 'seed', ?)",
        ("已载入初始样例：2 个 ReplicaSet、5 个 Pod（4 个就绪）、1 个 Service",),
    )
    db.commit()


def seed_if_empty(db: sqlite3.Connection) -> bool:
    count = db.execute("SELECT COUNT(*) AS c FROM replicasets").fetchone()["c"]
    if count:
        return False
    _seed_data(db)
    return True


def reset_to_seed(db: sqlite3.Connection) -> None:
    """清空全部演练数据并恢复初始样例（供页面“重置”和测试使用）。"""
    db.executescript(
        """
        DROP TABLE IF EXISTS audit_log;
        DROP TABLE IF EXISTS plans;
        DROP TABLE IF EXISTS handovers;
        DROP TABLE IF EXISTS services;
        DROP TABLE IF EXISTS pods;
        DROP TABLE IF EXISTS replicasets;
        """
    )
    init_schema(db)
    _seed_data(db)
