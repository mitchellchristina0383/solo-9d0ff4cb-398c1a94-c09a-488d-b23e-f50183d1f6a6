"""SQLite 连接、建表与样例数据。"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

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
    -- 逐笔记录两组期望数在本笔事务提交时的“实际前后值”（并发下不得用请求开始时的快照值）
    source_desired_before INTEGER,
    source_desired_after  INTEGER,
    target_desired_before INTEGER,
    target_desired_after  INTEGER,
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
    # timeout 与 busy_timeout 双保险：并发写时在锁上等待，而不是立刻 database is locked。
    db = sqlite3.connect(str(path), timeout=30.0)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA foreign_keys = ON")
    db.execute("PRAGMA busy_timeout = 30000")
    return db


@contextmanager
def immediate_transaction(db: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """并发写用的串行化事务。

    ``BEGIN IMMEDIATE`` 在事务一开始就取 SQLite 写锁：所有“交接 / 计划执行”
    因此按 BEGIN 成功的顺序逐笔生效，后到者拿到锁后必须重新读取最新状态再核对条件，
    从而避免基于旧快照做判断和写入。

    保持 sqlite3 默认的延迟事务控制（isolation_level=""）。若调用方连接上已经有
    未提交事务（``in_transaction`` 为真，例如测试在同一连接上先做了未提交 DML），
    则不重复下发 BEGIN，也不替调用方 COMMIT/ROLLBACK，只加入其既有事务。
    """
    started_here = not db.in_transaction
    if started_here:
        db.execute("BEGIN IMMEDIATE")
    try:
        yield db
    except BaseException:
        if started_here:
            db.rollback()
        raise
    else:
        if started_here:
            db.commit()


# 老版本数据库（含旧命名卷里的 drill.db）平滑补列。
_COLUMN_MIGRATIONS: dict[str, dict[str, str]] = {
    "handovers": {
        "source_desired_before": "INTEGER",
        "source_desired_after": "INTEGER",
        "target_desired_before": "INTEGER",
        "target_desired_after": "INTEGER",
    },
}


def _migrate_schema(db: sqlite3.Connection) -> None:
    for table, columns in _COLUMN_MIGRATIONS.items():
        existing = {row["name"] for row in db.execute(f"PRAGMA table_info({table})").fetchall()}
        if not existing:
            continue  # 表尚不存在：SCHEMA 建表时已含新列
        for name, decl in columns.items():
            if name not in existing:
                db.execute(f"ALTER TABLE {table} ADD COLUMN {name} {decl}")
    db.commit()


def init_schema(db: sqlite3.Connection) -> None:
    db.executescript(SCHEMA)
    _migrate_schema(db)
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
