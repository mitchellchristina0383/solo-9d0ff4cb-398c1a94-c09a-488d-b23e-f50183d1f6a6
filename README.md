# ReplicaSet 换组交接演练台（Flask + SQLite）

本地演练“换 ReplicaSet 时，把正在接流量的就绪 Pod 安全交给新控制器”的小页面。
选择器**只使用等值标签**（`key=value`，多个条件 AND）。

## 启动（Docker Compose）

```bash
docker compose up --build
# 打开 http://localhost:5000
```

- SQLite 数据放在命名卷 `drill-data`（容器内 `/data/drill.db`），重启不丢。
- 想回到完全初始状态：页面右上角“↺ 重置样例”，或 `docker compose down -v` 后重新 `up`。
- 健康检查：`GET /api/health`。

不想用 Docker 时也可以本地直接跑：

```bash
python3 -m pip install -r requirements.txt
python3 wsgi.py                 # 或：gunicorn --bind 127.0.0.1:5000 wsgi:application
```

## 初始样例

| 对象 | 选择器（等值） | 期望 | 归属 Pod |
| --- | --- | --- | --- |
| ReplicaSet `web-v1`（旧/蓝） | `app=shop, release=blue` | **4** | 3 个（2 Ready，1 NotReady） |
| ReplicaSet `web-v2`（新/绿） | `app=shop, release=green` | 2 | 2 个（均 Ready） |
| Service `shop-svc` | `app=shop` | — | 就绪 endpoints = 4 |

> 初始特意让 `web-v1` desired=4 / owned=3，方便直接生成 `+1` 计划并演练“交接后旧计划失效”。

## 页面能做什么

1. **Service 就绪名单**：只列出“选择器命中且 Ready”的 Pod（UID、归属、标签），并附带“命中但未就绪”列表。
2. **Pod 交接**：单选一个 Ready Pod + 目标 ReplicaSet。同一事务内：
   - Pod 行原地更新：**UID 不变、Ready 状态不变**，只改标签与 `rs_id` 归属；
   - 重算标签，保证**仍命中原 `shop-svc`**，且在所有 RS 中**只命中目标 RS**；
   - `web-v1.desired − 1`、目标 RS `desired + 1`；
   - 任何标签冲突 / 未就绪 / 期望数触底等“办不到”情况：**全部不写入**，页面红字说明原因。
3. **副本增删计划**：按某 RS 的当前 `desired` 与归属 UID 集合生成 `+N` / `-N`（缩容优先删未就绪 Pod），计划保存为 `pending`，可稍后执行。
   - 计划保存依据 `basis_desired` + `basis_uids`（排序后的归属 UID 快照）。
   - 页面实时比对依据，过期计划显示“⚠ 情况已变化，请重新生成计划”。
   - 此时若仍点击“执行旧计划（应被退回）”，后端在执行瞬间再次校验，不匹配则把计划置为 **`returned`（已退回）**、不增删任何 Pod，并提示重新生成。
4. **冲突演练**：一键构造
   - `web-canary-conflict`（选择器 `app=canary`，与 Service 的 `app=shop` 同键冲突，无解）；
   - `web-v1-copy`（与 `web-v1` 选择器完全重叠，无法做到“只命中新 RS”）。
5. **交接记录 + 审计日志**：保留每次成功 / 拒绝 / 退回的原因。

## 推荐演练脚本（页面点击路径）

1. 在 ③ 给 `web-v1` “生成增删计划”：得到 `+1` 计划 #1（先不要执行）。
2. 在 ④ 点“构造键值冲突”，回到 ② 选择 `web-v1-a1`，目标选 `web-canary-conflict`，交接 → 红字报 `label_conflict`；刷新可见 UID、标签、归属、desired 全部未变。
3. （可选）构造“选择器重叠”，把某个 Ready Pod 交给 `web-v1-copy` → 报 `selector_overlap`，状态不变。
4. 在 ② 选择 `web-v1-a1`，目标 `web-v2`，正式交接 → 成功；Service 就绪名单仍是 4 个 UID（`web-v1-a1` 还在），但归属变 `web-v2`，desired 变为 3/3。
5. 回到 ③，计划 #1 已显示“情况已变化（期望 4→3；归属集合变化）”。点“执行旧计划（应被退回）”→ 计划变“已退回”，顶部红字提示重算，Pod 总数仍为 5。
6. 点“重新生成计划”：desired=3 / owned=2，仍是 `+1`，执行 → 新增 1 个 Ready Pod，总数 6。

也可以直接跑自动化冒烟（服务启动后，另开终端）：

```bash
python3 scripts/smoke_api.py http://127.0.0.1:5000
```

## JSON API

| 方法与路径 | 说明 |
| --- | --- |
| `GET /api/health` | 健康检查 |
| `GET /api/state` | 全量快照（RS/Pod/Service endpoints/计划/审计，含计划 `stale` 标记） |
| `POST /api/handover` | body：`{"pod_uid": "...", "target_rs_id": "rs-v2"}` |
| `POST /api/rs/<rs_id>/desired` | body：`{"desired": 3}` |
| `POST /api/plans` | body：`{"rs_id": "rs-v1"}` 生成计划 |
| `POST /api/plans/<id>/execute` | 执行；依据失配返回 `409 stale_plan` 并把计划置为 `returned` |
| `POST /api/drill/conflict` | body：`{"kind": "app_key" | "overlap"}` |
| `POST /api/reset` | 恢复初始样例 |

业务错误统一为：`HTTP 409/400/404` + `{"ok": false, "error": {"code": "...", "message": "中文原因"}}`。

## 跑测试

```bash
python3 -m pip install -r requirements-dev.txt
python3 -m pytest -v
```

## 目录结构

```
app/
  __init__.py      # create_app、页面表单路由、JSON API
  domain.py        # 等值选择器/命中、交接标签规划与冲突判定、计划过期判定（纯逻辑）
  db.py            # SQLite schema、固定样例、重置
  services.py      # 交接事务、计划生成/执行、快照组装
  templates/       # base.html / index.html（服务端渲染，无构建步骤）
  static/style.css
scripts/smoke_api.py   # 对真实 HTTP 服务的端到端冒烟（标准库）
tests/test_drill.py    # 12 个用例
Dockerfile / docker-compose.yml
docs/                  # 交付说明与实测原始输出
```
