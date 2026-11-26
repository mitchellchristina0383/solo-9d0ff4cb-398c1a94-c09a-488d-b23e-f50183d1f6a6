# 交付说明：ReplicaSet 换组交接演练台

> **并发交接修复（2026-11-26 追加）**：同 Pod 重复交接唯一生效、不同 Pod 并发均完成、交接/缩容两种生效顺序均正确、交接记录逐笔写实际前后值。详见 **[`CONCURRENCY-FIX.md`](./CONCURRENCY-FIX.md)**；当前测试 **24/24 通过**，并发 HTTP 实测 **43/43 PASS**。下文为初始交付说明（其中“12/12”为修复前基线）。

日期：2026-11-26　形态：Flask 3 服务端渲染 + SQLite + Docker Compose　代码全部位于 `/workspace`。

## 1. 需求对照与实现

| 要求 | 实现位置 | 说明 |
| --- | --- | --- |
| Flask + SQLite 本地演练页 | `app/__init__.py`、`app/db.py`、`templates/index.html` | 单页工作台，无前端构建步骤 |
| 两个 ReplicaSet、各自 Pod、一个 Service | `app/db.py` `_seed_data()` | `web-v1`（3 归属 Pod：2 Ready/1 NotReady，desired 4）、`web-v2`（2 Pod 全 Ready，desired 2）、`shop-svc`（选择器 `app=shop`，就绪 endpoints=4） |
| 选择器只用等值标签 | `app/domain.py` `matches()` | 选择器就是 `dict[str,str]`，语义为所有 `key=value` AND；UI 不提供任何集合型选择器 |
| 选就绪 Pod 交接 | 页面 ②、`services.handover()` | 未就绪 / 不存在 / 同组交接均拒绝；只有 Ready Pod 的单选框可选，后端再次强制校验 |
| 保留 UID 和就绪状态 | `services.handover()` 的 UPDATE | 只 `UPDATE pods SET labels_json=?, rs_id=?`，**不更新 `uid` 与 `ready` 列**；测试断言同一 UID、`ready is True` |
| 仍命中原 Service | `domain.plan_handover_labels()` | 交接前命中的所有 Service 被记为 `kept_services`，写入目标标签后逐键复核；掉出 Service 即判冲突 |
| 只命中新 ReplicaSet | 同上 | 先对齐目标选择器，再循环解除对其它 RS 的命中，最后断言 `matched_rs_ids == [target]` |
| 归属 + 两边期望数一起变更（旧−1/新+1） | `services.handover()` 同一 `with db:` 事务 | 改 `pods.rs_id`、旧 RS `desired=desired-1 ... WHERE desired>0`、新 RS `desired=desired+1`，并写交接记录/审计；任一步失败整体回滚 |
| 办不到就保留原状并说明原因 | `HandoverImpossible` / `UseCaseError` | 标签键值冲突 `label_conflict`、选择器重叠 `selector_overlap`、未就绪 `pod_not_ready`、期望数触底 `desired_floor` 等；写一条拒绝审计，页面红字 / API 409 返回中文原因，业务数据不变（有状态签名断言） |
| 按期望数与现有归属生成增删计划、稍后执行 | `services.generate_plan/execute_plan()` | `desired > owned` 生成 `+N`；`<` 生成删除名单（未就绪优先、再按入库顺序）；相等为空操作。计划持久化为 `pending` |
| 交接改变期望数或归属后，旧计划退回并提示重算 | `domain.plan_is_stale()` + `execute_plan()` | 计划保存 `basis_desired` 与排序后的 `basis_uids`；页面实时显示“⚠ 情况已变化，请重新生成计划”；执行瞬间再次比对，失配则置 `status=returned`、不增删任何 Pod，返回 `stale_plan` 与重算提示 |
| 页面显示现有归属和 Service 就绪名单 | `index.html` ①③ | Service endpoints（Ready+命中）、每个 RS 的 desired/归属数/就绪数与 Pod 列表、每个 Pod 的“现有归属”和“选择器实际命中” |
| Docker Compose 启动 | `Dockerfile`、`docker-compose.yml` | `docker compose up --build` 后访问 `http://localhost:5000`；SQLite 落在命名卷 `drill-data:/data`，带 healthcheck |

### 关于“退回”的交互时点（有意设计）

计划在生成时固化依据。交接发生后：

- **执行前**：计划仍是 `pending`，但 `GET /api/state` 与页面会给出 `stale=true` 和“情况已变化，请重新生成计划”的红色提示，按钮文案变为“执行旧计划（应被退回）”；
- **点击执行旧计划时**：后端做乐观校验，失配后在同一事务把计划落库为 `returned（已退回）`，不执行任何增删，并 flash “旧计划已退回……请……重新生成计划”。

这样可以完整试跑题目要求的“交接后执行旧计划 → 被退回”。退回后的计划不能再执行（`plan_returned`），只能重新生成。

## 2. 标签冲突是怎么判定的

`plan_handover_labels()`（纯函数，便于单测）：

1. 从 Pod 现有标签出发，写入目标 RS 选择器要求的全部 `key=value`；
2. 若这会改坏交接前命中的 Service 选择器固定键（典型：Service 要 `app=shop`，目标要 `app=canary`）→ `label_conflict`，无解；
3. 把“目标选择器键 + 需保留 Service 的选择器键”视为锁定键；若仍命中其它 RS，尝试删除其选择器中未锁定的键来解除命中；若其选择器键全部被锁定（典型：目标 RS 与旧 RS 选择器完全相同）→ `selector_overlap`，无法做到“只命中新 RS”；
4. 最终断言：命中目标、不命中任何其它 RS、不丢失原 Service。

任何一步报错都发生在数据库写入之前；路由层保证数据原样。

## 3. 实测结果（2026-11-26，本沙箱）

环境：Python 3.11.2；Flask 3.1.3、pytest 9.2.0、gunicorn 27.0.1（装在临时 venv 中，仅用于实测）。

### 3.1 自动化测试：12/12 通过

命令：`python3 -m pytest -v`（原始输出：`docs/pytest-output.txt`）

```
collected 12 items
tests/test_drill.py ............                                         [100%]
============================== 12 passed in 1.01s ==============================
```

覆盖：样例 endpoints、首页渲染、成功交接（UID/Ready/标签/Service/只命中新 RS/desired 3↔3）、
未就绪与同组拒绝、`app` 键值冲突、选择器重叠冲突且状态签名不变、
**旧组增容计划交接后执行返回 `409 stale_plan` 并置 `returned`、重算后执行成功**、
**新组计划同样因 desired 2→3 被判定过期**、缩容删除计划的过期保护、
网页表单 PRG 流程出现“已退回 / 情况已变化 / 重新生成”、重置恢复样例。

### 3.2 真实 HTTP 冒烟（gunicorn，与容器同一 WSGI 启动方式）

命令：

```bash
DRILL_DB=/tmp/drill-smoke/smoke.db \
  gunicorn --bind 127.0.0.1:5000 --workers 1 --threads 4 \
  --no-control-socket wsgi:application
python3 scripts/smoke_api.py
```

原始记录：`docs/smoke-output.txt`。结论：**22 项检查全部 PASS**，关键实测报文：

- 键值冲突：`HTTP 409 label_conflict`
  > 标签冲突：Service shop-svc 要求 app='shop'，目标 ReplicaSet web-canary-conflict 要求 app='canary'，两组等值要求无法同时成立。……因此保留原状。
  随后核对：UID1 仍归属 web-v1、`release=blue`、旧组 desired 仍为 4。
- 重叠选择器：`HTTP 409 selector_overlap`
  > ……Pod 仍会命中 ReplicaSet web-v1……每个标签键都被目标选择器或原 Service 选择器锁定……无法做到只命中新 ReplicaSet，已保留原状。
- 成功交接 UID1 → rs-v2：新标签 `{app: shop, release: green, zone: a}`（无关标签 `zone` 保留），desired 4→3 / 2→3，Service endpoints 仍为同样 4 个 UID，`matched_rs_ids == ['rs-v2']`。
- 交接后首页 HTML 实测包含“情况已变化，请重新生成计划”。
- 执行旧计划 #1：`HTTP 409 stale_plan`
  > 计划 #1 生成后，web-v1 的期望副本数或 Pod 归属已改变，情况已变化（期望副本数 4 → 3；Pod 归属集合变化（移走 10000000…））。……旧计划已退回，请……重新生成计划。
  Pod 总数仍为 5（未误增删），计划状态 `returned`。
- 重算计划：desired=3 / owned=2 仍为 `+1`；执行后新增 1 个 Ready Pod，总数 6，旧组归属回到 3。

> 这个“数值差额恰好仍是 +1 但依旧拒绝旧计划”的用例是有意保留的：判定依据是 desired 与归属 UID 集合的快照，而不是只看增减数量，避免交接后误执行基于旧归属选出的删除名单。

### 3.3 网页表单链路（真实 gunicorn + cookie）

`/plans/generate` 302 → `/handover` 302 → `/plans/1/execute` 按 PRG 跟随为 200，最终 HTML 中实测包含：
flash“旧计划已退回，请按当前期望数和现有归属重新生成计划后再执行。”、`flash-error` 样式、退回原因中的“情况已变化”、以及“重新生成计划”按钮。执行过的计划卡片渲染 `badge-status-executed`，退回的渲染 `badge-status-returned`；`GET /static/style.css` → 200。

### 3.4 交付前改动核对

- `python -m compileall app tests scripts`：通过。
- `git diff --cached --check`（临时 `git add -A` 后检查，随后已 unstage，未提交）：无空白/冲突标记问题。
- `docker-compose.yml` 用 PyYAML 解析通过：服务 `web`、端口 `5000:5000`、命名卷 `drill-data`、healthcheck 均在。
- 文件清单（21 个）：`app/{__init__,db,domain,services}.py`、两个模板、CSS、`tests/test_drill.py`、`scripts/smoke_api.py`、`Dockerfile`、`docker-compose.yml`、`requirements*.txt`、`README.md`、`docs/`（DELIVERY + 两份实测原始输出）。
- 按约定**没有创建 git commit**；当前所有交付物都是工作区新文件。

## 4. 未完成项 / 限制（如实说明）

1. **本沙箱内没有 docker / docker compose 可执行程序，也没有 daemon 或 `/var/run/docker.sock`，且无免密 sudo**，因此没能在本机真正跑一次 `docker compose up --build`。已做的等价验证：
   - compose YAML 解析与字段核对；
   - 按 `requirements.txt` 在 venv 真实安装 Flask 3.1.3 / gunicorn 27.0.1；
   - 用 Dockerfile CMD 的等价命令（含 `--no-control-socket`）真实启动并通过 health、首页与全部冒烟。
   首次在有 Docker 的机器上 `docker compose up --build` 时需要能访问 PyPI / Docker Hub 拉基础镜像与依赖。
2. 未做真实浏览器截图或像素级视觉走查；各关键状态（初始、冲突组、交接后 stale、执行/退回、flash）的 HTML 均通过 Flask test client 与真实 curl 抓取并断言了关键中文标记。
3. 这是**教学演练模型**，不是真实 Kubernetes 控制器：
   - 没有 ReplicaSet controller 的调和循环，改 desired 不会自动增删 Pod，副本变化只通过“计划执行”发生；
   - 没有独立 Endpoints/EndpointSlice 对象，就绪名单由“Ready 且命中 Service 选择器”即时计算；
   - 计划扩容出的新 Pod 直接置为 Ready，标签取 RS 选择器的最小集合（没有 PodTemplate 更丰富的模板）；
   - 单用户、无鉴权、SQLite 单 worker（Compose 固定 `--workers 1`）。
4. 冲突演练组可在页面一键构造/移除，也可“重置样例”；没有提供任意选择器编辑器——这是为了保证“只用等值标签”和冲突判定的确定性。
5. 审计/交接记录只保留最近若干条用于页面展示，数据仍全量在 SQLite 中，未做分页。
