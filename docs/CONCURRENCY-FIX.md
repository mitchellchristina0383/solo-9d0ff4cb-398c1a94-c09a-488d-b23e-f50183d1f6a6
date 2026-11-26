# 并发交接修复说明与实测报告

日期：2026-11-26　范围：修复“同一 Pod 重复交接 / 不同 Pod 并发交接 / 交接与缩容竞争”的并发正确性，并逐笔记录两组期望数的实际前后值。

---

## 1. 修复前复现到的问题

用两个线程各开一个 SQLite 连接（等价于 gunicorn 多线程下两个标签页/请求）对初始样例实测：

1. **同一 Pod、同一目标组同时交接**（`UID web-v1-a1 → rs-v2`）：
   两笔都返回成功，旧组 desired `4 → 2`、新组 `2 → 4`（**两组期望数被各改两次**），`handovers` 表写入 **2 条**成功记录。
2. **缩容先删除 Pod、交接尚未写入**：把交接阻塞在“已读完 Pod/标签规划、尚未写事务”的时点，让缩容计划先提交删除 `web-v1-a1`，再放行交接。
   旧代码的 `UPDATE pods ... WHERE uid=?` **不检查 rowcount**，于是交接仍返回成功，desired 被改成 `1 → 0` / `2 → 3`，并给一个已不存在的 Pod 留下 1 条“成功交接记录”。
3. **交接先提交、缩容计划后执行的 TOCTOU 窗口**：旧代码在事务外做过期判定、在另一个 `with db:` 事务里删除。若“判定未过期”之后、删除之前交接提交，
   删除语句 `DELETE ... WHERE uid=? AND rs_id=?` 命中 0 行，旧逻辑抛 `plan_delete_failed` 回滚——Pod 虽然没删，但计划仍停在 `pending`，**没有按要求变成 `returned` 退回重算**。

根因：读取旧快照后，写入没有在同一把写锁内复核；Pod 更新不校验归属/行数；计划依据复核与执行不是同一个原子事务。

---

## 2. 修复内容

### 2.1 `app/db.py`

- `connect()`：`sqlite3.connect(..., timeout=30)` + `PRAGMA busy_timeout=30000`，并发写在锁上等待而不是立刻 `database is locked`。
- 新增 `immediate_transaction(db)` 上下文：显式下发 **`BEGIN IMMEDIATE`**，事务开始即取 SQLite 写锁。所有交接、计划执行的“复核 + 写入”都经它串行化；后到者拿锁后读到的必是前一笔已提交的最新状态，从而能“按生效顺序核对条件”。
- `handovers` 表新增 4 列，逐笔保存实际前后值：
  `source_desired_before` / `source_desired_after` / `target_desired_before` / `target_desired_after`。
- 新增轻量迁移 `_migrate_schema()`：对已存在的旧库（含 Docker 命名卷 `drill-data` 里的旧 `drill.db`）自动 `ALTER TABLE ... ADD COLUMN`，旧交接记录仍可展示，不丢数据。已用 `HEAD` 版旧 schema 建库验证迁移与旧记录渲染。

### 2.2 `app/services.py`

- **`handover()` 重写为“锁内重读 + 逐条件复核 + 条件写入”**：
  - 事务前只读 Pod/目标 RS 用于提前返回 404，并保留可注入的并发时序点 `handover:before_begin`；
  - `BEGIN IMMEDIATE` 后重新读取 Pod：已被缩容删除 → 写一条拒绝审计并返回 `409 pod_not_found`（`error.reason=deleted_by_scale_down`），**不更新 desired、不写 handovers**；
  - 用 Pod 当前 `rs_id` 重新确定源组并重读两组 desired、全部 RS/Service，重新计算标签；未就绪 / 已在目标组 / desired 触底 / 标签冲突等条件全部按最新状态再判一次；
  - 重复交接的落败者此时看到 Pod 已属于目标组 → `409 same_replicaset`（带 `error.concurrent=true`），不再联动 desired、不新增成功记录；
  - `UPDATE pods ... WHERE uid=? AND rs_id=?` 增加归属条件并校验 `rowcount==1`；旧组扣减仍带 `desired>0` 守卫；任何意外都整体回滚，绝不报成功；
  - 写入后在同一事务内**回读两组 desired 的实际 after 值**，连同 before 值和 `desired_change` 文本一起落 `handovers`；返回体与 `/api/state` 同时提供这些字段及 `desired_pairs / desired_before / desired_after` 等结构化视图。
  - UID、`ready` 两列依旧完全不更新；标签与 Service/选择器规则仍由原 `domain.plan_handover_labels()` 保证。
- **`execute_plan()` 关闭 TOCTOU 窗口**：保留事务前预检（页面快速提示），通过后进入 `plan:after_precheck` 时序点；随后在 `BEGIN IMMEDIATE` 事务内**重读计划状态并重新核对 desired + 归属 UID 快照**：
  - 失配 → 在同一事务把计划置为 `returned`、写审计并提交，再抛 `409 stale_plan`，**不增删任何 Pod**；
  - 计划已 `executed/returned` → 对应 `plan_already_executed / plan_returned`；
  - 复核通过才执行扩容/删除，删除仍带 `rs_id` 守卫与 rowcount 校验。
- `build_snapshot()`：交接记录附带两组期望数的结构化前后值（新列 + 多个常用别名），供页面与 API/脚本使用。
- 新增生产环境为 no-op 的 `_CONCURRENCY_HOOK` 测试卡点（`handover:before_begin`、`plan:after_precheck`），用于确定性复现“读到旧状态后、提交前”的两种危险交错；不改变正常运行行为。

### 2.3 页面 `app/templates/index.html` + `app/static/style.css`

- 第 ⑤ 区每条交接记录新增“**两组期望数实际前后值（本笔提交时读取）**” chips：`web-v1：4 → 3`、`web-v2：2 → 3`，并保留原 `desired_change` 文本行。
- 第 ⑤ 区标题补充并发语义说明。旧记录（迁移后新列为 NULL）只显示原文，不显示 chips、不会崩页。

### 2.4 测试与实测脚本（新增，不改旧用例）

- `tests/test_concurrency.py`（12 用例）：同 Pod 并发唯一生效（直连服务层 + Flask HTTP 两条链路）、两个不同 Pod 并发均完成且前后值链式衔接、缩容先删的确定性交错、交接先交导致预检后失配的确定性交错、6 轮无预设 `Barrier` 竞速不变量、`/api/state` 与首页渲染前后值。
- `scripts/concurrent_check.py`（标准库，真实 HTTP，43 项检查）：
  - A：用两个独立 cookieJar 的线程**同时 POST 演练页表单 `/handover`**，等价两个标签页同时点提交，并只按本会话 flash 横幅判定成功/拒绝；
  - B：同 Pod JSON 并发重复交接 5 轮；
  - C：两个不同 Pod 同时交接；
  - D1/D2：交接 vs 缩容的两种生效顺序；
  - E：8 轮真正同时发出的无预设竞速，核对全局不变量并统计实际生效方。

---

## 3. 实测结果（2026-11-26，本沙箱）

运行方式说明见第 4 节；以下均为真实执行结果，原始输出已存档：

| 实测 | 命令 | 结果 | 原始记录 |
| --- | --- | --- | --- |
| 全部单测 | `python3 -m pytest -o addopts="" -v` | **24/24 通过**（原 12 + 新增并发 12，含 6 组参数化竞速） | `docs/pytest-output.txt` |
| 原 HTTP 冒烟 | `python3 scripts/smoke_api.py http://127.0.0.1:5000` | **22/22 PASS** | `docs/smoke-output.txt` |
| 并发专项 HTTP | `python3 scripts/concurrent_check.py http://127.0.0.1:5000` | **43/43 PASS，退出码 0** | `docs/concurrent-output.txt` |
| 页面表单两种顺序 | `python3 /tmp/form_order_check.py`（临时脚本） | **FORM ORDER FLOWS OK** | 关键输出存档 `docs/form-order-output.txt` |
| 旧库迁移 | 用 `HEAD:app/db.py` 建旧 schema + 旧交接行后启动新代码 | 自动补 4 列、旧记录正常渲染、新交接写新列：**MIGRATION OK** | 本报告 3.5 |
| 服务日志 | gunicorn access/error log | 全部请求中 **0 个 5xx**，无 traceback / `database is locked` | `/tmp/drill-e2e/*.log`（沙箱临时目录） |

### 3.1 重复交接（实测）

- 两笔同时交接 `UID1 → rs-v2`：恰好 1 个 `200`、1 个 `409 same_replicaset`；
  desired 稳定为 **3 / 3**（修复前是 2 / 4），`handovers` 仅 **1** 条，记录前后值 `web-v1 4→3`、`web-v2 2→3`。
- 页面表单双会话并发：恰好 1 个 flash“交接成功……（旧组 3，新组 3）”，另 1 个 flash 为并发重复拒绝提示。
- JSON 连跑 5 轮，每轮均 `[200, 409]` + desired 3/3 + 1 条记录。
- 追加高压突发（未存档脚本，终端实测）：**10 轮 ×4 个同 Pod 标签页**、**6 轮 ×6 个同 Pod 标签页**，每轮都恰好 1 个 200、其余 409，desired 3/3、1 条记录；整个压测期间服务端 **0 个 5xx**。

### 3.2 两个不同 Pod 并发交接（实测）

`UID1、UID2` 同时交给 `rs-v2`：两笔均 200；desired **2 / 4**；2 条记录按提交顺序链式衔接：
旧组 `(4→3),(3→2)`、新组 `(2→3),(3→4)`（按 id 排序即实际生效顺序）。
两个 Pod 的 UID 不变、`ready=true`、归属 `rs-v2`、`matched_rs_ids==['rs-v2']`、仍在 `shop-svc` 就绪名单，Service endpoints 仍为 4 个 UID。
另做 **12 轮真正不同的两个 Pod 屏障并发**突发：每轮两笔都 200、desired 2/4、2 条记录，无一例外。

### 3.3 缩容先删掉 Pod（实测两种层面）

- **确定性危险交错**（pytest）：交接停在 `handover:before_begin`（已读到 Pod、未取写锁）→ 缩容计划执行并删除 `UID3、UID1` → 放行交接：
  交接抛 `409 pod_not_found`；desired 保持 **1 / 2**（没有变成 0/3）；`handovers` **0** 条；UID1 不存在。
- **真实 HTTP 生效顺序 D1**：先 `desired=1` 生成删除计划（名单 `[UID3, UID1]`）并执行成功，再交接 UID1：
  `404/409 pod_not_found`；desired 仍 1/2；0 条交接记录。
- 页面表单顺序一：缩容 flash“计划 #1 已执行：已删除 2 个 Pod”，随后交接 flash 为红色错误“找不到 Pod：10000000-…”。

### 3.4 交接先生效（实测两种层面）

- **确定性危险交错**（pytest）：计划已按旧依据通过事务前预检并停在 `plan:after_precheck` → 交接 UID1 成功（desired 1→0 / 2→3）→ 放行计划：
  抛 `stale_plan`，计划落库为 **`returned`**，Pod 总数 **5（一个没删）**，UID1 在 `rs-v2`，desired 0/3。
- **真实 HTTP 生效顺序 D2** 与 **页面表单顺序二** 结论相同：`409 stale_plan`、计划“已退回”、5 个 Pod 全保留，flash 提示“旧计划已退回……请……重新生成计划”。
- 8 轮无预设同时竞速：每轮都恰有一个唯一生效方；交接赢时必为 `stale_plan/returned/5 Pod/0·3/1 记录`，缩容赢时必为 `executed/UID1 删除/1·2/0 交接记录`。最后一次存档运行 8 轮都由交接先取到写锁（见 `docs/concurrent-output.txt` 末尾分布 8:0）；多次重复执行中曾记录到 **交接 7 / 缩容 1** 的分布，即真实同时请求下两种顺序都自然出现过。无论随机调度如何，“缩容先生效”都由 D1 的 HTTP 顺序实测与 pytest 的确定性交错用例强制覆盖。

### 3.5 页面实际渲染的记录文本（抓取首页解析）

```
web-v1-a1 web-v1 → web-v2 …
UID 10000000-0000-4000-8000-000000000001 保持不变；ready 状态保持不变
app=shop release=blue zone=a ⇒ app=shop release=green zone=a
两组期望数实际前后值（本笔提交时读取）： web-v1：4 → 3 ； web-v2：2 → 3
web-v1 desired 4→3；web-v2 desired 2→3
```

---

## 4. 启动方式与“用现有 Docker Compose”的实际情况

仓库的 `docker-compose.yml` / `Dockerfile` 未改，仍是：

```bash
docker compose up --build        # http://localhost:5000，命名卷 drill-data:/data
```

**本沙箱限制（如实说明）**：环境中没有 `docker` / `docker compose` 可执行程序、没有 `/var/run/docker.sock` daemon，
当前用户 uid=1001 且无 sudo，用户命名空间 `unshare -Ur` 被禁用，因此**无法在本机真正执行 `docker compose up --build`**。
已做的等价与配置核对：

- 用 PyYAML 解析 `docker-compose.yml` 通过：服务 `web`、`5000:5000`、环境 `DRILL_DB=/data/drill.db`、命名卷 `drill-data:/data`、healthcheck 均在；
- 在临时依赖目录安装 `requirements.txt` 中完全相同的 **Flask 3.1.3 / gunicorn 27.0.1**（及 pytest 9.2.0）；
- 用 **Dockerfile CMD 的同一命令与参数**（仅绑定地址用本机回环、DB 指向临时文件）真实启动：
  `gunicorn --bind 127.0.0.1:5000 --workers 1 --threads 4 --no-control-socket wsgi:application`，
  随后跑通第 3 节全部 HTTP/页面实测。首次在有 Docker 的机器上 `docker compose up --build` 需能访问 Docker Hub / PyPI。

### 在演练页人工复核两个标签页（30 秒）

1. `docker compose up --build` 后打开两个浏览器标签页，都进入 ②，选同一个 Ready Pod（如 `web-v1-a1`）、目标 `web-v2`；
2. 几乎同时点两页的“🚚 执行交接”，并确认浏览器 confirm；
3. 结果：一页绿色“交接成功（旧组 3，新组 3）”，另一页红色并发重复/已归属提示；
   ③ 两组 desired 为 3/3，⑤ 只有 1 条新记录且显示 `web-v1 4→3`、`web-v2 2→3`，审计里有一条 `handover_denied`。
4. 交接/缩容两顺序：把 `web-v1` desired 改成 1 → 生成计划（删除名单含未就绪 UID3 与 UID1）：
   - 先执行计划再交接 UID1 → 红色“找不到/已被缩容删除”，desired 保持 1/2，⑤ 无新成功记录；
   - 重置后同样生成计划，先交接 UID1，再执行旧计划 → 计划变“已退回”，5 个 Pod 全在，提示重新生成。

---

## 5. 未完成项 / 限制

1. **未能在本沙箱实际运行 `docker compose up --build`**（无 docker CLI/daemon、非 root、禁止 user namespace）。
   已用镜像 CMD 的等价 gunicorn 命令完成全部真实 HTTP 验证，并校验了 compose YAML；容器内的最终 compose 首跑仍建议在有 Docker 的环境确认一次。
2. 没有真实浏览器像素级操作；“两个标签页”用两个独立 cookie 会话的并发表单 POST + 屏障同时发起来等价模拟，flash 文案已按会话分别断言。
3. 并发方案基于 **SQLite 写锁串行化**（与题目“现有 Docker Compose、单 worker 多线程 + SQLite”的部署匹配，且比进程内锁更强：多进程/多 worker 也成立）。
   若未来改成多 worker 且高争用，`busy_timeout=30s` 下请求会排队；本演练的事务都很短。未引入 Postgres/行级锁或版本号列。
4. 为确定性测试新增了两个 no-op 时序卡点（`_CONCURRENCY_HOOK`），生产不注入时没有任何行为变化；没有为它加 HTTP 控制开关，避免成为可被外部利用的测试后门。
5. 竞速的自然调度不一定在单次 8 轮脚本里均匀出现两个生效方（最后存档这一轮为交接先 8 / 缩容先 0；多次运行中也出现过交接 7 / 缩容 1）；
   无论随机调度如何，“缩容先生效”的正确性都由 `concurrent_check.py` D1 的真实 HTTP 顺序实测与 pytest 的确定性交错用例强制保证，不依赖随机调度。
6. 未提交 git commit（按约定仅修改工作区）；改动文件见 `git status` / `git diff`。
