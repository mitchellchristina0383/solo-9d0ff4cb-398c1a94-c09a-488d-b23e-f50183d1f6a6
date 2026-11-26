#!/usr/bin/env python3
"""对真实运行的服务做端到端冒烟（仅用标准库）。

用法：
    python3 scripts/smoke_api.py [BASE_URL]

覆盖：样例概览 -> 标签冲突拒绝且不变 -> 重叠选择器拒绝 ->
生成计划 -> 交接 -> 页面提示“情况已变化” -> 执行旧计划被退回 ->
重算并执行成功。
"""

from __future__ import annotations

import json
import sys
import urllib.error
import urllib.request

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:5000"
UID1 = "10000000-0000-4000-8000-000000000001"
UID2 = "10000000-0000-4000-8000-000000000002"
RS_OLD = "rs-v1"
RS_NEW = "rs-v2"

failures: list[str] = []


def call(method: str, path: str, body: dict | None = None):
    data = None
    headers = {}
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(BASE + path, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())


def check(name: str, condition: bool, detail: str = "") -> None:
    mark = "PASS" if condition else "FAIL"
    print(f"[{mark}] {name}" + (f" -- {detail}" if detail and not condition else ""))
    if not condition:
        failures.append(name)


def line(title: str) -> None:
    print("\n" + "=" * 72 + f"\n{title}\n" + "=" * 72)


def main() -> int:
    line("0. 健康检查 + 重置为初始样例")
    status, body = call("GET", "/api/health")
    check("GET /api/health 200", status == 200 and body.get("ok") is True, str(body))
    status, _ = call("POST", "/api/reset")
    check("POST /api/reset 200", status == 200, f"status={status}")

    _, body = call("GET", "/api/state")
    state = body["state"]
    print("ReplicaSets:")
    for rs in state["replicasets"]:
        print(f"  - {rs['name']}: selector={rs['selector']} "
              f"desired={rs['desired']} owned={rs['owned_count']} ready={rs['ready_count']}")
    svc = state["services"][0]
    print(f"Service {svc['name']} selector={svc['selector']} "
          f"ready endpoints={len(svc['endpoint_uids'])}")
    check("初始 2 RS / 5 Pod / 4 就绪 endpoints",
          len(state["replicasets"]) == 2 and len(state["pods"]) == 5
          and len(svc["endpoint_uids"]) == 4)

    line("1. 标签键值冲突（app=canary 对 Service app=shop）必须拒绝")
    status, body = call("POST", "/api/drill/conflict", {"kind": "app_key"})
    check("构造 app_key 冲突 RS", status == 200 and body["ok"], str(body))

    status, body = call("POST", "/api/handover",
                        {"pod_uid": UID1, "target_rs_id": "rs-conflict-app"})
    print(f"HTTP {status} code={body.get('error', {}).get('code')}")
    print("原因：" + body.get("error", {}).get("message", ""))
    check("冲突交接返回 409 label_conflict",
          status == 409 and body["error"]["code"] == "label_conflict")

    _, body = call("GET", "/api/state")
    st = body["state"]
    pod1 = next(p for p in st["pods"] if p["uid"] == UID1)
    old = next(r for r in st["replicasets"] if r["id"] == RS_OLD)
    check("冲突后 UID1 仍归属 web-v1 且标签未变",
          pod1["rs_id"] == RS_OLD and pod1["labels"]["release"] == "blue")
    check("冲突后旧组 desired 仍为 4", old["desired"] == 4)

    line("2. 选择器完全重叠冲突也必须拒绝")
    status, _ = call("POST", "/api/drill/conflict", {"kind": "overlap"})
    check("构造 overlap 冲突 RS", status == 200)
    status, body = call("POST", "/api/handover",
                        {"pod_uid": UID2, "target_rs_id": "rs-conflict-overlap"})
    print(f"HTTP {status} code={body.get('error', {}).get('code')}")
    print("原因：" + body.get("error", {}).get("message", ""))
    check("重叠选择器交接返回 409 selector_overlap",
          status == 409 and body["error"]["code"] == "selector_overlap")

    line("3. 先生成 web-v1 的增容计划（desired=4 / owned=3 → +1）")
    status, body = call("POST", "/api/plans", {"rs_id": RS_OLD})
    check("生成计划成功且 add_count=1",
          status == 200 and body["result"]["add_count"] == 1, str(body))
    plan_id = body["result"]["plan_id"]
    print(f"plan #{plan_id}: {body['result']['summary']}")

    line("4. 成功交接 UID1 → web-v2（保留 UID/Ready/Service 流量）")
    status, body = call("POST", "/api/handover",
                        {"pod_uid": UID1, "target_rs_id": RS_NEW})
    check("交接成功 200", status == 200 and body["ok"], str(body))
    print(body["result"])

    _, body = call("GET", "/api/state")
    st = body["state"]
    pod1 = next(p for p in st["pods"] if p["uid"] == UID1)
    svc = st["services"][0]
    check("UID 不变且仍 Ready", pod1["uid"] == UID1 and pod1["ready"] is True)
    check("归属变为 web-v2 且只命中 web-v2",
          pod1["rs_id"] == RS_NEW and pod1["matched_rs_ids"] == [RS_NEW])
    check("仍在 Service 就绪名单且总数仍为 4",
          UID1 in svc["endpoint_uids"] and len(svc["endpoint_uids"]) == 4)
    check("两边 desired 为 3 / 3",
          next(r for r in st["replicasets"] if r["id"] == RS_OLD)["desired"] == 3
          and next(r for r in st["replicasets"] if r["id"] == RS_NEW)["desired"] == 3)

    # 页面在执行前就应把旧计划标成“情况已变化”
    with urllib.request.urlopen(BASE + "/", timeout=10) as resp:
        html = resp.read().decode()
    check("首页 200 且包含“情况已变化，请重新生成计划”",
          resp.status == 200 and "情况已变化，请重新生成计划" in html)

    line("5. 交接后执行旧计划 → 必须退回（409 stale_plan），不产生副本变动")
    status, body = call("POST", f"/api/plans/{plan_id}/execute")
    print(f"HTTP {status} code={body.get('error', {}).get('code')}")
    print("提示：" + body.get("error", {}).get("message", ""))
    check("旧计划执行返回 409 stale_plan 且提示重新生成",
          status == 409 and body["error"]["code"] == "stale_plan"
          and "重新生成" in body["error"]["message"])

    _, body = call("GET", "/api/state")
    st = body["state"]
    plan = next(p for p in st["plans"] if p["id"] == plan_id)
    check("旧计划状态为 returned（已退回）", plan["status"] == "returned")
    check("没有误增删：Pod 总数仍为 5", len(st["pods"]) == 5)

    line("6. 重新生成计划并执行 → 成功")
    status, body = call("POST", "/api/plans", {"rs_id": RS_OLD})
    check("重算计划 add_count 仍为 1（desired=3 / owned=2）",
          status == 200 and body["result"]["add_count"] == 1, str(body))
    new_plan = body["result"]["plan_id"]
    status, body = call("POST", f"/api/plans/{new_plan}/execute")
    check("执行新计划成功", status == 200 and body["ok"], str(body))
    print(body.get("result", {}).get("result", ""))

    _, body = call("GET", "/api/state")
    st = body["state"]
    check("执行后 Pod 总数 6 且 web-v1 归属回到 3",
          len(st["pods"]) == 6
          and next(r for r in st["replicasets"] if r["id"] == RS_OLD)["owned_count"] == 3)

    line("结果")
    if failures:
        print(f"{len(failures)} 项失败：{failures}")
        return 1
    print("全部冒烟检查通过 ✅（标签冲突拒绝、旧计划退回重算均符合预期）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
