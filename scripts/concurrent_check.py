#!/usr/bin/env python3
"""并发交接专项实测：对真实运行的服务（Docker Compose / gunicorn）发请求。

用法：
    python3 scripts/concurrent_check.py [BASE_URL]

实测项：
  A. 模拟“两个标签页”同时提交同一个 HTML 交接表单（页面路由 /handover）：
     只有一笔成功 flash，另一笔报并发重复；期望数 3/3，仅 1 条交接记录，页面展示实际前后值。
  B. 同一 Pod 同时 JSON 交接给同一目标组（多轮）：每轮恰好一个 200、一个 409。
  C. 两个不同 Pod 同时交接：两笔都 200，期望数 2/4，两条记录的前后值按生效顺序链式衔接。
  D. 交接 vs 缩容的两种“生效顺序”（确定性顺序实测）：
     - 缩容先执行并删掉 Pod -> 交接报错、期望数 1/2 不变、0 条成功记录；
     - 交接先提交 -> 旧计划 409 stale_plan / returned，5 个 Pod 全保留。
  E. 多轮无预设同时竞速：无论谁赢，全局不变量必须成立，并统计实际生效顺序分布。

仅用标准库。退出码 0 表示全部 PASS。
"""

from __future__ import annotations

import json
import sys
import threading
import urllib.error
import urllib.parse
import urllib.request
from http.cookiejar import CookieJar

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:5000"
UID1 = "10000000-0000-4000-8000-000000000001"
UID2 = "10000000-0000-4000-8000-000000000002"
UID3 = "10000000-0000-4000-8000-000000000003"
RS_OLD = "rs-v1"
RS_NEW = "rs-v2"

failures: list[str] = []
print_lock = threading.Lock()


def call(method: str, path: str, body: dict | None = None, timeout: int = 30):
    data = None
    headers = {}
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(BASE + path, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode()
            return resp.status, json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode()
        try:
            return exc.code, json.loads(raw)
        except json.JSONDecodeError:
            return exc.code, {"raw": raw}


def check(name: str, condition: bool, detail: str = "") -> None:
    mark = "PASS" if condition else "FAIL"
    with print_lock:
        print(f"[{mark}] {name}" + (f" -- {detail}" if detail and not condition else ""))
    if not condition:
        failures.append(str(name))


def line(title: str) -> None:
    print("\n" + "=" * 74 + f"\n{title}\n" + "=" * 74)


def reset() -> None:
    status, _ = call("POST", "/api/reset")
    assert status == 200, f"reset failed: {status}"


def state() -> dict:
    status, body = call("GET", "/api/state")
    assert status == 200, body
    return body["state"]


def rs_desired(st: dict, rs_id: str) -> int:
    return next(r["desired"] for r in st["replicasets"] if r["id"] == rs_id)


def handover_rows(st: dict, uid: str | None = None) -> list[dict]:
    rows = st["handovers"]
    if uid is not None:
        rows = [h for h in rows if h["pod_uid"] == uid]
    return rows


def wait_health() -> None:
    for _ in range(50):
        try:
            status, body = call("GET", "/api/health", timeout=3)
            if status == 200 and body.get("ok"):
                return
        except Exception:
            pass
        import time
        time.sleep(0.4)
    raise SystemExit(f"服务不可用：{BASE}")


# ---------------------------------------------------------------------------
# A. 真实页面表单：两个标签页同时提交同一个 Pod 交接
# ---------------------------------------------------------------------------

def section_a_forms() -> None:
    line("A. 演练页表单并发：两个标签页同时交接同一 Pod -> 只能一笔生效")
    reset()
    barrier = threading.Barrier(2)
    pages: list[str] = []

    def submit_form():
        cj = CookieJar()
        opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
        form = urllib.parse.urlencode({
            "pod_uid": UID1, "target_rs_id": RS_NEW,
        }).encode()
        req = urllib.request.Request(
            BASE + "/handover", data=form, method="POST",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        barrier.wait(timeout=10)
        try:
            with opener.open(req, timeout=30) as resp:
                pages.append(resp.read().decode())
        except urllib.error.HTTPError as exc:
            pages.append(exc.read().decode())

    threads = [threading.Thread(target=submit_form) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=40)

    # 只认本请求 session 里的 flash 横幅；整页审计流会同时显示双方的审计条目，不能用于判定。
    import re
    flash_texts: list[str] = []
    for page in pages:
        banners = re.findall(r'<div class="flash flash-(?:ok|error)">(.*?)</div>',
                             page, flags=re.S)
        text = " ".join(re.sub(r"<[^>]+>", "", b) for b in banners)
        flash_texts.append(text)

    success_pages = [t for t in flash_texts if "交接成功" in t]
    denied_pages = [t for t in flash_texts if ("并发交接" in t or "已归属" in t or "重复交接" in t)]
    check("两个标签页中恰好 1 个 flash 为“交接成功”", len(success_pages) == 1,
          f"success={len(success_pages)} flashes={flash_texts}")
    check("另 1 个 flash 为拒绝/重复交接提示", len(denied_pages) == 1,
          f"denied={len(denied_pages)} flashes={flash_texts}")

    st = state()
    check("表单并发后 desired 为 3/3（不是各改两次的 2/4）",
          rs_desired(st, RS_OLD) == 3 and rs_desired(st, RS_NEW) == 3,
          f"v1={rs_desired(st, RS_OLD)} v2={rs_desired(st, RS_NEW)}")
    rows = handover_rows(st)
    check("表单并发后只有 1 条成功交接记录", len(rows) == 1, f"rows={len(rows)}")

    with urllib.request.urlopen(BASE + "/", timeout=10) as resp:
        html = resp.read().decode()
    check("演练页展示“两组期望数实际前后值”", "两组期望数实际前后值" in html)
    check("演练页记录含实际变化 4→3 与 2→3",
          "desired 4→3" in html and "desired 2→3" in html)


# ---------------------------------------------------------------------------
# B. JSON 重复交接多轮
# ---------------------------------------------------------------------------

def section_b_duplicate_json(rounds: int = 5) -> None:
    line(f"B. 同一 Pod JSON 并发交接给同一目标组，{rounds} 轮")
    for i in range(rounds):
        reset()
        barrier = threading.Barrier(2)
        results: list[tuple[int, str | None]] = []

        def worker():
            barrier.wait(timeout=10)
            status, body = call("POST", "/api/handover",
                                {"pod_uid": UID1, "target_rs_id": RS_NEW})
            code = body.get("error", {}).get("code") if isinstance(body, dict) else None
            results.append((status, code))

        ts = [threading.Thread(target=worker) for _ in range(2)]
        for t in ts:
            t.start()
        for t in ts:
            t.join(timeout=40)

        ok = sorted(s for s, _ in results)
        st = state()
        rows = handover_rows(st, UID1)
        good = (
            ok == [200, 409]
            and rs_desired(st, RS_OLD) == 3
            and rs_desired(st, RS_NEW) == 3
            and len(rows) == 1
        )
        check(f"第 {i + 1} 轮：恰好 200+409、desired 3/3、1 条记录", good,
              f"results={results} v1={rs_desired(st, RS_OLD)} "
              f"v2={rs_desired(st, RS_NEW)} rows={len(rows)}")


# ---------------------------------------------------------------------------
# C. 两个不同 Pod 并发交接
# ---------------------------------------------------------------------------

def section_c_different_pods() -> None:
    line("C. 两个不同 Pod（UID1/UID2）同时交接 -> 两笔都生效")
    reset()
    barrier = threading.Barrier(2)
    results: dict[str, tuple[int, dict]] = {}

    def worker(uid: str):
        barrier.wait(timeout=10)
        results[uid] = call("POST", "/api/handover",
                           {"pod_uid": uid, "target_rs_id": RS_NEW})

    ts = [threading.Thread(target=worker, args=(u,)) for u in (UID1, UID2)]
    for t in ts:
        t.start()
    for t in ts:
        t.join(timeout=40)

    check("两笔交接都返回 200",
          all(status == 200 for status, _ in results.values()),
          str({u: s for u, (s, _) in results.items()}))

    st = state()
    check("期望数为 2/4（4-2 / 2+2）",
          rs_desired(st, RS_OLD) == 2 and rs_desired(st, RS_NEW) == 4,
          f"v1={rs_desired(st, RS_OLD)} v2={rs_desired(st, RS_NEW)}")
    rows = sorted(handover_rows(st), key=lambda h: h["id"])
    check("恰有 2 条交接记录", len(rows) == 2, f"rows={len(rows)}")

    src = sorted((h["source_desired_before"], h["source_desired_after"]) for h in rows)
    tgt = sorted((h["target_desired_before"], h["target_desired_after"]) for h in rows)
    check("旧组前后值按生效顺序链接 (4→3),(3→2)", src == [(3, 2), (4, 3)], str(src))
    check("新组前后值按生效顺序链接 (2→3),(3→4)", tgt == [(2, 3), (3, 4)], str(tgt))

    for uid in (UID1, UID2):
        pod = next(p for p in st["pods"] if p["uid"] == uid)
        check(f"{uid[:8]} UID 不变 / Ready 保留 / 归属 web-v2 / 只命中 web-v2 / 仍在 Service",
              pod["uid"] == uid and pod["ready"] is True and pod["rs_id"] == RS_NEW
              and pod["matched_rs_ids"] == [RS_NEW]
              and uid in st["services"][0]["endpoint_uids"])
    check("Service 就绪 endpoints 仍为 4 个 UID",
          len(st["services"][0]["endpoint_uids"]) == 4)


# ---------------------------------------------------------------------------
# D1. 缩容先生效
# ---------------------------------------------------------------------------

def section_d1_scale_first() -> None:
    line("D1. 缩容计划先执行删掉 Pod，再交接 -> 必须报错且无副作用")
    reset()
    status, _ = call("POST", f"/api/rs/{RS_OLD}/desired", {"desired": 1})
    check("调整 desired=1 成功", status == 200)
    status, body = call("POST", "/api/plans", {"rs_id": RS_OLD})
    plan_id = body["result"]["plan_id"]
    check("缩容计划删除名单含 UID3 与 UID1",
          body["result"]["delete_uids"] == [UID3, UID1],
          str(body.get("result", {}).get("delete_uids")))

    status, body = call("POST", f"/api/plans/{plan_id}/execute")
    check("缩容计划先执行成功（删除 UID3、UID1）", status == 200 and body.get("ok"), str(body)[:200])

    status, body = call("POST", "/api/handover",
                        {"pod_uid": UID1, "target_rs_id": RS_NEW})
    code = body.get("error", {}).get("code")
    check("随后交接 UID1 返回 4xx 错误", status in (404, 409) and not body.get("ok"),
          f"status={status} body={str(body)[:200]}")
    check("错误码为 pod_not_found", code == "pod_not_found", f"code={code}")

    st = state()
    check("两组期望数保持原值 1/2（交接没有再 -1/+1）",
          rs_desired(st, RS_OLD) == 1 and rs_desired(st, RS_NEW) == 2,
          f"v1={rs_desired(st, RS_OLD)} v2={rs_desired(st, RS_NEW)}")
    check("没有留下任何成功交接记录", handover_rows(st) == [])
    check("UID1 已不存在", all(p["uid"] != UID1 for p in st["pods"]))


# ---------------------------------------------------------------------------
# D2. 交接先生效
# ---------------------------------------------------------------------------

def section_d2_handover_first() -> None:
    line("D2. 交接先提交，再执行旧缩容计划 -> 409 stale_plan/returned，Pod 全保留")
    reset()
    call("POST", f"/api/rs/{RS_OLD}/desired", {"desired": 1})
    _, body = call("POST", "/api/plans", {"rs_id": RS_OLD})
    plan_id = body["result"]["plan_id"]

    status, body = call("POST", "/api/handover",
                        {"pod_uid": UID1, "target_rs_id": RS_NEW})
    check("交接先成功", status == 200 and body.get("ok"), str(body)[:200])

    status, body = call("POST", f"/api/plans/{plan_id}/execute")
    check("旧计划执行返回 409 stale_plan",
          status == 409 and body.get("error", {}).get("code") == "stale_plan",
          f"status={status} body={str(body)[:200]}")

    st = state()
    plan = next(p for p in st["plans"] if p["id"] == plan_id)
    check("计划状态为 returned（已退回重算）", plan["status"] == "returned")
    check("5 个 Pod 全部保留（缩容一个没删）", len(st["pods"]) == 5,
          f"pods={len(st['pods'])}")
    check("期望数为 0/3（交接联动生效，计划未改动副本）",
          rs_desired(st, RS_OLD) == 0 and rs_desired(st, RS_NEW) == 3)
    check("UID1 保留且归属 web-v2",
          next(p for p in st["pods"] if p["uid"] == UID1)["rs_id"] == RS_NEW)


# ---------------------------------------------------------------------------
# E. 无预设同时竞速：全局不变量
# ---------------------------------------------------------------------------

def section_e_parallel_race(rounds: int = 8) -> None:
    line(f"E. 交接与缩容计划真正同时发出，{rounds} 轮不变量核对")
    winners = {"handover": 0, "scale": 0, "impossible": 0}
    for i in range(rounds):
        reset()
        call("POST", f"/api/rs/{RS_OLD}/desired", {"desired": 1})
        _, body = call("POST", "/api/plans", {"rs_id": RS_OLD})
        plan_id = body["result"]["plan_id"]

        barrier = threading.Barrier(2)
        hres: dict = {}
        pres: dict = {}

        def hworker():
            barrier.wait(timeout=10)
            hres["status"], hres["body"] = call(
                "POST", "/api/handover", {"pod_uid": UID1, "target_rs_id": RS_NEW})

        def pworker():
            barrier.wait(timeout=10)
            pres["status"], pres["body"] = call(
                "POST", f"/api/plans/{plan_id}/execute")

        t1 = threading.Thread(target=hworker)
        t2 = threading.Thread(target=pworker)
        t1.start(); t2.start()
        t1.join(timeout=40); t2.join(timeout=40)

        h_ok = hres.get("status") == 200
        p_ok = pres.get("status") == 200
        st = state()
        v1, v2 = rs_desired(st, RS_OLD), rs_desired(st, RS_NEW)
        n_pods = len(st["pods"])
        n_h = len(handover_rows(st))
        plan = next(p for p in st["plans"] if p["id"] == plan_id)
        uid1 = next((p for p in st["pods"] if p["uid"] == UID1), None)

        good = not (h_ok and p_ok)
        if h_ok:
            winners["handover"] += 1
            good &= (
                pres.get("status") == 409
                and pres.get("body", {}).get("error", {}).get("code") == "stale_plan"
                and plan["status"] == "returned"
                and n_pods == 5 and uid1 is not None and uid1["rs_id"] == RS_NEW
                and (v1, v2) == (0, 3) and n_h == 1
            )
        elif p_ok:
            winners["scale"] += 1
            good &= (
                hres.get("status") in (404, 409)
                and hres.get("body", {}).get("error", {}).get("code") == "pod_not_found"
                and plan["status"] == "executed"
                and uid1 is None and (v1, v2) == (1, 2) and n_h == 0
            )
        else:
            winners["impossible"] += 1
            good = False

        check(f"竞速第 {i + 1} 轮自洽（生效方：{'交接' if h_ok else '缩容' if p_ok else '无/异常'}）",
              good,
              f"h={hres.get('status')} p={pres.get('status')} v={v1}/{v2} "
              f"pods={n_pods} hrows={n_h} plan={plan['status']}")

    print(f"\n实际生效顺序分布：交接先生效 {winners['handover']} 轮，"
          f"缩容先生效 {winners['scale']} 轮，异常 {winners['impossible']} 轮")
    check("所有竞速轮次都有明确且自洽的唯一生效方", winners["impossible"] == 0)


def main() -> int:
    print(f"目标服务：{BASE}")
    wait_health()
    status, body = call("GET", "/api/health")
    check("health 200", status == 200 and body.get("ok") is True)

    section_a_forms()
    section_b_duplicate_json()
    section_c_different_pods()
    section_d1_scale_first()
    section_d2_handover_first()
    section_e_parallel_race()

    # 收尾恢复初始样例，方便人工继续在页面操作
    reset()

    line("结果")
    if failures:
        print(f"{len(failures)} 项失败：{failures}")
        return 1
    print("全部并发实测检查通过 ✅（重复交接唯一生效、不同 Pod 均完成、两种顺序均正确）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
