"""Flask 应用入口：服务端渲染页面 + JSON API。"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

from flask import (
    Flask,
    g,
    jsonify,
    redirect,
    render_template,
    request,
    url_for,
)

from .db import connect, init_schema, reset_to_seed, seed_if_empty
from .services import (
    UseCaseError,
    build_snapshot,
    create_conflict_rs,
    discard_plan,
    execute_plan,
    generate_plan,
    handover,
    remove_drill_rs,
    set_desired,
)


def _default_db_path() -> str:
    return os.environ.get("DRILL_DB", str(Path.cwd() / "drill.db"))


def create_app(db_path: str | Path | None = None) -> Flask:
    app = Flask(__name__)
    app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "local-drill-secret")
    app.config["DB_PATH"] = str(db_path or _default_db_path())

    def get_db() -> sqlite3.Connection:
        if "db" not in g:
            g.db = connect(app.config["DB_PATH"])
        return g.db

    @app.teardown_appcontext
    def close_db(exc: BaseException | None) -> None:
        db = g.pop("db", None)
        if db is not None:
            db.close()

    # 启动即建表并在空库时播种。
    bootstrap_db = connect(app.config["DB_PATH"])
    try:
        init_schema(bootstrap_db)
        seed_if_empty(bootstrap_db)
    finally:
        bootstrap_db.close()

    # ------------------------------------------------------------------
    # 页面
    # ------------------------------------------------------------------

    @app.get("/")
    def index():
        snapshot = build_snapshot(get_db())
        return render_template("index.html", s=snapshot)

    def redirect_back(anchor: str = "top"):
        return redirect(url_for("index") + (f"#{anchor}" if anchor else ""))

    @app.post("/handover")
    def web_handover():
        try:
            result = handover(
                get_db(),
                request.form.get("pod_uid", ""),
                request.form.get("target_rs_id", ""),
            )
        except UseCaseError as exc:
            _flash(exc.message, "error")
        else:
            _flash(
                f"交接成功：{result['pod_name']} 已由旧组移交给新控制器，"
                f"UID 与就绪状态保留，仍命中 shop-svc；"
                f"两边期望副本数已联动（旧组 {result['source_desired']}，"
                f"新组 {result['target_desired']}）。",
                "ok",
            )
        return redirect_back("handover-panel")

    @app.post("/rs/<rs_id>/desired")
    def web_set_desired(rs_id: str):
        try:
            set_desired(get_db(), rs_id, request.form.get("desired"))
        except UseCaseError as exc:
            _flash(exc.message, "error")
        else:
            _flash("期望副本数已更新；此前计划若依据变化，执行时会被退回。", "ok")
        return redirect_back("rs-" + rs_id)

    @app.post("/plans/generate")
    def web_generate_plan():
        rs_id = request.form.get("rs_id", "")
        try:
            result = generate_plan(get_db(), rs_id)
        except UseCaseError as exc:
            _flash(exc.message, "error")
            return redirect_back("plans")
        _flash(f"已生成计划 #{result['plan_id']}（待执行）：{result['summary']}", "ok")
        return redirect_back("plan-" + str(result["plan_id"]))

    @app.post("/plans/<int:plan_id>/execute")
    def web_execute_plan(plan_id: int):
        try:
            result = execute_plan(get_db(), plan_id)
        except UseCaseError as exc:
            # 关键路径：过期计划在这里被置为 returned，并提示重新计算。
            _flash(exc.message, "error")
            return redirect_back("plan-" + str(plan_id))
        _flash(f"计划 #{plan_id} 已执行：{result['result']}", "ok")
        return redirect_back("plan-" + str(plan_id))

    @app.post("/plans/<int:plan_id>/discard")
    def web_discard_plan(plan_id: int):
        try:
            discard_plan(get_db(), plan_id)
        except UseCaseError as exc:
            _flash(exc.message, "error")
        return redirect_back("plans")

    @app.post("/drill/conflict/<kind>")
    def web_conflict(kind: str):
        try:
            preset = create_conflict_rs(get_db(), kind)
        except UseCaseError as exc:
            _flash(exc.message, "error")
        else:
            _flash(f"已加入冲突演练 ReplicaSet：{preset['name']}，可在交接面板选择它试跑。", "ok")
        return redirect_back("handover-panel")

    @app.post("/drill/remove/<rs_id>")
    def web_remove_drill(rs_id: str):
        try:
            remove_drill_rs(get_db(), rs_id)
        except UseCaseError as exc:
            _flash(exc.message, "error")
        else:
            _flash("演练 ReplicaSet 已移除。", "ok")
        return redirect_back("rs-" + rs_id)

    @app.post("/reset")
    def web_reset():
        reset_to_seed(get_db())
        _flash("已重置为初始样例：2 个 ReplicaSet、5 个 Pod、1 个 Service。", "ok")
        return redirect_back("top")

    # ------------------------------------------------------------------
    # JSON API（供脚本 / 自动化实测使用）
    # ------------------------------------------------------------------

    @app.get("/api/health")
    def api_health():
        return jsonify(ok=True)

    @app.get("/api/state")
    def api_state():
        return jsonify(ok=True, state=build_snapshot(get_db()))

    def _api_call(fn, *args, **kwargs):
        try:
            return jsonify(ok=True, result=fn(get_db(), *args, **kwargs))
        except UseCaseError as exc:
            return (
                jsonify(
                    ok=False,
                    error={"code": exc.code, "message": exc.message, **exc.extra},
                ),
                exc.status,
            )

    @app.post("/api/handover")
    def api_handover():
        body = request.get_json(silent=True) or {}
        return _api_call(handover, body.get("pod_uid", ""), body.get("target_rs_id", ""))

    @app.post("/api/rs/<rs_id>/desired")
    def api_set_desired(rs_id: str):
        body = request.get_json(silent=True) or {}
        return _api_call(set_desired, rs_id, body.get("desired"))

    @app.post("/api/plans")
    def api_generate_plan():
        body = request.get_json(silent=True) or {}
        return _api_call(generate_plan, body.get("rs_id", ""))

    @app.post("/api/plans/<int:plan_id>/execute")
    def api_execute_plan(plan_id: int):
        return _api_call(execute_plan, plan_id)

    @app.post("/api/drill/conflict")
    def api_conflict():
        body = request.get_json(silent=True) or {}
        return _api_call(create_conflict_rs, body.get("kind", ""))

    @app.post("/api/drill/remove/<rs_id>")
    def api_remove_drill(rs_id: str):
        return _api_call(remove_drill_rs, rs_id)

    @app.post("/api/reset")
    def api_reset():
        reset_to_seed(get_db())
        return jsonify(ok=True)

    @app.errorhandler(404)
    def not_found(err):  # noqa: ANN001
        if request.path.startswith("/api/"):
            return jsonify(ok=False, error={"code": "not_found", "message": "接口不存在"}), 404
        return redirect(url_for("index"))

    # 模板辅助
    @app.template_filter("status_cn")
    def status_cn(status: str) -> str:
        return {"pending": "待执行", "executed": "已执行", "returned": "已退回"}.get(status, status)

    return app


def _flash(message: str, category: str) -> None:
    from flask import flash

    flash(message, category)
