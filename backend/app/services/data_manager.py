"""跨模块共享数据管理器：读写 data/ 目录下的业务数据产物。

职责：
- 每个服务产出 domain_snapshot 后序列化到 data/{module}/snapshot.json
- 下游服务从 data/ 目录读取上游的标准化数据产物
- 审计记录写入 data/audit/
- 用量记录写入 data/usage/
- 导出任务结果写入 data/exports/
- API 注册快照写入 data/api/
- 订阅审批记录写入 data/subscriptions/
"""
from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

# 数据目录相对于仓库根
DATA_ROOT = Path(os.environ.get("DATA_ROOT", str(
    Path(__file__).resolve().parents[3] / "data"
)))

# 东八区
TZ = timezone(timedelta(hours=8))


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _read_json(path: Path, default: Any = None) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return default


def _write_json(path: Path, data: Any) -> None:
    _ensure_dir(path.parent)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def _now_iso() -> str:
    return datetime.now(TZ).isoformat()


def now_iso() -> str:
    """公开的时间戳助手，供各业务服务记录业务事实时间。"""
    return _now_iso()


# ========== API 注册快照 ==========


def write_api_snapshot(api_id: int, api_data: dict[str, Any]) -> None:
    """将单个 API 注册数据写入共享快照，供路由、缓存、限流等模块消费。"""
    snapshot = _read_json(DATA_ROOT / "api" / "registry.json", {})
    key = str(api_id)
    api_data["_synced_at"] = _now_iso()
    snapshot[key] = api_data
    _write_json(DATA_ROOT / "api" / "registry.json", snapshot)


def read_api_snapshot(api_id: int | None = None) -> dict[str, Any] | dict[str, dict[str, Any]]:
    """读取 API 注册快照。返回单个或全部。"""
    snapshot = _read_json(DATA_ROOT / "api" / "registry.json", {})
    if api_id is not None:
        return snapshot.get(str(api_id), {})
    return snapshot


def remove_api_snapshot(api_id: int) -> None:
    """标记 API 为下线。"""
    snapshot = _read_json(DATA_ROOT / "api" / "registry.json", {})
    key = str(api_id)
    if key in snapshot:
        snapshot[key]["status"] = "已下线"
        snapshot[key]["_deleted_at"] = _now_iso()
        _write_json(DATA_ROOT / "api" / "registry.json", snapshot)


# ========== 订阅审批 ==========


def write_subscription(sub_id: int, sub_data: dict[str, Any]) -> None:
    """写入订阅申请记录到共享目录。"""
    record = {
        "subscription_id": sub_id,
        **sub_data,
        "_created_at": sub_data.get("_created_at", _now_iso()),
        "_updated_at": _now_iso(),
    }
    _write_json(DATA_ROOT / "subscriptions" / f"{sub_id:06d}.json", record)


def read_subscription(sub_id: int) -> dict[str, Any]:
    return _read_json(DATA_ROOT / "subscriptions" / f"{sub_id:06d}.json", {})


def list_subscriptions() -> list[dict[str, Any]]:
    """列出所有订阅记录。"""
    _ensure_dir(DATA_ROOT / "subscriptions")
    records = []
    for f in sorted(DATA_ROOT.glob("subscriptions/*.json")):
        record = _read_json(f)
        if record:
            records.append(record)
    return records


def approve_subscription(sub_id: int, approved: bool, approver: str = "system") -> dict[str, Any]:
    """审批订阅，记录审批流程。"""
    record = read_subscription(sub_id)
    if not record:
        return {}
    record["approval_status"] = "已通过" if approved else "已驳回"
    record["approver"] = approver
    record["_approved_at"] = _now_iso()
    write_subscription(sub_id, record)
    return record


# ========== 用量计费 ==========


def write_usage(usage_id: int, usage_data: dict[str, Any]) -> None:
    """写入用量记录到共享目录。"""
    usage_data["_created_at"] = usage_data.get("_created_at", _now_iso())
    usage_data["_updated_at"] = _now_iso()
    _write_json(DATA_ROOT / "usage" / f"{usage_id:06d}.json", usage_data)


def read_usage(usage_id: int) -> dict[str, Any]:
    return _read_json(DATA_ROOT / "usage" / f"{usage_id:06d}.json", {})


def list_usage(billing_cycle: str | None = None) -> list[dict[str, Any]]:
    """列出用量记录，支持按计费周期过滤。"""
    _ensure_dir(DATA_ROOT / "usage")
    records = []
    for f in sorted(DATA_ROOT.glob("usage/*.json")):
        record = _read_json(f)
        if record:
            if billing_cycle is None or record.get("billing_cycle") == billing_cycle:
                records.append(record)
    return records


def settle_billing(billing_cycle: str, use_auto_settlement: bool = True) -> dict[str, Any]:
    """结算指定计费周期，生成结算汇总。"""
    records = list_usage(billing_cycle)
    total_amount = sum(float(r.get("amount", 0)) for r in records)
    total_calls = sum(int(r.get("call_count", 0)) for r in records)
    settlement = {
        "billing_cycle": billing_cycle,
        "total_amount": total_amount,
        "total_calls": total_calls,
        "record_count": len(records),
        "settled_at": _now_iso(),
        "auto_settlement": use_auto_settlement,
    }
    _write_json(DATA_ROOT / "usage" / f"_settlement_{billing_cycle}.json", settlement)

    for r in records:
        r["settlement_status"] = "已结算"
        r["_settled_at"] = _now_iso()
        write_usage(r.get("subscription_id", 0), r)
    return settlement


# ========== 调用审计 ==========


def write_audit(audit_id: int, audit_data: dict[str, Any]) -> None:
    """写入审计记录。"""
    audit_data["_logged_at"] = _now_iso()
    _write_json(DATA_ROOT / "audit" / f"{audit_id:06d}.json", audit_data)


def read_audit(audit_id: int) -> dict[str, Any]:
    return _read_json(DATA_ROOT / "audit" / f"{audit_id:06d}.json", {})


def list_audit(api_name: str | None = None, caller: str | None = None,
               start_time: str | None = None, end_time: str | None = None) -> list[dict[str, Any]]:
    """按条件查询审计记录。"""
    _ensure_dir(DATA_ROOT / "audit")
    records = []
    for f in sorted(DATA_ROOT.glob("audit/*.json")):
        record = _read_json(f)
        if not record:
            continue
        if api_name and record.get("api_name") != api_name:
            continue
        if caller and record.get("caller") != caller:
            continue
        if start_time and record.get("_logged_at", "") < start_time:
            continue
        if end_time and record.get("_logged_at", "") > end_time:
            continue
        records.append(record)
    return records


def archive_audit_records(before_date: str) -> dict[str, Any]:
    """归档指定日期之前的审计记录。"""
    records = list_audit()
    archived = [r for r in records if r.get("_logged_at", "")[:10] < before_date]
    archive_data = {
        "archive_date": _now_iso(),
        "before_date": before_date,
        "archived_count": len(archived),
        "records": archived,
    }
    _write_json(DATA_ROOT / "audit" / f"_archive_{before_date}.json", archive_data)
    # 标记已归档
    for r in archived:
        rid = r.get("audit_id", 0)
        r["status"] = "已归档"
        write_audit(rid, r)
    return archive_data


# ========== 导出任务 ==========


def write_export(export_id: int, export_data: dict[str, Any]) -> None:
    """写入导出任务结果。"""
    export_data["_created_at"] = export_data.get("_created_at", _now_iso())
    export_data["_updated_at"] = _now_iso()
    _write_json(DATA_ROOT / "exports" / f"{export_id:06d}.json", export_data)
    # 同时生成一份导出 CSV
    _write_export_csv(export_id, export_data)


def _write_export_csv(export_id: int, export_data: dict[str, Any]) -> None:
    """根据导出数据生成 CSV 文件头。"""
    columns = export_data.get("columns", [])
    rows = export_data.get("rows", [])
    csv_path = DATA_ROOT / "exports" / f"{export_id:06d}.csv"
    lines = [",".join(columns)]
    for row in rows:
        lines.append(",".join(str(v) for v in row))
    csv_path.write_text("\n".join(lines), encoding="utf-8")


def read_export(export_id: int) -> dict[str, Any]:
    return _read_json(DATA_ROOT / "exports" / f"{export_id:06d}.json", {})


def list_exports() -> list[dict[str, Any]]:
    _ensure_dir(DATA_ROOT / "exports")
    records = []
    for f in sorted(DATA_ROOT.glob("exports/[0-9]*.json")):
        # 只认当前版本指针文件，排除 .v{n}/.manifest/.partial 等附属文件
        if not f.stem.isdigit():
            continue
        record = _read_json(f)
        if record:
            records.append(record)
    return records


# ========== 导出版本与完成凭证 ==========


def export_checksum(columns: list[Any], rows: list[Any]) -> str:
    """对导出表头与数据行计算内容校验和，作为可核对的完成事实。"""
    payload = json.dumps({"columns": list(columns), "rows": rows},
                         ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _export_manifest_path(export_id: int) -> Path:
    return DATA_ROOT / "exports" / f"{export_id:06d}.manifest.json"


def _export_version_path(export_id: int, version: int) -> Path:
    return DATA_ROOT / "exports" / f"{export_id:06d}.v{version}.json"


def _export_partial_path(export_id: int) -> Path:
    return DATA_ROOT / "exports" / f"{export_id:06d}.partial.json"


def read_export_manifest(export_id: int) -> dict[str, Any]:
    """读取导出版本清单：最后完成版本、进行中的重发版本与历史版本。"""
    return _read_json(_export_manifest_path(export_id), {})


def write_partial_export(export_id: int, partial_data: dict[str, Any]) -> dict[str, Any]:
    """写入取数中途的半成品导出文件（无完成凭证）。

    任务中断时文件已落盘但取数未完成：manifest 只提升 pending_version，
    current_version 不变，归并侧据此判定输入凭证不完整。
    """
    manifest = read_export_manifest(export_id)
    current = int(manifest.get("current_version") or 0)
    pending = int(manifest.get("pending_version") or 0)
    partial_data["_partial"] = True
    partial_data["_started_at"] = _now_iso()
    _write_json(_export_partial_path(export_id), partial_data)
    manifest.update({
        "export_id": export_id,
        "current_version": current,
        "pending_version": max(current, pending) + 1,
    })
    manifest.setdefault("versions", [])
    _write_json(_export_manifest_path(export_id), manifest)
    return manifest


def write_export_version(export_id: int, export_data: dict[str, Any]) -> dict[str, Any]:
    """以新版本写入导出产物，并交付可核对的完成凭证（receipt）。

    版本单调递增；旧版本文件保留，已确认的归并结果仍可按当时版本回查。
    凭证包含：版本号、表头、行数、内容校验和、完成时间与真实来源。
    """
    manifest = read_export_manifest(export_id)
    current = int(manifest.get("current_version") or 0)
    pending = int(manifest.get("pending_version") or 0)
    # 中断时已预订的 pending_version 由本次完成承接；否则递增
    version = pending if pending > current else current + 1
    columns = list(export_data.get("columns") or [])
    rows = export_data.get("rows") or []
    receipt = {
        "export_id": export_id,
        "version": version,
        "task_no": export_data.get("task_no"),
        "api_name": export_data.get("api_name"),
        "query_params": export_data.get("query_params"),
        "columns": columns,
        "rows_count": len(rows),
        "checksum": export_checksum(columns, rows),
        "status": "已完成",
        "completed_at": _now_iso(),
    }
    version_doc = {
        **export_data,
        "version": version,
        "receipt": receipt,
        "_created_at": export_data.get("_created_at", _now_iso()),
        "_updated_at": _now_iso(),
    }
    _write_json(_export_version_path(export_id, version), version_doc)
    # 当前版本指针：保持既有 {id:06d}.json 指向最新完成版本
    _write_json(DATA_ROOT / "exports" / f"{export_id:06d}.json", version_doc)
    _write_export_csv(export_id, version_doc)
    # 重发完成，清掉中断留下的半成品文件
    partial = _export_partial_path(export_id)
    if partial.exists():
        partial.unlink()
    versions = manifest.setdefault("versions", [])
    versions.append({
        "version": version,
        "checksum": receipt["checksum"],
        "rows_count": len(rows),
        "completed_at": receipt["completed_at"],
    })
    manifest.update({
        "export_id": export_id,
        "current_version": version,
        "pending_version": None,
    })
    _write_json(_export_manifest_path(export_id), manifest)
    return receipt


def read_export_version(export_id: int, version: int | None = None) -> dict[str, Any]:
    """读取指定版本的导出产物；version=None 时读取当前完成版本。"""
    if version is None:
        return read_export(export_id)
    return _read_json(_export_version_path(export_id, version), {})


def read_export_receipt(export_id: int, version: int | None = None) -> dict[str, Any]:
    """读取导出完成凭证（可核对的完成事实）。"""
    doc = read_export_version(export_id, version)
    return doc.get("receipt", {}) if doc else {}


# ========== 归并运行记录 ==========


def next_merge_run_id() -> int:
    existing = [f for f in DATA_ROOT.glob("merge_runs/[0-9]*.json") if f.stem.isdigit()]
    return max((int(f.stem) for f in existing), default=0) + 1


def write_merge_run(run_id: int, run: dict[str, Any]) -> None:
    """写入一次归并运行记录（含空结果校验明细与确认版本）。"""
    _write_json(DATA_ROOT / "merge_runs" / f"{run_id:06d}.json", run)


def read_merge_run(run_id: int) -> dict[str, Any]:
    return _read_json(DATA_ROOT / "merge_runs" / f"{run_id:06d}.json", {})


def list_merge_runs() -> list[dict[str, Any]]:
    _ensure_dir(DATA_ROOT / "merge_runs")
    runs = []
    for f in sorted(DATA_ROOT.glob("merge_runs/[0-9]*.json")):
        if not f.stem.isdigit():
            continue
        run = _read_json(f)
        if run:
            runs.append(run)
    return runs


# ========== 模块数据快照 ==========


def write_module_snapshot(module: str, snapshot: dict[str, Any]) -> None:
    """各服务将自己的 domain_snapshot 写入共享目录，供跨模块消费。"""
    snapshot["_snapshot_at"] = _now_iso()
    _write_json(DATA_ROOT / module / "snapshot.json", snapshot)


def read_module_snapshot(module: str) -> dict[str, Any]:
    return _read_json(DATA_ROOT / module / "snapshot.json", {})


def list_module_snapshots() -> list[dict[str, Any]]:
    """读取所有模块的快照汇总。"""
    snapshots = []
    for module_dir in DATA_ROOT.iterdir():
        if module_dir.is_dir() and not module_dir.name.startswith("_"):
            snap = _read_json(module_dir / "snapshot.json")
            if snap:
                snapshots.append(snap)
    return snapshots