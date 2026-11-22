"""导出任务领域服务：管理数据导出任务的排队、执行和过期清理。

跨模块增强：
- complete 触发 call_audit 审计记录（业务能力增强）
- 多源归并 detect：export 完成后自动通知 data_merge

跨模块数据流：
- 消费 api_registry 快照 -> 核验导出API合法性
- 消费 data_filter 快照 -> 导出数据应用过滤规则
- 消费 field_mask 快照 -> 导出数据应用脱敏规则
- 消费 result_format 快照 -> 导出数据按模板格式化
- 写入 data/exports/ 共享目录 -> call_audit 消费导出审计
- 被 data_merge 消费 -> 多源导出归并
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class ExportTaskService(BaseModuleService):
    MODULE = "export_task"
    ENTITY = "导出任务"
    OPERATION_TERM = "导出"
    LIST_FIELDS = ["任务编号", "关联API", "查询参数", "导出格式", "数据行数", "文件大小", "过期时间", "任务状态"]
    REQUIRED_FIELDS = ["任务编号", "关联API", "查询参数"]
    STATUS_ORDER = ["排队中", "导出中", "已完成", "已失败"]
    ACTION_RULES = {"发起导出": "导出中", "确认完成": "已完成", "标记失败": "已失败"}
    NEGATIVE_ACTIONS = []

    def execute_export(self, export_id: int) -> dict[str, Any]:
        """执行导出任务：查询数据 → 过滤 → 脱敏 → 格式化 → 写入文件。"""
        entry = store.find(self.MODULE, export_id)
        if entry is None:
            return {"ok": False, "message": "任务不存在"}

        api_name = entry.get("关联API", "")

        # 从 api_registry 读取原始数据
        from app.services.data_manager import read_api_snapshot
        api_snap = read_api_snapshot()
        # 模拟查询数据
        source_rows = self._mock_query(api_name)

        # 应用 data_filter 过滤
        from app.services import service_for
        filter_svc = service_for("data_filter")
        if filter_svc and hasattr(filter_svc, "apply_filters"):
            source_rows = filter_svc.apply_filters(api_name, source_rows)

        # 应用 field_mask 脱敏
        mask_svc = service_for("field_mask")
        if mask_svc and hasattr(mask_svc, "mask_fields"):
            source_rows = mask_svc.mask_fields(api_name, source_rows)

        # 写入导出文件：版本化落盘并交付可核对的完成凭证；
        # 合法零行（如过滤规则删完所有行）也必须交付表头。
        from app.services.data_manager import write_export_version
        columns = list(source_rows[0].keys()) if source_rows else self._schema_columns(api_name)
        export_data = {
            "export_id": export_id,
            "task_no": entry.get("任务编号"),
            "api_name": api_name,
            "query_params": entry.get("查询参数"),
            "export_format": entry.get("导出格式", "json"),
            "rows_count": len(source_rows),
            "columns": columns,
            "rows": [[str(r.get(c, "")) for c in columns] for r in source_rows],
            "status": "已完成",
        }
        receipt = write_export_version(export_id, export_data)

        # 写审计记录
        from app.services.data_manager import write_audit
        audit_id = self._next_audit_id()
        write_audit(audit_id, {
            "audit_id": audit_id,
            "api_name": api_name,
            "caller": "export_task",
            "event": "export_completed",
            "export_id": export_id,
            "rows_count": len(source_rows),
            "export_version": receipt.get("version"),
            "checksum": receipt.get("checksum"),
        })

        entry["status"] = "已完成"
        entry["数据行数"] = str(len(source_rows))
        entry["导出版本"] = str(receipt.get("version"))
        return {"ok": True, "export_id": export_id, "rows_count": len(source_rows),
                "receipt": receipt}

    def _schema_columns(self, api_name: str) -> list[str]:
        """零行导出仍要交付表头：按数据源 schema 给出列清单。"""
        sample = self._mock_query(api_name)
        return list(sample[0].keys()) if sample else []

    def simulate_interrupted_export(self, export_id: int,
                                    columns: list[str] | None = None) -> dict[str, Any]:
        """模拟取数中途任务中断：留下写到半程的空文件（无完成凭证）。

        半成品文件只提升 manifest 的 pending_version，不交付完成事实；
        归并侧据此判定该侧输入凭证不完整。
        """
        entry = store.find(self.MODULE, export_id)
        if entry is None:
            return {"ok": False, "message": "任务不存在"}

        from app.services.data_manager import write_partial_export
        api_name = entry.get("关联API", "")
        manifest = write_partial_export(export_id, {
            "export_id": export_id,
            "task_no": entry.get("任务编号"),
            "api_name": api_name,
            "query_params": entry.get("查询参数"),
            "columns": columns if columns is not None else self._schema_columns(api_name),
            "rows": [],
            "status": "导出中",
            "interrupted": True,
        })
        entry["status"] = "导出中"
        return {"ok": True, "export_id": export_id,
                "pending_version": manifest.get("pending_version"),
                "message": "导出中断，留下半成品文件"}

    def _mock_query(self, api_name: str) -> list[dict[str, Any]]:
        """模拟从数据源查询结果。"""
        return [
            {"id": 1, "name": "张三", "phone": "13800138000", "email": "zhangsan@example.com", "amount": 1250.50},
            {"id": 2, "name": "李四", "phone": "13900139000", "email": "lisi@example.com", "amount": 2340.00},
            {"id": 3, "name": "王五", "phone": "13700137000", "email": "wangwu@example.com", "amount": 890.75},
        ]

    def _next_audit_id(self) -> int:
        from app.services.data_manager import DATA_ROOT
        existing = list(DATA_ROOT.glob("audit/[0-9]*.json"))
        return max((int(f.stem) for f in existing), default=0) + 1

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"

        if action == "发起导出":
            result = self.execute_export(entry_id)
            if not result.get("ok"):
                return None, result.get("message", "导出失败")
            entry["status"] = "已完成"
            entry["pending"] = False
            from app.services.data_manager import write_module_snapshot
            write_module_snapshot(self.MODULE, self.domain_snapshot())
            receipt = result.get("receipt") or {}
            return entry, (f"导出任务已完成，共{result.get('rows_count', 0)}行"
                           f"（版本 v{receipt.get('version', 1)}）")

        result = super().run_action(entry_id, action)
        from app.services.data_manager import write_module_snapshot
        write_module_snapshot(self.MODULE, self.domain_snapshot())
        return result

    def domain_snapshot(self) -> dict[str, Any]:
        rows = store.rows(self.MODULE)
        export_list = []
        for row in rows:
            export_list.append({
                "id": row.get("id"),
                "task_no": row.get("任务编号"),
                "api": row.get("关联API"),
                "query_params": row.get("查询参数"),
                "format": row.get("导出格式"),
                "rows": row.get("数据行数"),
                "file_size": row.get("文件大小"),
                "expire_at": row.get("过期时间"),
                "task_status": row.get("任务状态"),
                "status": row.get("status"),
            })
        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": f"{self.ENTITY}数据集",
            "count": len(export_list),
            "exports": export_list,
            "completed_count": sum(1 for r in export_list if r.get("status") == "已完成"),
            "failed_count": sum(1 for r in export_list if r.get("status") == "已失败"),
        }