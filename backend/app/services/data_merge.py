"""数据归并领域服务：管理多API数据源的归并、冲突处理和去重策略。

跨模块数据流：
- 消费 api_registry 快照 -> 归并源API核验
- 消费 data_filter 快照 -> 各源数据归并前先过滤
- 被 export_task 消费 -> 归并后数据导出
- 归并结果写入共享快照

空导出校验：
- 导出任务交付可核对的完成凭证（版本/表头/行数/校验和/完成时间）
- 归并提交时对每侧导出生成空结果校验明细，区分合法零行与中断半成品
- 没有完整输入凭证时不得发布成功结果；已确认的空结果可按版本回查
- 对待补侧重试时沿前次记录核对恢复后的文件和表头
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class DataMergeService(BaseModuleService):
    MODULE = "data_merge"
    ENTITY = "归并规则"
    OPERATION_TERM = "去重"
    LIST_FIELDS = ["规则编号", "主API", "从API", "归并键", "冲突策略", "去重策略", "结果排序", "规则版本"]
    REQUIRED_FIELDS = ["规则编号", "主API", "从API"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用规则": "已停用"}
    NEGATIVE_ACTIONS = ["停用规则"]

    # ========== 空导出校验与归并发布 ==========

    def validate_export_side(self, export_id: int | None, side: str) -> dict[str, Any]:
        """校验一侧导出输入，生成空结果校验明细（该侧为何可用或待补）。

        合法零行必须同时满足：已完成的取数（完成凭证）、可关联的真实来源
        与表头、版本无冲突（不存在未完成的重发）、内容校验和一致。
        """
        from app.services.data_manager import (
            export_checksum, read_export_manifest, read_export_version)

        detail: dict[str, Any] = {
            "side": side,
            "export_id": export_id,
            "usable": False,
            "pending_reasons": [],
            "version": None,
            "seen_version": None,
            "columns": [],
            "rows_count": None,
            "zero_row": False,
            "zero_row_legit": False,
            "source_api": "",
            "completed_at": None,
            "checksum": None,
        }
        if export_id is None:
            detail["pending_reasons"].append("导出文件缺失：未指定导出输入")
            return detail

        manifest = read_export_manifest(export_id)
        current = int(manifest.get("current_version") or 0)
        pending = manifest.get("pending_version")
        # 校验时最早可见的版本：进行中的重发优先于最后确认版本
        detail["seen_version"] = int(pending) if pending else (current or None)

        if current == 0:
            if pending:
                detail["pending_reasons"].append(
                    f"取数未完成：任务中断留下半成品文件（v{pending} 未交付完成凭证）")
            else:
                detail["pending_reasons"].append("导出文件缺失：没有任何已完成的取数版本")
            return detail

        doc = read_export_version(export_id, current)
        receipt = doc.get("receipt") or {}
        columns = list(doc.get("columns") or [])
        rows = doc.get("rows") or []
        detail.update({
            "version": current,
            "columns": columns,
            "rows_count": len(rows),
            "zero_row": len(rows) == 0,
            "source_api": receipt.get("api_name") or doc.get("api_name") or "",
            "completed_at": receipt.get("completed_at"),
            "checksum": receipt.get("checksum"),
        })

        if not receipt:
            detail["pending_reasons"].append("缺少完成凭证：导出任务未交付可核对的完成事实")
        else:
            if list(receipt.get("columns") or []) != columns:
                detail["pending_reasons"].append("表头不一致：完成凭证的表头与文件实际表头不符")
            if receipt.get("checksum") != export_checksum(columns, rows):
                detail["pending_reasons"].append("凭证校验失败：文件内容与完成凭证的校验和不一致")

        if pending and int(pending) > current:
            detail["pending_reasons"].append(
                f"重发未完成：最早可见 v{pending} 的表头，最后确认为 v{current}，存在版本冲突")

        if detail["pending_reasons"]:
            return detail

        detail["usable"] = True
        detail["zero_row_legit"] = len(rows) == 0
        return detail

    def submit_merge(self, rule_id: int, primary_export_id: int,
                     secondary_export_id: int) -> dict[str, Any]:
        """提交归并：校验两侧导出输入凭证，生成空结果校验明细。

        两侧都具备完整输入凭证才发布成功结果；任一侧待补则不发布，
        明细里记下每侧为何可用或待补。
        """
        from app.services.data_manager import next_merge_run_id, write_merge_run
        primary = self.validate_export_side(primary_export_id, "primary")
        secondary = self.validate_export_side(secondary_export_id, "secondary")
        run_id = next_merge_run_id()
        run = self._build_run(run_id, rule_id, primary, secondary, attempt=1)
        write_merge_run(run_id, run)
        return self._run_result(run)

    def retry_merge(self, run_id: int) -> dict[str, Any]:
        """对待补的归并发起重试：沿前次记录核对恢复后的文件和表头。"""
        from app.services.data_manager import read_merge_run, write_merge_run
        run = read_merge_run(run_id)
        if not run:
            return {"ok": False, "message": f"归并运行 {run_id} 不存在"}
        if run.get("published"):
            return {"ok": True, "run_id": run_id, "status": "已确认",
                    "message": "归并已确认，无需重试", "detail": run}

        prev_sides = run.get("sides", {})
        new_sides: dict[str, dict[str, Any]] = {}
        for side in ("primary", "secondary"):
            prev = prev_sides.get(side, {})
            detail = self.validate_export_side(prev.get("export_id"), side)
            prev_columns = list(prev.get("columns") or [])
            recovered_columns = list(detail.get("columns") or [])
            # 沿前次记录核对恢复后的文件和表头
            detail["recheck"] = {
                "previous_seen_version": prev.get("seen_version"),
                "previous_version": prev.get("version"),
                "previous_columns": prev_columns,
                "recovered_version": detail.get("version"),
                "recovered_columns": recovered_columns,
                "header_matches_previous": (not prev_columns) or prev_columns == recovered_columns,
                "version_advanced": (detail.get("version") or 0) >= (prev.get("seen_version") or 0),
            }
            new_sides[side] = detail

        attempt = len(run.get("attempts", [])) + 1
        new_run = self._build_run(run_id, run.get("rule_id"), new_sides["primary"],
                                  new_sides["secondary"], attempt=attempt, base=run)
        write_merge_run(run_id, new_run)
        return self._run_result(new_run)

    def get_merge_run(self, run_id: int) -> dict[str, Any]:
        """按运行ID回查归并结果；已确认的空结果可按当时确认的版本回查导出内容。"""
        from app.services.data_manager import read_merge_run
        return read_merge_run(run_id)

    def _rows_of(self, side: dict[str, Any]) -> list[dict[str, Any]]:
        """按校验确认的版本读取一侧导出的数据行。"""
        from app.services.data_manager import read_export_version
        doc = read_export_version(side["export_id"], side["version"])
        columns = list(doc.get("columns") or [])
        return [{col: val for col, val in zip(columns, row_vals)}
                for row_vals in doc.get("rows") or []]

    def _build_run(self, run_id: int, rule_id: int,
                   primary: dict[str, Any], secondary: dict[str, Any],
                   attempt: int, base: dict[str, Any] | None = None) -> dict[str, Any]:
        """组装一次归并运行记录；两侧可用时才执行归并并发布。"""
        from app.services.data_manager import now_iso
        usable = primary["usable"] and secondary["usable"]
        merged_count = None
        if usable:
            merged = self.merge_and_audit(rule_id, self._rows_of(primary),
                                          self._rows_of(secondary))
            merged_count = len(merged)
        now = now_iso()
        attempts = list(base.get("attempts", [])) if base else []
        attempts.append({
            "attempt": attempt,
            "at": now,
            "status": "已确认" if usable else "待补",
            "primary_usable": primary["usable"],
            "secondary_usable": secondary["usable"],
        })
        return {
            "run_id": run_id,
            "rule_id": rule_id,
            "status": "已确认" if usable else "待补",
            "published": usable,
            "created_at": base.get("created_at") if base else now,
            "updated_at": now,
            "confirmed_at": (now if usable else None) if not base else (
                base.get("confirmed_at") or (now if usable else None)),
            "confirmed_versions": (
                {"primary": primary["version"], "secondary": secondary["version"]}
                if usable else None),
            "merged_count": merged_count,
            "sides": {"primary": primary, "secondary": secondary},
            "attempts": attempts,
        }

    def _run_result(self, run: dict[str, Any]) -> dict[str, Any]:
        """把归并运行记录转成对调用方的结果。"""
        if run["published"]:
            versions = run.get("confirmed_versions") or {}
            message = (f"归并已确认（run {run['run_id']}），采用版本 "
                       f"主v{versions.get('primary')}/从v{versions.get('secondary')}，"
                       f"结果 {run.get('merged_count')} 行")
        else:
            reasons = []
            for side_name, side in (run.get("sides") or {}).items():
                for reason in side.get("pending_reasons", []):
                    reasons.append(f"{side_name}: {reason}")
            message = "归并待补：" + "；".join(reasons)
        return {"ok": bool(run["published"]), "run_id": run["run_id"],
                "status": run["status"], "message": message, "detail": run}

    def merge_sources(self, rule_id: int, source_a_rows: list[dict[str, Any]],
                      source_b_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """按归并规则合并两个数据源。"""
        rule = store.find(self.MODULE, rule_id)
        if rule is None or rule.get("status") != "已生效":
            return source_a_rows + source_b_rows

        merge_key = rule.get("归并键", "id")
        conflict_policy = rule.get("冲突策略", "主源优先")
        dedup_policy = rule.get("去重策略", "全保留")

        merged = {}
        for row in source_a_rows:
            key = str(row.get(merge_key, ""))
            merged[f"a_{key}"] = {"_source": "primary", **row}

        for row in source_b_rows:
            key = str(row.get(merge_key, ""))
            pk = f"a_{key}"
            if pk in merged:
                if conflict_policy == "主源优先":
                    continue
                elif conflict_policy == "从源覆盖":
                    merged[pk] = {"_source": "secondary", **row}
                elif conflict_policy == "合并字段":
                    merged[pk].update({"from_secondary": row})
            else:
                merged[f"b_{key}"] = {"_source": "secondary", **row}

        result = list(merged.values())

        # 去重
        if dedup_policy == "按键去重":
            seen = set()
            deduped = []
            for row in result:
                key = str(row.get(merge_key, ""))
                if key not in seen:
                    seen.add(key)
                    deduped.append(row)
            result = deduped

        # 排序
        sort_field = rule.get("结果排序", "")
        if sort_field:
            result = sorted(result, key=lambda r: str(r.get(sort_field, "")))

        return result

    def merge_and_audit(self, rule_id: int, source_a_rows: list[dict[str, Any]],
                        source_b_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """归并两个数据源并写入审计记录。

        跨模块增强：归并完成后自动写入call_audit审计。
        """
        merged = self.merge_sources(rule_id, source_a_rows, source_b_rows)
        rule = store.find(self.MODULE, rule_id)

        # 联动增强：归并结果写入审计
        from app.services.data_manager import write_audit, DATA_ROOT
        audit_id = max((int(f.stem) for f in DATA_ROOT.glob("audit/[0-9]*.json")), default=0) + 1
        write_audit(audit_id, {
            "audit_id": audit_id,
            "api_name": rule.get("主API", "") if rule else "",
            "caller": "data_merge",
            "event": "merge_completed",
            "primary_count": len(source_a_rows),
            "secondary_count": len(source_b_rows),
            "merged_count": len(merged),
            "rule_no": rule.get("规则编号", "") if rule else "",
        })
        return merged

    def merge_from_exports(self, rule_id: int) -> list[dict[str, Any]]:
        """从导出产物直接读取两个数据源并进行归并。

        跨模块增强：被 export_task 完成后调用，实现导出->归并->审计链路。
        """
        from app.services.data_manager import list_exports
        exports = list_exports()
        exports.sort(key=lambda x: x.get("_created_at", ""), reverse=True)
        source_a = exports[0] if len(exports) > 0 else {}
        source_b = exports[1] if len(exports) > 1 else {}

        rows_a = []
        rows_b = []
        cols_a = source_a.get("columns", [])
        cols_b = source_b.get("columns", [])
        for row_vals in source_a.get("rows", []):
            row = {col: val for col, val in zip(cols_a, row_vals)} if cols_a else {}
            rows_a.append(row)
        for row_vals in source_b.get("rows", []):
            row = {col: val for col, val in zip(cols_b, row_vals)} if cols_b else {}
            rows_b.append(row)

        return self.merge_and_audit(rule_id, rows_a, rows_b)

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"

        if action == "标记生效":
            from app.services.data_manager import read_api_snapshot
            registry = read_api_snapshot()
            primary_api = entry.get("主API", "")
            secondary_api = entry.get("从API", "")
            api_names = {v.get("api_name") for v in (registry.values() if isinstance(registry, dict) else []) if isinstance(v, dict)}
            if primary_api not in api_names and secondary_api not in api_names:
                return None, "至少一个源API尚未注册"

        result = super().run_action(entry_id, action)
        from app.services.data_manager import write_module_snapshot
        write_module_snapshot(self.MODULE, self.domain_snapshot())
        return result

    def domain_snapshot(self) -> dict[str, Any]:
        rows = store.rows(self.MODULE)
        merge_list = []
        for row in rows:
            merge_list.append({
                "id": row.get("id"),
                "rule_no": row.get("规则编号"),
                "primary_api": row.get("主API"),
                "secondary_api": row.get("从API"),
                "merge_key": row.get("归并键"),
                "conflict_policy": row.get("冲突策略"),
                "dedup_policy": row.get("去重策略"),
                "sort": row.get("结果排序"),
                "version": row.get("规则版本"),
                "status": row.get("status"),
            })
        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": f"{self.ENTITY}数据集",
            "count": len(merge_list),
            "merges": merge_list,
            "active_count": sum(1 for r in merge_list if r.get("status") == "已生效"),
        }