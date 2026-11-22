"""空导出校验与归并发布：区分合法零行与取数中断的半成品。

业务场景：
1. 一侧导出的过滤规则确实删完了所有行 → 合法零行（订单号、货值两列表头）；
2. 另一侧任务中断留下写到半程的空文件，随后重发同名文件；
3. 重发未完成时提交归并 → 不得发布成功结果，空结果校验明细记录待补原因；
4. 补齐第二边后沿前次记录重试 → 采用恢复后的新版本，归并确认；
5. 已确认的空结果仍可按当时确认的版本回查。
"""

import os
import shutil
import sys
import tempfile
from pathlib import Path

# 设置测试环境
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


class TestEmptyExportMerge:
    """空导出校验明细与归并发布门禁。"""

    def setup_method(self):
        """每个测试前重置 store，并把 DATA_ROOT 切到独立临时目录。"""
        from app.seed import SEED_ROWS
        from app.store import store as s
        s._tables = {name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()}

        from app.services import data_manager
        self._saved_root = data_manager.DATA_ROOT
        self._tmp = Path(tempfile.mkdtemp(prefix='empty_export_merge_'))
        data_manager.DATA_ROOT = self._tmp

    def teardown_method(self):
        from app.services import data_manager
        data_manager.DATA_ROOT = self._saved_root
        shutil.rmtree(self._tmp, ignore_errors=True)

    # ----------------------------------------------------------
    # 造数辅助
    # ----------------------------------------------------------

    def _legit_zero_export(self, export_id: int = 101) -> dict:
        """造一份标题为订单号、货值的零行导出（过滤规则删完所有行的合法产物）。"""
        from app.services.data_manager import write_export_version
        return write_export_version(export_id, {
            "export_id": export_id,
            "task_no": f"EXPO-ZERO-{export_id}",
            "api_name": "订单导出API",
            "query_params": "货值<0",
            "export_format": "csv",
            "columns": ["订单号", "货值"],
            "rows": [],
            "status": "已完成",
        })

    # ----------------------------------------------------------
    # 导出任务侧：可核对的完成事实
    # ----------------------------------------------------------

    def test_legit_zero_row_export_receipt(self):
        """合法零行导出：完成凭证记录表头、行数与校验和，可按版本回查。"""
        from app.services.data_manager import read_export_receipt, read_export_version

        receipt = self._legit_zero_export(101)
        assert receipt["version"] == 1
        assert receipt["columns"] == ["订单号", "货值"]
        assert receipt["rows_count"] == 0
        assert receipt["checksum"]
        assert receipt["completed_at"]

        # 按版本回查导出内容与凭证
        doc = read_export_version(101, 1)
        assert doc["columns"] == ["订单号", "货值"]
        assert doc["rows"] == []
        assert read_export_receipt(101, 1)["checksum"] == receipt["checksum"]

        # 归并侧判定为合法零行
        from app.services.data_merge import DataMergeService
        detail = DataMergeService().validate_export_side(101, "primary")
        assert detail["usable"] is True
        assert detail["zero_row_legit"] is True
        assert detail["columns"] == ["订单号", "货值"]
        assert detail["source_api"] == "订单导出API"

    def test_interrupted_partial_export_has_no_receipt(self):
        """写到半程的空文件：无完成凭证，校验判为待补。"""
        from app.services.data_manager import read_export_manifest, write_partial_export
        from app.services.data_merge import DataMergeService

        write_partial_export(303, {
            "export_id": 303,
            "task_no": "EXPO-HALF",
            "api_name": "订单导出API-从",
            "columns": ["订单号", "货值"],
            "rows": [],
            "status": "导出中",
            "interrupted": True,
        })
        manifest = read_export_manifest(303)
        assert manifest["current_version"] == 0
        assert manifest["pending_version"] == 1

        detail = DataMergeService().validate_export_side(303, "secondary")
        assert detail["usable"] is False
        assert detail["zero_row_legit"] is False
        assert any("取数未完成" in r for r in detail["pending_reasons"])

    def test_execute_export_zero_rows_still_delivers_header(self):
        """导出任务被过滤规则删完所有行时，仍交付表头与完成凭证。"""
        from app.services import service_for

        # 过滤规则：条件不匹配任何行 → 全部删除
        filter_svc = service_for("data_filter")
        fentry, missing = filter_svc.create_entry({
            "规则编号": "FILT-ZERO-1",
            "关联API": "订单过滤API",
            "过滤字段": "name",
            "过滤条件": "不存在的人XYZ",
            "空值处理": "保留空值",
        })
        assert not missing
        fentry["status"] = "已生效"

        export_svc = service_for("export_task")
        eentry, _ = export_svc.create_entry({
            "任务编号": "EXPO-ZERO-FILT",
            "关联API": "订单过滤API",
            "查询参数": "all",
        })
        result = export_svc.execute_export(eentry["id"])
        assert result["ok"]
        receipt = result["receipt"]
        assert receipt["rows_count"] == 0
        # 零行也要交付表头（mock 数据源 schema）
        assert receipt["columns"] == ["id", "name", "phone", "email", "amount"]
        assert receipt["version"] == 1

        # 该导出在归并侧判为合法零行
        from app.services.data_merge import DataMergeService
        detail = DataMergeService().validate_export_side(eentry["id"], "primary")
        assert detail["usable"] is True
        assert detail["zero_row_legit"] is True

    # ----------------------------------------------------------
    # 归并侧：空结果校验明细与发布门禁
    # ----------------------------------------------------------

    def test_merge_never_publishes_without_full_credentials(self):
        """两侧都缺凭证时不得发布成功结果，明细记录待补原因。"""
        from app.services.data_merge import DataMergeService

        merge = DataMergeService()
        result = merge.submit_merge(rule_id=3, primary_export_id=901,
                                    secondary_export_id=902)
        assert result["ok"] is False
        assert result["status"] == "待补"
        for side in ("primary", "secondary"):
            side_detail = result["detail"]["sides"][side]
            assert side_detail["usable"] is False
            assert side_detail["pending_reasons"]
        assert result["detail"]["published"] is False

    def test_merge_blocked_until_republished_side_completes(self):
        """完整场景：重发期间提交待补 → 补齐后重试确认 → 按版本回查。"""
        from app.services.data_manager import (
            read_export_version, write_export_version, write_partial_export)
        from app.services.data_merge import DataMergeService

        # 1) 侧A：合法零行导出（订单号、货值两列表头）
        receipt_a = self._legit_zero_export(101)
        assert receipt_a["rows_count"] == 0

        # 2) 侧B：先完成 v1，随后任务中断留下写到半程的空文件（重发 v2 未完成）
        write_export_version(202, {
            "export_id": 202,
            "task_no": "EXPO-B",
            "api_name": "订单导出API-从",
            "query_params": "all",
            "columns": ["订单号", "货值"],
            "rows": [["A0001", "12.50"]],
            "status": "已完成",
        })
        write_partial_export(202, {
            "export_id": 202,
            "task_no": "EXPO-B",
            "api_name": "订单导出API-从",
            "columns": ["订单号", "货值"],
            "rows": [],
            "status": "导出中",
            "interrupted": True,
        })

        merge = DataMergeService()

        # 3) 重发期间提交归并 → 不得发布成功结果
        first = merge.submit_merge(rule_id=3, primary_export_id=101,
                                   secondary_export_id=202)
        assert first["ok"] is False
        assert first["status"] == "待补"
        run_id = first["run_id"]

        sides = first["detail"]["sides"]
        # 侧A：合法零行可用，关联真实来源、表头与已完成的取数
        primary = sides["primary"]
        assert primary["usable"] is True
        assert primary["zero_row_legit"] is True
        assert primary["columns"] == ["订单号", "货值"]
        assert primary["source_api"] == "订单导出API"
        assert primary["completed_at"]
        # 侧B：待补——最早可见 v2 的表头与最后确认的 v1 存在版本冲突
        secondary = sides["secondary"]
        assert secondary["usable"] is False
        assert secondary["seen_version"] == 2
        assert secondary["version"] == 1
        assert any("重发未完成" in r for r in secondary["pending_reasons"])

        # 归并运行未发布，无确认版本
        run = merge.get_merge_run(run_id)
        assert run["published"] is False
        assert run["confirmed_versions"] is None
        assert len(run["attempts"]) == 1

        # 4) 补齐第二边：重发完成 → v2（确认仍是零行）
        receipt_b2 = write_export_version(202, {
            "export_id": 202,
            "task_no": "EXPO-B",
            "api_name": "订单导出API-从",
            "query_params": "all",
            "columns": ["订单号", "货值"],
            "rows": [],
            "status": "已完成",
        })
        assert receipt_b2["version"] == 2

        # 沿前次记录重试 → 核对恢复后的文件和表头
        second = merge.retry_merge(run_id)
        assert second["ok"] is True, second["message"]
        confirmed = second["detail"]
        assert confirmed["status"] == "已确认"
        assert confirmed["published"] is True
        assert confirmed["confirmed_versions"] == {"primary": 1, "secondary": 2}
        # 两侧均为合法零行 → 归并结果是合法的空结果
        assert confirmed["merged_count"] == 0
        assert len(confirmed["attempts"]) == 2

        recheck = confirmed["sides"]["secondary"]["recheck"]
        assert recheck["previous_seen_version"] == 2
        assert recheck["recovered_version"] == 2
        assert recheck["header_matches_previous"] is True
        assert recheck["version_advanced"] is True

        # 5) 已确认的空结果仍可按它的版本回查（之后重发 v3 不影响）
        write_export_version(202, {
            "export_id": 202,
            "task_no": "EXPO-B",
            "api_name": "订单导出API-从",
            "query_params": "all",
            "columns": ["订单号", "货值"],
            "rows": [["A0002", "88.00"]],
            "status": "已完成",
        })
        doc_v2 = read_export_version(202, confirmed["confirmed_versions"]["secondary"])
        assert doc_v2["version"] == 2
        assert doc_v2["rows"] == []
        assert doc_v2["columns"] == ["订单号", "货值"]
        # 当前版本已前进到 v3
        assert read_export_version(202)["version"] == 3
        # 运行记录里的确认版本不变
        assert merge.get_merge_run(run_id)["confirmed_versions"] == {
            "primary": 1, "secondary": 2}
