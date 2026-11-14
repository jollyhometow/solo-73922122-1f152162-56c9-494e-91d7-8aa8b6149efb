"""请求/响应模型。"""
from __future__ import annotations

import datetime as _dt
import re
from typing import Any

from pydantic import BaseModel, Field, field_validator

_IDENT_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]{0,62}$")
MAX_CANDIDATES = 3
MAX_SQL_LENGTH = 20_000

# 快照导出：单个窗口最多冻结 5000 条原始记录，每页最多 200 条。
MAX_SNAPSHOT_ROWS = 5_000
MAX_SNAPSHOT_PAGE_SIZE = 200
DEFAULT_SNAPSHOT_PAGE_SIZE = 200
MAX_EXPORT_KEY_LENGTH = 128


class IndexCandidate(BaseModel):
    """一个 B-tree 候选索引：表名 + 有序列名（顺序即索引键顺序）。"""

    table: str = Field(..., description="表名，可带 schema 前缀，如 public.package_scan")
    columns: list[str] = Field(
        ..., min_length=1, max_length=8, description="有序索引列名，顺序即 B-tree 键顺序"
    )

    @field_validator("table")
    @classmethod
    def _validate_table(cls, v: str) -> str:
        parts = [p.strip() for p in v.split(".")]
        if len(parts) not in (1, 2) or any(not _IDENT_RE.match(p) for p in parts):
            raise ValueError("表名必须是合法标识符，形如 name 或 schema.name")
        return ".".join(parts)

    @field_validator("columns")
    @classmethod
    def _validate_columns(cls, v: list[str]) -> list[str]:
        cleaned = [c.strip() for c in v]
        if any(not _IDENT_RE.match(c) for c in cleaned):
            raise ValueError("列名必须是合法标识符（字母/下划线开头，最长 63）")
        if len(set(cleaned)) != len(cleaned):
            raise ValueError("同一候选内列名不能重复")
        return cleaned


class EvaluateRequest(BaseModel):
    sql_text: str | None = Field(None, description="只读 SELECT；与 sample_id 二选一")
    sample_id: str | None = Field(None, description="预置样例 ID，见 GET /samples")
    candidates: list[IndexCandidate] = Field(
        ..., min_length=1, max_length=MAX_CANDIDATES, description="最多 3 个 B-tree 候选"
    )

    @field_validator("sql_text")
    @classmethod
    def _validate_sql_text(cls, v: str | None) -> str | None:
        if v is None:
            return None
        v = v.strip()
        if not v:
            raise ValueError("sql_text 不能为空字符串")
        if len(v) > MAX_SQL_LENGTH:
            raise ValueError(f"SQL 长度超过上限 {MAX_SQL_LENGTH} 字符")
        return v

    @field_validator("candidates")
    @classmethod
    def _validate_unique_candidates(cls, v: list[IndexCandidate]) -> list[IndexCandidate]:
        keys = [(c.table, tuple(c.columns)) for c in v]
        if len(set(keys)) != len(keys):
            raise ValueError("候选索引之间不能完全重复（相同表名 + 相同有序列名）")
        return v


class CandidateResult(BaseModel):
    indexrelid: int
    index_name: str
    definition: str
    used_in_plan: bool


class EvaluateResponse(BaseModel):
    baseline_plan: dict[str, Any]
    baseline_total_cost: float
    hypothetical_plan: dict[str, Any]
    hypothetical_total_cost: float
    cost_delta: float
    improvement_ratio: float | None
    plan_changed: bool
    adopted_candidates: list[CandidateResult]
    rejected_candidates: list[CandidateResult]
    # 观测字段：证明并发隔离与基线不继承旧候选
    session_hypothetical_indexes_before_baseline: int
    session_hypothetical_indexes_after_reset: int


class HealthResponse(BaseModel):
    status: str
    pool: dict[str, Any]
    db: dict[str, Any]
    snapshots: dict[str, Any]


# ---------------- 快照导出 ----------------


class SnapshotCreateRequest(BaseModel):
    """创建（或复用）一个冻结快照。"""

    export_key: str = Field(
        ...,
        min_length=1,
        max_length=MAX_EXPORT_KEY_LENGTH,
        description="客户端导出键：有效期内同键同参数复用同一快照，换参数返回冲突",
    )
    warehouse_id: int = Field(..., ge=1, description="仓号（正整数）")
    business_date: _dt.date = Field(
        ..., description="UTC 业务日（YYYY-MM-DD）；窗口为该日 18:00 至次日 06:00，左闭右开"
    )
    page_size: int = Field(
        DEFAULT_SNAPSHOT_PAGE_SIZE,
        ge=1,
        le=MAX_SNAPSHOT_PAGE_SIZE,
        description=f"每页条数，1~{MAX_SNAPSHOT_PAGE_SIZE}",
    )

    @field_validator("export_key")
    @classmethod
    def _validate_export_key(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("export_key 不能为空或纯空白")
        if any(ord(ch) < 0x20 or ord(ch) == 0x7F for ch in v):
            raise ValueError("export_key 不能包含控制字符")
        return v


class SnapshotPageResponse(BaseModel):
    snapshot_id: str = Field(..., description="快照号；后续翻页使用")
    export_key: str
    warehouse_id: int
    business_date: _dt.date
    window_start: _dt.datetime
    window_end: _dt.datetime
    created_at: _dt.datetime
    expires_at: _dt.datetime
    total_rows: int = Field(..., description="本次快照冻结的记录数（<= 5000）")
    truncated: bool = Field(
        ..., description="窗口内记录超过 5000 条时为 True，仅冻结前 5000 条"
    )
    page_size: int
    offset: int = Field(..., description="本页第一条在快照中的下标（从 0 开始）")
    returned_rows: int
    has_more: bool
    next_cursor: str | None = Field(
        None, description="下一页游标；无下一页时为 null"
    )
    reused: bool = Field(
        ..., description="本次是否复用了同键同参数的既有构建结果"
    )
    records: list[dict[str, Any]] = Field(
        ..., description="按 scan_time、scan_id 升序排列的原始扫描记录"
    )
