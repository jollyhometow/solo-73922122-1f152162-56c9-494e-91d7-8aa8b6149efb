"""FastAPI 入口：HypoPG 候选索引成本评估 API。

仅暴露只读评估接口；查询不执行（EXPLAIN 无 ANALYZE），索引不落地（HypoPG 会话级假想）。
"""
from __future__ import annotations

import logging
import uuid

from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse
from psycopg.rows import dict_row

from .config import get_settings
from .db import TwoConnectionPool
from .samples import SAMPLES
from .schemas import (
    EvaluateRequest,
    EvaluateResponse,
    HealthResponse,
    MAX_SNAPSHOT_PAGE_SIZE,
    SnapshotCreateRequest,
    SnapshotPageResponse,
)
from .service import EvaluationError, evaluate
from .snapshot import (
    InvalidCursorError,
    SnapshotBuildError,
    SnapshotCapacityError,
    SnapshotConflictError,
    SnapshotGoneError,
    SnapshotManager,
)
from .validation import SQLValidationError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("hypopg_api")

settings = get_settings()
pool = TwoConnectionPool(settings)
snapshots = SnapshotManager(pool, settings)

app = FastAPI(
    title="夜间包裹扫描查询 · 候选索引成本评估 & 快照导出 API",
    description=(
        "**候选索引成本评估**：提交只读 SELECT 与最多 3 个 B-tree 候选索引"
        "（表名 + 有序列名），基于 PostgreSQL + HypoPG 返回无候选/启用全部候选的 "
        "EXPLAIN JSON、总成本对比与被采纳候选。只估算：不执行查询，也不创建真实索引。\n\n"
        "**快照导出**：提交客户端导出键 + 仓号 + UTC 业务日，冻结该仓当日 "
        "18:00 至次日 06:00（左闭右开）内最多 5000 条原始扫描记录，"
        "按 scan_time、scan_id 排序，每页最多 200 条。"
        "首次请求返回快照号与首批数据，后续用游标读取同一份冻结结果；"
        "快照保留 10 分钟，到期或服务重启后旧游标返回 410。"
    ),
    version="1.1.0",
)


@app.on_event("startup")
async def _startup() -> None:
    await pool.open()
    snapshots.start()
    log.info(
        "连接池已就绪，物理连接上限 = %s；快照保留 %s 秒",
        settings.pool_max_connections, settings.snapshot_ttl_seconds,
    )


@app.on_event("shutdown")
async def _shutdown() -> None:
    await snapshots.stop()
    await pool.close()
    log.info("连接池与快照管理器已关闭")


@app.exception_handler(SQLValidationError)
async def _sql_validation_handler(_: Request, exc: SQLValidationError):
    return JSONResponse(status_code=400, content={"error": "invalid_sql", "detail": str(exc)})


@app.exception_handler(EvaluationError)
async def _evaluation_handler(_: Request, exc: EvaluationError):
    return JSONResponse(status_code=400, content={"error": "evaluation_rejected", "detail": str(exc)})


@app.exception_handler(SnapshotConflictError)
async def _snapshot_conflict_handler(_: Request, exc: SnapshotConflictError):
    return JSONResponse(
        status_code=409, content={"error": "snapshot_key_conflict", "detail": str(exc)}
    )


@app.exception_handler(SnapshotGoneError)
async def _snapshot_gone_handler(_: Request, exc: SnapshotGoneError):
    return JSONResponse(
        status_code=410, content={"error": "snapshot_gone", "detail": str(exc)}
    )


@app.exception_handler(InvalidCursorError)
async def _invalid_cursor_handler(_: Request, exc: InvalidCursorError):
    return JSONResponse(
        status_code=400, content={"error": "invalid_cursor", "detail": str(exc)}
    )


@app.exception_handler(SnapshotBuildError)
async def _snapshot_build_handler(_: Request, exc: SnapshotBuildError):
    # 构建失败不返回快照号/游标，不留任何可翻页的半成品
    return JSONResponse(
        status_code=500, content={"error": "snapshot_build_failed", "detail": str(exc)}
    )


@app.exception_handler(SnapshotCapacityError)
async def _snapshot_capacity_handler(_: Request, exc: SnapshotCapacityError):
    return JSONResponse(
        status_code=503, content={"error": "snapshot_capacity_exceeded", "detail": str(exc)}
    )


@app.get("/health", response_model=HealthResponse, tags=["ops"])
async def health() -> HealthResponse:
    """存活探针 + 连接池/快照观测（演示两条连接上限与复用/丢弃计数）。"""
    db_info: dict = {"connected": False}
    try:
        async with pool.lease() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    "SELECT current_database() AS db, current_user AS usr, "
                    "       (SELECT extversion FROM pg_extension WHERE extname='hypopg') AS hypopg"
                )
                db_info = {"connected": True, **(await cur.fetchone())}
    except Exception as exc:  # 健康检查不应抛 500 掩盖状态
        db_info = {"connected": False, "error": repr(exc)}
    return HealthResponse(
        status="ok", pool=pool.stats(), db=db_info, snapshots=snapshots.stats()
    )


@app.get("/samples", tags=["evaluation"])
async def list_samples() -> dict:
    """预置样例 SQL 及能/不能改善计划的候选示例。"""
    return {
        "samples": [
            {
                "sample_id": s.sample_id,
                "title": s.title,
                "sql_text": s.sql_text,
                "helpful_candidates": [c.__dict__ for c in s.helpful_candidates],
                "useless_candidates": [c.__dict__ for c in s.useless_candidates],
            }
            for s in SAMPLES
        ]
    }


@app.post(
    "/evaluate",
    response_model=EvaluateResponse,
    tags=["evaluation"],
    responses={
        400: {"description": "SQL 非法、表/列不存在或候选参数错误（不返回部分比较）"},
        500: {"description": "数据库内部错误；连接已清理后归还或丢弃"},
    },
)
async def evaluate_query(req: EvaluateRequest, request: Request) -> EvaluateResponse:
    """执行一次完整的基线 vs 全候选成本评估。

    客户端在处理中断连时，服务端仍会等待服务端查询取消、事务回滚与
    hypopg_reset 全部完成后才归还连接（详见连接池实现）。
    """
    request_id = uuid.uuid4().hex
    pause_seconds = 0.0
    raw_pause = request.headers.get("x-demo-pause-seconds")
    if raw_pause:
        try:
            pause_seconds = float(raw_pause)
            if pause_seconds < 0:
                raise ValueError
        except ValueError:
            raise SQLValidationError(
                "X-Demo-Pause-Seconds 必须是非负数字（单位秒）"
            )
    pause_mode = request.headers.get("x-demo-pause-mode", "asyncio").lower()
    if pause_mode not in ("asyncio", "pg_sleep"):
        raise SQLValidationError(
            "X-Demo-Pause-Mode 只允许 asyncio 或 pg_sleep"
        )

    log.info(
        "评估请求 %s 开始：候选数=%d 演示暂停=%.1fs 模式=%s",
        request_id, len(req.candidates), pause_seconds, pause_mode,
    )
    try:
        result = await evaluate(
            pool, settings, req,
            pause_seconds=pause_seconds,
            pause_mode=pause_mode,
            is_disconnected=request.is_disconnected,
        )
    except (SQLValidationError, EvaluationError):
        raise
    except Exception as exc:
        # 失败不返回部分比较；租约上下文已保证清理完成
        log.exception("评估请求 %s 失败: %r", request_id, exc)
        return JSONResponse(
            status_code=500,
            content={
                "error": "evaluation_failed",
                "request_id": request_id,
                "detail": "数据库评估失败，未产生比较结果；连接已完成清理",
            },
        )
    log.info(
        "评估请求 %s 完成：基线成本=%.2f 候选成本=%.2f 采纳=%d 拒绝=%d",
        request_id,
        result.baseline_total_cost,
        result.hypothetical_total_cost,
        len(result.adopted_candidates),
        len(result.rejected_candidates),
    )
    return result


@app.post(
    "/snapshots",
    response_model=SnapshotPageResponse,
    tags=["snapshot"],
    responses={
        200: {"description": "快照已创建（或复用同键同参数的既有快照），返回首批数据"},
        409: {"description": "同一 export_key 在有效期内使用了不同参数"},
        422: {"description": "参数校验失败（仓号、业务日、页大小等）"},
        500: {"description": "构建失败；不返回快照号或游标，不留半成品"},
        503: {"description": "有效快照数达到上限（SNAPSHOT_MAX_ACTIVE）"},
    },
)
async def create_snapshot(req: SnapshotCreateRequest) -> SnapshotPageResponse:
    """创建（或复用）快照并返回第一批数据。

    - 有效期内同 ``export_key`` + 同 ``warehouse_id`` + 同 ``business_date``
      的首次请求、并发请求与断连重试，全部复用同一份构建结果（``reused`` 标识）；
    - 同键换参数返回 409；
    - 冻结完成后源表的补录/更正不影响已冻结的页；
    - 快照保留 10 分钟，到期后旧游标返回 410。
    """
    return await snapshots.create(req)


@app.get(
    "/snapshots/{snapshot_id}/pages",
    response_model=SnapshotPageResponse,
    tags=["snapshot"],
    responses={
        200: {"description": "游标有效，返回同一快照的下一页"},
        400: {"description": "游标格式/签名/快照号不匹配"},
        410: {"description": "快照已到期或服务已重启，游标失效"},
    },
)
async def read_snapshot_page(
    snapshot_id: str,
    cursor: str = Query(..., description="上一页返回的 next_cursor"),
    page_size: int | None = Query(
        None,
        ge=1,
        le=MAX_SNAPSHOT_PAGE_SIZE,
        description=(
            f"本页条数，1~{MAX_SNAPSHOT_PAGE_SIZE}；不传则沿用创建快照时的页大小"
        ),
    ),
) -> SnapshotPageResponse:
    """按游标读取同一批冻结记录的下一页（记录内容与顺序与首次请求完全一致）。"""
    return await snapshots.read(snapshot_id, cursor, page_size)
