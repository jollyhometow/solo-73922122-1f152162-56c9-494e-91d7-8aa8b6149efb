"""快照导出：把某仓某夜班窗口的原始扫描明细冻结到内存，再按游标逐页下发。

需求要点与实现方式：

- **窗口**：UTC 业务日当天 18:00 至次日 06:00，左闭右开。
- **冻结**：一次 ``SELECT`` 读取最多 5000 条（额外多读 1 条用于判断是否截断），
  全部读完后才生成快照。任何读取/构建失败都不会注册快照，因此不存在
  “能翻第一页、后面却缺页”的半成品。- **幂等**：``export_key`` 是客户端导出键。有效期内同键同参数的首次请求、
  并发请求和断连重试都复用同一个快照（并发构建用同一个后台 Task）；
  同键换参数直接 409。
- **不受源表影响**：原始记录已复制到内存，冻结后源表的补录/更正/删除
  不会改变任何一页。
- **有效期**：默认 600 秒。到期后快照被移除，旧游标返回 410；进程重启后
  快照内存消失，游标签名仍可验证（默认密钥稳定），同样返回 410 而不是
  含混的格式错误。
- **连接上限**：构建只用现有 :class:`TwoConnectionPool` 的一条租约连接，
  物理连接上限仍为 2；单条 SELECT 自带语句级一致性快照。
"""
from __future__ import annotations

import asyncio
import base64
import contextlib
import hashlib
import hmac
import json
import logging
import time
import uuid
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time, timedelta, timezone
from decimal import Decimal

from psycopg import sql as pg_sql
from psycopg.rows import dict_row

from .config import Settings
from .db import TwoConnectionPool
from .schemas import (
    MAX_SNAPSHOT_PAGE_SIZE,
    MAX_SNAPSHOT_ROWS,
    SnapshotCreateRequest,
    SnapshotPageResponse,
)

log = logging.getLogger("hypopg_api.snapshot")

# 窗口固定为业务日 18:00 起、连续 12 小时（到次日 06:00），左闭右开。
_WINDOW_START_HOUR = 18
_WINDOW_LENGTH = timedelta(hours=12)
_SNAPSHOT_COLUMNS = (
    "scan_id",
    "package_no",
    "warehouse_id",
    "conveyor_id",
    "scan_time",
    "scan_event",
    "destination",
    "service_type",
    "length_cm",
    "width_cm",
    "height_cm",
    "weight_kg",
)


# ---------------- 错误类型（由 main.py 映射为明确 HTTP 状态） ----------------


class SnapshotConflictError(RuntimeError):
    """同 export_key 在有效期内被用于不同参数 -> 409。"""


class SnapshotGoneError(RuntimeError):
    """快照已到期或服务已重启，旧游标无法继续翻页 -> 410。"""


class InvalidCursorError(ValueError):
    """游标格式/签名/归属校验失败 -> 400。"""


class SnapshotBuildError(RuntimeError):
    """快照构建失败（数据库等），未注册任何半成品 -> 500。"""


class SnapshotCapacityError(RuntimeError):
    """有效快照数达到上限，且没有可回收的已过期快照 -> 503。"""


# ---------------- 游标签名 ----------------------------


class CursorCodec:
    """游标 = URL-safe base64(JSON payload) + HMAC 截断签名。

    密钥默认写死为稳定值，使进程重启后旧游标仍能先通过完整性校验，
    从而得到明确的 “410 快照已失效”，而不是无法区分来源的 400。
    """

    def __init__(self, secret: str) -> None:
        self._secret = secret.encode("utf-8")

    def _sign(self, payload: bytes) -> str:
        return hmac.new(self._secret, payload, hashlib.sha256).hexdigest()[:32]

    def encode(self, snapshot_id: str, offset: int, page_size: int) -> str:
        payload = json.dumps(
            {"s": snapshot_id, "o": offset, "p": page_size},
            separators=(",", ":"), sort_keys=True,
        ).encode("utf-8")
        token = base64.urlsafe_b64encode(payload).rstrip(b"=").decode("ascii")
        return f"{token}.{self._sign(payload)}"

    def decode(self, cursor: str) -> tuple[str, int, int]:
        if not cursor or "." not in cursor:
            raise InvalidCursorError("游标格式非法")
        token, sig = cursor.rsplit(".", 1)
        try:
            padded = token + "=" * (-len(token) % 4)
            payload = base64.urlsafe_b64decode(padded.encode("ascii"))
        except Exception as exc:
            raise InvalidCursorError("游标编码非法") from exc
        if not hmac.compare_digest(self._sign(payload), sig):
            raise InvalidCursorError("游标签名无效（可能被篡改或密钥已更换）")
        try:
            data = json.loads(payload)
            snapshot_id = data["s"]
            offset = data["o"]
            page_size = data["p"]
        except Exception as exc:
            raise InvalidCursorError("游标内容非法") from exc
        if not isinstance(snapshot_id, str) or not snapshot_id:
            raise InvalidCursorError("游标缺少快照号")
        if not isinstance(offset, int) or isinstance(offset, bool) or offset < 0:
            raise InvalidCursorError("游标偏移量非法")
        if (
            not isinstance(page_size, int) or isinstance(page_size, bool)
            or page_size < 1 or page_size > MAX_SNAPSHOT_PAGE_SIZE
        ):
            raise InvalidCursorError("游标中的每页条数非法")
        return snapshot_id, offset, page_size


# ---------------- 快照数据结构 ----------------


@dataclass(frozen=True)
class SnapshotParams:
    export_key: str
    warehouse_id: int
    business_date: date

    # 快照身份只由「导出键 + 仓号 + UTC 业务日」决定，与需求一致：
    # page_size 只影响读取节奏（每页最多 200 条），不改变冻结的记录集合，
    # 因此同键同窗口换页大小仍然复用同一快照，不会误报冲突。

    def as_dict(self) -> dict:
        return {
            "export_key": self.export_key,
            "warehouse_id": self.warehouse_id,
            "business_date": self.business_date.isoformat(),
        }


@dataclass(frozen=True)
class Snapshot:
    snapshot_id: str
    params: SnapshotParams
    window_start: datetime
    window_end: datetime
    rows: tuple[dict, ...]
    total_rows: int
    truncated: bool
    created_at: datetime
    expires_at: datetime
    expires_monotonic: float


@dataclass
class _Build:
    task: asyncio.Task | None
    params: SnapshotParams


# ---------------- 管理器 ----------------


def business_window(business_date: date) -> tuple[datetime, datetime]:
    start = datetime.combine(
        business_date, dt_time(_WINDOW_START_HOUR, 0, 0), tzinfo=timezone.utc
    )
    return start, start + _WINDOW_LENGTH


def _normalize_row(row: dict) -> dict:
    """把 psycopg 的 Decimal 转成 JSON 数字，其余（时间、文本、整数）原样保留。"""
    return {
        key: (float(value) if isinstance(value, Decimal) else value)
        for key, value in row.items()
    }


class SnapshotManager:
    """单进程内存快照库；与单 uvicorn worker 的部署约束一致。"""

    def __init__(self, pool: TwoConnectionPool, settings: Settings) -> None:
        self._pool = pool
        self._settings = settings
        self._ttl = settings.snapshot_ttl_seconds
        self._max_active = settings.snapshot_max_active
        self._cursor = CursorCodec(settings.snapshot_cursor_secret)
        self._snapshots: dict[str, Snapshot] = {}
        self._key_index: dict[str, str] = {}
        self._builds: dict[str, _Build] = {}
        self._lock = asyncio.Lock()
        self._reaper: asyncio.Task | None = None
        self._total_built = 0
        self._total_reused = 0
        self._total_expired = 0
        self._total_failed = 0

    # ---------- 生命周期 ----------

    def start(self) -> None:
        if self._reaper is None or self._reaper.done():
            self._reaper = asyncio.create_task(self._reap_loop())

    async def stop(self) -> None:
        if self._reaper is not None:
            self._reaper.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._reaper
            self._reaper = None
        # 取消在途构建：取消只会让租约层走标准清理路径，且未完成的构建
        # 不会注册快照，因此不会留下可翻页的半成品。
        builds = [b.task for b in self._builds.values() if b.task is not None]
        for task in builds:
            task.cancel()
        if builds:
            await asyncio.gather(*builds, return_exceptions=True)
        async with self._lock:
            self._snapshots.clear()
            self._key_index.clear()
            self._builds.clear()

    async def _reap_loop(self) -> None:
        interval = max(1.0, min(30.0, self._ttl / 10.0))
        try:
            while True:
                await asyncio.sleep(interval)
                async with self._lock:
                    self._prune_expired()
        except asyncio.CancelledError:
            raise

    # ---------- 过期清理（调用方必须持有 self._lock） ----------

    def _prune_expired(self, now_monotonic: float | None = None) -> None:
        now = time.monotonic() if now_monotonic is None else now_monotonic
        expired = [
            sid for sid, snap in self._snapshots.items()
            if snap.expires_monotonic <= now
        ]
        for sid in expired:
            snap = self._snapshots.pop(sid, None)
            if snap is None:
                continue
            if self._key_index.get(snap.params.export_key) == sid:
                self._key_index.pop(snap.params.export_key, None)
            self._total_expired += 1
            log.info("快照 %s 到期，已从内存移除（旧游标将返回 410）", sid)

    # ---------- 数据读取 ----------

    async def _build(self, params: SnapshotParams, build: "_Build") -> Snapshot:
        start, end = business_window(params.business_date)
        # 多读 1 条：既不超过 5000 条入库上限，又能准确报告“窗口内是否还有更多”。
        limit = MAX_SNAPSHOT_ROWS + 1
        sql = (
            "SELECT " + ", ".join(_SNAPSHOT_COLUMNS) +
            " FROM public.package_scan"
            " WHERE warehouse_id = %s AND scan_time >= %s AND scan_time < %s"
            " ORDER BY scan_time, scan_id"
            " LIMIT %s"
        )
        try:
            async def _read() -> list[dict]:
                async with self._pool.lease() as conn:
                    async with conn.cursor(row_factory=dict_row) as cur:
                        # 事务内局部超时：与异步侧 wait_for 形成双保险，
                        # 即使事件循环上的取消迟到，数据库也不会无限占用连接。
                        # SET 不接受绑定参数，值必须用 Literal 组装。
                        await cur.execute(
                            pg_sql.SQL("SET LOCAL statement_timeout = {}").format(
                                pg_sql.Literal(
                                    self._settings.snapshot_build_timeout_seconds * 1000
                                )
                            )
                        )
                        await cur.execute(
                            sql,
                            (params.warehouse_id, start, end, limit),
                        )
                        return await cur.fetchall()

            rows = await asyncio.wait_for(
                _read(), timeout=self._settings.snapshot_build_timeout_seconds
            )
        except asyncio.TimeoutError as exc:
            raise SnapshotBuildError("快照构建超时，未生成任何可翻页数据") from exc
        except Exception as exc:
            raise SnapshotBuildError("快照构建失败，未生成任何可翻页数据") from exc

        truncated = len(rows) > MAX_SNAPSHOT_ROWS
        if truncated:
            rows = rows[:MAX_SNAPSHOT_ROWS]
        rows = [_normalize_row(row) for row in rows]

        now_mono = time.monotonic()
        now_wall = datetime.now(timezone.utc)
        snap = Snapshot(
            snapshot_id=uuid.uuid4().hex,
            params=params,
            window_start=start,
            window_end=end,
            rows=tuple(rows),
            total_rows=len(rows),
            truncated=truncated,
            created_at=now_wall,
            expires_at=now_wall + timedelta(seconds=self._ttl),
            expires_monotonic=now_mono + self._ttl,
        )

        async with self._lock:
            # 只有仍是该键“当前构建”的结果才允许注册，避免被取代的旧构建
            # 覆盖新身份（例如键过期后重建的竞态）。
            if self._builds.get(params.export_key) is not build:
                raise SnapshotBuildError("快照构建已被同键的新构建取代，本次结果未注册")
            current_id = self._key_index.get(params.export_key)
            if current_id is not None:
                current = self._snapshots.get(current_id)
                if current is not None:
                    if current.params == params:
                        # 同键同参数已有有效快照：以先注册者为准（幂等去重）。
                        return current
                    raise SnapshotConflictError(
                        "export_key 已被另一组参数的快照占用"
                        f"：已有 {current.params.as_dict()}，本次 {params.as_dict()}"
                    )
            # 注册前再确认一次容量：构建期间可能有其他键把容量占满。
            if len(self._snapshots) + len(self._builds) - 1 >= self._max_active:
                raise SnapshotCapacityError(
                    f"有效快照数已达上限 {self._max_active}，请稍后重试"
                )
            self._snapshots[snap.snapshot_id] = snap
            self._key_index[params.export_key] = snap.snapshot_id
            self._total_built += 1
        log.info(
            "快照 %s 构建完成：export_key=%s warehouse=%s date=%s rows=%d%s",
            snap.snapshot_id, params.export_key, params.warehouse_id,
            params.business_date.isoformat(), snap.total_rows,
            "（已截断至上限）" if snap.truncated else "",
        )
        return snap

    async def _start_or_reuse_build(self, params: SnapshotParams) -> tuple[Snapshot, bool]:
        """返回 (快照, 是否复用)。不同参数的冲突在持锁时立即报出，不等待构建。"""
        async with self._lock:
            self._prune_expired()
            sid = self._key_index.get(params.export_key)
            if sid is not None:
                snap = self._snapshots.get(sid)
                if snap is not None:
                    if snap.params != params:
                        raise SnapshotConflictError(
                            "export_key 在有效期内已被用于不同参数"
                            f"：已有 {snap.params.as_dict()}，本次 {params.as_dict()}"
                        )
                    self._total_reused += 1
                    return snap, True

            build = self._builds.get(params.export_key)
            if build is not None:
                if build.params != params:
                    raise SnapshotConflictError(
                        "export_key 正在构建另一组参数，不能同时换参数"
                        f"：已有 {build.params.as_dict()}，本次 {params.as_dict()}"
                    )
                task = build.task
                if task is None:  # 构建刚被登记但任务尚未赋值：极短窗口
                    raise SnapshotCapacityError("快照正在建立，请稍后重试")
                reused = True
            else:
                # 容量闸门放在持锁处：已注册快照 + 在途构建一起计数，
                # 避免并发首次请求同时越过上限。已到期的快照刚被 _prune_expired
                # 回收，因此这里只拒绝“真正无处可放”的请求（503），
                # 不会为了腾地方淘汰仍在有效期内的快照。
                if len(self._snapshots) + len(self._builds) >= self._max_active:
                    raise SnapshotCapacityError(
                        f"有效快照数已达上限 {self._max_active}，请稍后重试"
                    )
                build = _Build(task=None, params=params)
                task = asyncio.create_task(self._build(params, build))
                build.task = task
                self._builds[params.export_key] = build
                task.add_done_callback(
                    lambda t, key=params.export_key, b=build: self._on_build_done(key, b, t)
                )
                reused = False

        # shield：客户端断连只会取消当前请求，不会取消共享构建；
        # 构建完成后快照留在内存中，断连重试即可复用同一结果。
        try:
            snap = await asyncio.shield(task)
        except asyncio.CancelledError:
            raise
        except (SnapshotBuildError, SnapshotConflictError):
            raise
        except Exception as exc:
            # 任何构建异常都不带 snapshot_id / cursor 返回，且不注册快照
            log.warning("快照构建失败（未注册半成品）: %r", exc)
            raise SnapshotBuildError("快照构建失败，未生成任何可翻页数据") from exc
        return snap, reused

    def _on_build_done(
        self, key: str, build: _Build, task: asyncio.Task
    ) -> None:
        # 仅移除自己这个构建登记，避免误删后续新构建。
        if self._builds.get(key) is build:
            self._builds.pop(key, None)
        if task.cancelled():
            self._total_failed += 1
            return
        exc = task.exception()
        if exc is not None:
            self._total_failed += 1
            log.warning("快照构建任务失败 export_key=%s: %r", key, exc)

    # ---------- 对外操作 ----------

    def _page(
        self, snap: Snapshot, offset: int, page_size: int, reused: bool
    ) -> SnapshotPageResponse:
        rows = list(snap.rows[offset:offset + page_size])
        next_offset = offset + len(rows)
        has_more = next_offset < snap.total_rows
        next_cursor = (
            self._cursor.encode(snap.snapshot_id, next_offset, page_size)
            if has_more
            else None
        )
        # created_at/expires_at 使用 UTC；Pydantic 会规范化为 RFC3339。
        return SnapshotPageResponse(
            snapshot_id=snap.snapshot_id,
            export_key=snap.params.export_key,
            warehouse_id=snap.params.warehouse_id,
            business_date=snap.params.business_date,
            window_start=snap.window_start,
            window_end=snap.window_end,
            created_at=snap.created_at,
            expires_at=snap.expires_at,
            total_rows=snap.total_rows,
            truncated=snap.truncated,
            page_size=page_size,
            offset=offset,
            returned_rows=len(rows),
            has_more=has_more,
            next_cursor=next_cursor,
            reused=reused,
            records=rows,
        )

    async def create(self, req: SnapshotCreateRequest) -> SnapshotPageResponse:
        params = SnapshotParams(
            export_key=req.export_key,
            warehouse_id=req.warehouse_id,
            business_date=req.business_date,
        )
        snap, reused = await self._start_or_reuse_build(params)
        page_size = min(req.page_size, MAX_SNAPSHOT_PAGE_SIZE)
        return self._page(snap, 0, page_size, reused)

    async def read(
        self, snapshot_id: str, cursor: str, page_size: int | None
    ) -> SnapshotPageResponse:
        cursor_snapshot_id, offset, cursor_page_size = self._cursor.decode(cursor)
        if cursor_snapshot_id != snapshot_id:
            raise InvalidCursorError("游标与路径中的快照号不一致")

        async with self._lock:
            snap = self._snapshots.get(snapshot_id)
            if snap is None:
                # 签名有效但快照不在内存：已到期，或服务重启后内存中丢失。
                raise SnapshotGoneError(
                    "快照已到期或服务已重启，该游标失效；请用 export_key 重新创建快照"
                )
            if snap.expires_monotonic <= time.monotonic():
                # 即便后台清理尚未跑到，也在此刻明确失效。
                self._prune_expired()
                raise SnapshotGoneError(
                    "快照已到期，该游标失效；请用 export_key 重新创建快照"
                )
            if offset > snap.total_rows:
                raise InvalidCursorError("游标偏移量超出快照记录数")
            # 不传 page_size 时沿用游标中记录的节奏，保证翻页连续；
            # 显式传入则允许在 1~200 内调整本页大小，快照内容本身不受影响。
            effective = cursor_page_size if page_size is None else page_size
            effective = min(effective, MAX_SNAPSHOT_PAGE_SIZE)
            return self._page(snap, offset, effective, reused=False)

    def stats(self) -> dict:
        return {
            "active_snapshots": len(self._snapshots),
            "builds_in_flight": len(self._builds),
            "max_active": self._max_active,
            "ttl_seconds": self._ttl,
            "total_built": self._total_built,
            "total_reused": self._total_reused,
            "total_expired": self._total_expired,
            "total_failed": self._total_failed,
        }
