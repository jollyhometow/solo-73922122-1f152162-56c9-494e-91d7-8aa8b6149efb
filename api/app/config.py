"""进程级配置（全部来自环境变量，带安全默认值）。"""
from __future__ import annotations

import os
from dataclasses import dataclass


def _int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(f"环境变量 {name} 必须是整数，实际为 {raw!r}") from exc
    if value <= 0:
        raise RuntimeError(f"环境变量 {name} 必须为正整数，实际为 {value}")
    return value


@dataclass(frozen=True)
class Settings:
    db_dsn: str
    pool_max_connections: int
    statement_timeout_ms: int
    # 演示用暂停开关与上限（秒）。生产应保持关闭。
    demo_pause_enabled: bool
    demo_pause_max_seconds: float
    # 快照导出：有效期、构建超时、容量上限、游标签名密钥。
    snapshot_ttl_seconds: int
    snapshot_build_timeout_seconds: int
    snapshot_max_active: int
    snapshot_cursor_secret: str


def get_settings() -> Settings:
    return Settings(
        db_dsn=os.environ.get(
            "DB_DSN", "postgresql://app:app@127.0.0.1:5433/logistics"
        ),
        # 需求硬性约束：单 API 进程最多复用两条数据库连接
        pool_max_connections=min(2, _int("POOL_MAX_CONNECTIONS", 2)),
        statement_timeout_ms=_int("STATEMENT_TIMEOUT_MS", 30_000),
        demo_pause_enabled=os.environ.get("DEMO_PAUSE_ENABLED", "false").lower()
        in ("1", "true", "yes", "on"),
        demo_pause_max_seconds=float(os.environ.get("DEMO_PAUSE_MAX_SECONDS", "30")),
        # 快照默认保留 10 分钟；到期后即使记录仍在内存中也拒绝翻页。
        snapshot_ttl_seconds=_int("SNAPSHOT_TTL_SECONDS", 600),
        snapshot_build_timeout_seconds=_int("SNAPSHOT_BUILD_TIMEOUT_SECONDS", 60),
        # 单快照最多 5000 行，全部驻留内存；用容量上限兜住内存，而不是
        # 悄悄淘汰未到期的快照（那会破坏“有效期内可翻页”的承诺）。
        snapshot_max_active=_int("SNAPSHOT_MAX_ACTIVE", 32),
        # 游标签名密钥必须跨进程重启保持稳定，重启后的旧游标才能通过
        # 完整性校验并得到明确的 410（快照已随进程消失），而不是含糊的格式错误。
        # 单租户导出场景下它不是安全边界；需要更强隔离时请通过环境变量覆盖。
        snapshot_cursor_secret=os.environ.get(
            "SNAPSHOT_CURSOR_SECRET", "logistics-snapshot-cursor-v1"
        ),
    )
