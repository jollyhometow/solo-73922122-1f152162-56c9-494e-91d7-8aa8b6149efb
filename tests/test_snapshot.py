"""快照导出 API 端到端验证（对真实运行的 API + PostgreSQL 发起 HTTP 请求）。

运行：
  python -I tests/test_snapshot.py http://127.0.0.1:8000
  TEST_DB_DSN=postgresql://app:app@127.0.0.1:5432/logistics \
    python -I tests/test_snapshot.py http://127.0.0.1:8000

覆盖需求点：
1. 首次请求返回快照号与首批数据（最多 200 条，窗口 18:00~次日 06:00 左闭右开）；
2. 每窗口最多冻结 5000 条，超出标记 truncated；
3. 有效期内同键同参数（含并发请求、断连重试）复用同一快照号与同一批记录；
4. 同键换参数返回 409；
5. 翻页无漏行、无重行、跨页有序；
6. 游标被篡改 / 格式非法 / 与快照号不匹配 -> 400；
7. 参数校验 -> 422；
8. 空窗口可创建且无游标；
9. 冻结后源表补录/更正不影响已冻结页（需 TEST_DB_DSN）。
"""
from __future__ import annotations

import asyncio
import os
import sys
import uuid

import httpx

# 种子数据确定有 8334 行的窗口：仓 3，UTC 业务日 2026-11-11
# （窗口 2026-11-11 18:00 ~ 2026-11-12 06:00，左闭右开）。
SNAP_BASE = {"warehouse_id": 3, "business_date": "2026-11-11"}

_passed = 0
_failed: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    global _passed
    if cond:
        _passed += 1
        print(f"  PASS  {name}")
    else:
        _failed.append(f"{name} :: {detail}")
        print(f"  FAIL  {name}  {detail}")


def _mutate_source(dsn: str) -> int:
    import psycopg

    with psycopg.connect(dsn, autocommit=True) as conn:
        rows = conn.execute(
            "UPDATE public.package_scan SET scan_event = 'CHANGED' "
            "WHERE scan_id IN ("
            "  SELECT scan_id FROM public.package_scan "
            "  WHERE warehouse_id = 3 "
            "    AND scan_time >= TIMESTAMPTZ '2026-11-11 18:00:00+00' "
            "    AND scan_time <  TIMESTAMPTZ '2026-11-12 06:00:00+00' "
            "  ORDER BY scan_time, scan_id LIMIT 250"
            ") RETURNING scan_id"
        ).fetchall()
        return len(rows)


def _restore_source(dsn: str) -> None:
    import psycopg

    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            "UPDATE public.package_scan SET scan_event = 'INBOUND' "
            "WHERE scan_event = 'CHANGED'"
        )


async def main(base_url: str) -> int:
    async with httpx.AsyncClient(base_url=base_url, timeout=60) as c:
        h = (await c.get("/health")).json()
        check("连接池上限为 2", h["pool"]["max_connections"] == 2)
        check("健康检查暴露快照状态", "ttl_seconds" in h.get("snapshots", {}))

        key = f"test-{uuid.uuid4().hex[:12]}"

        print("== 场景 1：创建快照，返回快照号与首批数据 ==")
        r = await c.post("/snapshots", json={**SNAP_BASE, "export_key": key})
        check("HTTP 200", r.status_code == 200, r.text[:300])
        snap = r.json()
        snapshot_id = snap["snapshot_id"]
        check("返回快照号", bool(snapshot_id))
        check("首批最多 200 条", snap["returned_rows"] == 200, str(snap["returned_rows"]))
        check("冻结上限 5000 条", snap["total_rows"] == 5000, str(snap["total_rows"]))
        check("超上限标记为截断", snap["truncated"] is True)
        check("有下一页游标", bool(snap["next_cursor"]))
        check(
            "窗口为 18:00 至次日 06:00",
            snap["window_start"] == "2026-11-11T18:00:00Z"
            and snap["window_end"] == "2026-11-12T06:00:00Z",
            f"{snap['window_start']} ~ {snap['window_end']}",
        )
        check("记录均属于该仓", all(x["warehouse_id"] == 3 for x in snap["records"]))
        check(
            "所有记录落在窗口内（左闭右开）",
            all(
                "2026-11-11T18:00:00Z" <= x["scan_time"] < "2026-11-12T06:00:00Z"
                for x in snap["records"]
            ),
        )
        first_keys = [(x["scan_time"], x["scan_id"]) for x in snap["records"]]
        check("首批按 scan_time, scan_id 升序", first_keys == sorted(first_keys))

        print("== 场景 2：同键同参数复用同一快照 ==")
        r = await c.post("/snapshots", json={**SNAP_BASE, "export_key": key})
        again = r.json()
        check("HTTP 200", r.status_code == 200, r.text[:200])
        check("复用同一快照号", again["snapshot_id"] == snapshot_id,
              f"{snapshot_id} vs {again['snapshot_id']}")
        check("reused 标记为 True", again["reused"] is True)
        check("首批记录与首次一致", again["records"] == snap["records"])

        print("== 场景 3：同键换参数返回 409 ==")
        for label, payload in [
            ("换仓号", {**SNAP_BASE, "warehouse_id": 4}),
            ("换业务日", {**SNAP_BASE, "business_date": "2026-11-10"}),
        ]:
            r = await c.post("/snapshots", json={**payload, "export_key": key})
            check(f"{label} -> 409", r.status_code == 409,
                  f"{r.status_code} {r.text[:200]}")
        # page_size 只是本页节奏，不属于快照参数：同键同窗口换页大小仍复用同一快照
        r = await c.post("/snapshots", json={**SNAP_BASE, "export_key": key,
                                             "page_size": 50})
        ps = r.json()
        check("换页大小仍复用同一快照",
              r.status_code == 200 and ps["snapshot_id"] == snapshot_id
              and ps["reused"] is True,
              f"{r.status_code} {r.text[:200]}")
        check("换页大小后首批按新页大小返回", ps["returned_rows"] == 50,
              str(ps.get("returned_rows")))
        r = await c.post("/snapshots", json={**SNAP_BASE, "export_key": key + "-b"})
        check("不同导出键可创建独立快照",
              r.status_code == 200 and r.json()["snapshot_id"] != snapshot_id)

        print("== 场景 4：并发同键同参数复用同一构建结果 ==")
        ckey = f"conc-{uuid.uuid4().hex[:12]}"
        results = await asyncio.gather(
            *[c.post("/snapshots", json={**SNAP_BASE, "export_key": ckey})
              for _ in range(10)]
        )
        check("并发请求全部成功",
              all(x.status_code == 200 for x in results),
              str([x.status_code for x in results]))
        cids = {x.json()["snapshot_id"] for x in results}
        check("并发请求共享同一快照号", len(cids) == 1, str(cids))
        cjson = [x.json() for x in results]
        check("并发首批记录完全一致",
              all(x["records"] == cjson[0]["records"] for x in cjson))

        print("== 场景 5：翻页完整性（无漏行/重行/乱序） ==")
        seen_ids: list[int] = [x["scan_id"] for x in snap["records"]]
        seen_keys = list(first_keys)
        cursor = snap["next_cursor"]
        page_count = 1
        page_sizes = [snap["returned_rows"]]
        while cursor:
            r = await c.get(f"/snapshots/{snapshot_id}/pages",
                            params={"cursor": cursor})
            check(f"第 {page_count + 1} 页 HTTP 200", r.status_code == 200,
                  r.text[:200])
            p = r.json()
            check(f"第 {page_count + 1} 页最多 200 条", p["returned_rows"] <= 200)
            check(f"第 {page_count + 1} 页属于同一快照",
                  p["snapshot_id"] == snapshot_id)
            page_sizes.append(p["returned_rows"])
            seen_ids += [x["scan_id"] for x in p["records"]]
            seen_keys += [(x["scan_time"], x["scan_id"]) for x in p["records"]]
            cursor = p["next_cursor"]
            page_count += 1
            if page_count > 40:
                break
        check("翻页总数等于快照记录数",
              len(seen_ids) == snap["total_rows"],
              f"{len(seen_ids)} vs {snap['total_rows']}")
        check("无重复行", len(set(seen_ids)) == len(seen_ids))
        check("跨页整体有序", seen_keys == sorted(seen_keys))
        check("共 25 页（5000/200）", page_count == 25, str(page_count))
        check("除末页外每页 200 条",
              all(n == 200 for n in page_sizes[:-1]), str(page_sizes))

        print("== 场景 5b：有效快照容量上限（可选，SNAPSHOT_MAX_ACTIVE>=3 时跳过） ==")
        st = (await c.get("/health")).json()["snapshots"]
        max_active = st.get("max_active")
        if max_active is not None and max_active <= 2:
            print(f"    (SNAPSHOT_MAX_ACTIVE={max_active}，跳过容量场景)")
        else:
            print("    (默认容量充足，不主动填满；仅验证 /health 暴露上限)")
            check("健康检查暴露容量上限", isinstance(max_active, int) and max_active > 0,
                  str(max_active))

        print("== 场景 6：游标错误处理 ==")
        r = await c.get(f"/snapshots/{snapshot_id}/pages",
                        params={"cursor": "bad-cursor"})
        check("格式非法游标 -> 400", r.status_code == 400, str(r.status_code))
        r = await c.get(f"/snapshots/{snapshot_id}/pages",
                        params={"cursor": snap["next_cursor"][:-1] + "0"})
        check("篡改游标 -> 400", r.status_code == 400, str(r.status_code))
        r = await c.get("/snapshots/deadbeef/pages",
                        params={"cursor": snap["next_cursor"]})
        check("游标与快照号不匹配 -> 400", r.status_code == 400, str(r.status_code))
        r = await c.get(f"/snapshots/{snapshot_id}/pages",
                        params={"cursor": snap["next_cursor"], "page_size": 201})
        check("翻页页大小越界 -> 422", r.status_code == 422, str(r.status_code))

        print("== 场景 7：参数校验 ==")
        for label, payload in [
            ("仓号为 0", {"export_key": "x", "warehouse_id": 0,
                        "business_date": "2026-11-11"}),
            ("空导出键", {"export_key": "  ", "warehouse_id": 3,
                        "business_date": "2026-11-11"}),
            ("缺导出键", {"warehouse_id": 3, "business_date": "2026-11-11"}),
            ("页大小 201", {"export_key": "x", "warehouse_id": 3,
                          "business_date": "2026-11-11", "page_size": 201}),
            ("页大小为 0", {"export_key": "x", "warehouse_id": 3,
                          "business_date": "2026-11-11", "page_size": 0}),
            ("非法业务日", {"export_key": "x", "warehouse_id": 3,
                          "business_date": "2026-13-40"}),
        ]:
            r = await c.post("/snapshots", json=payload)
            check(f"拒绝{label} -> 422", r.status_code == 422, str(r.status_code))

        print("== 场景 8：空窗口 ==")
        r = await c.post("/snapshots", json={
            "export_key": f"empty-{uuid.uuid4().hex[:8]}",
            "warehouse_id": 3, "business_date": "2026-11-13",
        })
        empty = r.json()
        check("空窗口 HTTP 200", r.status_code == 200, r.text[:200])
        check("空窗口 total_rows=0", empty["total_rows"] == 0)
        check("空窗口无游标",
              empty["next_cursor"] is None and empty["has_more"] is False)
        check("空窗口不截断", empty["truncated"] is False)

        print("== 场景 9：小页连续翻页 ==")
        r = await c.post("/snapshots", json={
            **SNAP_BASE, "export_key": f"small-{uuid.uuid4().hex[:8]}",
            "page_size": 7,
        })
        small = r.json()
        check("自定义页大小生效", small["returned_rows"] == 7,
              str(small["returned_rows"]))
        r = await c.get(f"/snapshots/{small['snapshot_id']}/pages",
                        params={"cursor": small["next_cursor"]})
        p = r.json()
        check("翻页沿用游标中的页大小", r.status_code == 200 and p["returned_rows"] == 7,
              f"{r.status_code} {p.get('returned_rows')}")
        check("续页 offset 正确", p["offset"] == 7, str(p.get("offset")))
        r = await c.get(f"/snapshots/{small['snapshot_id']}/pages",
                        params={"cursor": small["next_cursor"], "page_size": 3})
        check("显式 page_size 可覆盖本页条数",
              r.status_code == 200 and r.json()["returned_rows"] == 3,
              f"{r.status_code} {r.text[:120]}")

        print("== 场景 10：冻结后源表变化不影响已冻结页 ==")
        dsn = os.environ.get("TEST_DB_DSN")
        if dsn:
            fkey = f"frozen-{uuid.uuid4().hex[:8]}"
            r = await c.post("/snapshots", json={**SNAP_BASE, "export_key": fkey})
            frozen = r.json()
            fid, fcur = frozen["snapshot_id"], frozen["next_cursor"]
            mutated = await asyncio.to_thread(_mutate_source, dsn)
            try:
                r = await c.get(f"/snapshots/{fid}/pages", params={"cursor": fcur})
                page2 = r.json()
                check("源表变更后下一页仍可读", r.status_code == 200, r.text[:200])
                check("冻结页内容未被改写",
                      all(x["scan_event"] != "CHANGED" for x in page2["records"]))
                check("源表确实发生了变更", mutated > 0, str(mutated))
                # 同键重试仍拿到同一份冻结数据
                r = await c.post("/snapshots", json={**SNAP_BASE, "export_key": fkey})
                check("变更后同键仍复用同一快照",
                      r.json()["snapshot_id"] == fid and r.json()["reused"] is True)
                # 换新键则读到变更后的新版本
                r = await c.post("/snapshots", json={
                    **SNAP_BASE, "export_key": fkey + "-new",
                })
                check("新导出键可读到新版本",
                      r.status_code == 200
                      and any(x["scan_event"] == "CHANGED" for x in r.json()["records"]))
            finally:
                await asyncio.to_thread(_restore_source, dsn)
        else:
            print("    (设置 TEST_DB_DSN 可追加源表变更断言)")

    print(f"\n通过 {_passed} 项，失败 {len(_failed)} 项")
    if _failed:
        print("失败明细：")
        for f in _failed:
            print(" -", f)
        return 1
    return 0


if __name__ == "__main__":
    url = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000"
    raise SystemExit(asyncio.run(main(url)))
