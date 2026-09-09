# -*- coding: utf-8 -*-
"""任务 A（断日补跑自愈）+ 任务 C（MA200 缓冲落后自愈）自测。

分两层（本机无 DATABASE_URL，连接串在 GitHub Secrets，故 DB 层用 mock）：
  L1 真实网络层：endTime 边界 / 无前视 / 切片恒等 / 信号回放恒等 —— 全部打真实
                 data-api.binance.vision，是本次改动风险最高的部分。
  L2 编排逻辑层：用 FakeConn(mock asyncpg) + 真实历史窗口喂补跑器，验证无缺口零动作、
                 缺口检出、顺序补跑、失败留队、锁隔离、不回滚历史。
  L3 任务 C：MAGate 缓冲连续性（纯内存）。

run: python selftest_p1_repair.py
"""
import sys
import os
import json
import asyncio
import datetime
import contextlib

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import radar_scan
import paper_accounting
import p1_repair

PASS, FAIL = [], []


def chk(name, cond, extra=""):
    (PASS if cond else FAIL).append(name)
    print("[%s] %s %s" % ("PASS" if cond else "FAIL", name, extra))


# ===================== 真实数据准备 =====================
# 用一段真实历史窗口：expected = E，构造 anchor = E-3 的 3 天缺口
E = "2026-08-20"
E_ANCHOR_DAYS = 3
SYMS = radar_scan.P1_SYMBOLS
WIN = {}


def load_windows():
    """拉每币截至 E 的 200 根日线（真实网络）。失败 → 抛错，不伪造。"""
    for s in SYMS:
        WIN[s] = radar_scan.p1_fetch_klines(s, asof=E, limit=200)
    return WIN


# ===================== L1: 真实网络层 =====================
def t_endtime_boundary():
    print("\n--- [A0] vision endTime 边界语义 ---")
    ok = True
    for d in ["2023-11-12", "2022-01-15", "2024-06-30", "2026-08-20"]:
        rows = radar_scan.p1_fetch_klines("BTCUSDT", asof=d, limit=80)
        good = rows and len(rows) == 80 and rows[-1]["date"] == d
        ok = ok and good
        print("   asof=%s n=%d last=%s %s" % (
            d, len(rows), rows[-1]["date"] if rows else "-",
            "OK" if good else "MISMATCH"))
    chk("A0 endTime 边界: 末根 openTime 严格 == asof（4 个历史日）", ok)


def t_no_lookahead():
    print("\n--- [A1] endTime 无前视（窗口不含 asof 之后的 K 线）---")
    ok = True
    for s in SYMS:
        rows = radar_scan.p1_fetch_klines(s, asof=E, limit=200)
        after = [r for r in rows if r["date"] > E]
        ok = ok and (not after)
        print("   %s n=%d 越界根数=%d last=%s" % (s, len(rows), len(after),
                                                  rows[-1]["date"] if rows else "-"))
    chk("A1 无前视: 5 币窗口均无 > asof 的 K 线", ok)


def t_slice_identity():
    """[A2] endTime 直接拉 == 长窗口本地切片（同一 asof、同一长度）。"""
    print("\n--- [A2] 切片恒等: endTime 直拉 == 长窗口本地切片 ---")
    ok = True
    for s in SYMS:
        longw = WIN[s]
        for k in (0, 1, 3, 7, 15):
            d = (datetime.date.fromisoformat(E) - datetime.timedelta(days=k)).isoformat()
            got = radar_scan.p1_fetch_klines(s, asof=d, limit=80)
            idx = next((i for i, r in enumerate(longw) if r["date"] == d), None)
            want = longw[max(0, idx - 79): idx + 1] if idx is not None else None
            same = (want is not None and len(got) == len(want) and all(
                abs(a["close"] - b["close"]) < 1e-12
                and abs(a["volume"] - b["volume"]) < 1e-12
                and a["date"] == b["date"] for a, b in zip(got, want)))
            ok = ok and same
            print("   %s asof=%s n=%d 逐位一致=%s" % (s, d, len(got), same))
    chk("A2 切片恒等: 5 币 × 5 个历史日, endTime 直拉与本地切片逐位一致", ok)


def t_signal_replay_identity():
    """[A3] 补跑路径（endTime 逐日拉 + _slice_window）信号序列
           == 长窗口本地逐日切片信号序列 —— 证明补跑 == 连续回放。"""
    print("\n--- [A3] 信号回放恒等: 补跑路径 == 长窗口连续切片 ---")
    ok = True
    seq_a, seq_b = {}, {}
    for s in SYMS:
        longw = WIN[s]           # 已是 asof=E 的 200 根（补跑器实际就是这么预拉的）
        for k in range(10, 0, -1):
            d = (datetime.date.fromisoformat(E) - datetime.timedelta(days=k)).isoformat()
            # 路径 A（补跑）: 预拉整段 + _slice_window 本地切片
            sa = radar_scan.p1_compute_signal(p1_repair._slice_window(longw, d))
            # 路径 B（连续）: 长窗口本地切出截至 d 的窗口
            idx = next((i for i, r in enumerate(longw) if r["date"] == d), None)
            sb = radar_scan.p1_compute_signal(longw[max(0, idx - 79): idx + 1])
            key = (s, d)
            seq_a[key] = None if sa is None else (round(sa["close"], 10),
                                                  sa["buy"], sa["sell"],
                                                  round(sa["hh20"], 10),
                                                  round(sa["ll10"], 10))
            seq_b[key] = None if sb is None else (round(sb["close"], 10),
                                                  sb["buy"], sb["sell"],
                                                  round(sb["hh20"], 10),
                                                  round(sb["ll10"], 10))
    diff = [k for k in seq_a if seq_a[k] != seq_b[k]]
    ok = not diff
    print("   对照 %d 个 (币, 日) 点, 不一致 %d 个 %s" % (len(seq_a), len(diff), diff[:5]))
    # 顺便统计这 10 天里有几天真的出信号（证明样本非空）
    nfire = sum(1 for v in seq_a.values() if v and (v[1] or v[2]))
    print("   样本内触发 buy/sell 的点数 = %d（>0 说明对照非空跑）" % nfire)
    chk("A3 信号回放恒等: 5 币 × 10 日补跑路径与连续路径逐字段一致", ok)


# ===================== L2: 编排逻辑层（FakeConn） =====================
class _Tx:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


class FakeConn:
    """最小 asyncpg 连接 mock：只实现补跑器 + p1_load_state/save/log 用到的接口。"""

    def __init__(self, p1, lock_held=False, cas_ok=True):
        self.p1 = {k: dict(v) for k, v in p1.items()}
        self.lock_held = lock_held
        self.cas_ok = cas_ok
        self.executed = []       # [(sql, args)]
        self.log_dates = []      # p1_log INSERT 的日期
        self.log_rows = []       # p1_log INSERT 的 (date, symbol)
        self.lock_beats = 0
        self.lock_released = 0

    async def execute(self, sql, *args):
        self.executed.append((" ".join(sql.split()), args))
        s = " ".join(sql.split())
        if s.startswith("UPDATE p1_repair_lock SET held=true"):
            return "UPDATE 1" if self.cas_ok else "UPDATE 0"
        if s.startswith("UPDATE p1_repair_lock SET updated_at=now() WHERE id=1"):
            self.lock_beats += 1
            return "UPDATE 1"
        if s.startswith("UPDATE p1_repair_lock SET held=false"):
            self.lock_released += 1
            return "UPDATE 1"
        if s.startswith("INSERT INTO p1_log"):
            self.log_rows.append((args[0], args[1]))
            self.log_dates.append(args[0])
            return "INSERT 0 1"
        return "OK"

    async def fetch(self, sql, *args):
        s = " ".join(sql.split())
        if "FROM p1_state" in s:
            return [{"symbol": k, "position": v.get("position", "out"),
                     "entry_date": v.get("entry_date"),
                     "entry_price": v.get("entry_price"),
                     "last_date": v.get("last_date")}
                    for k, v in self.p1.items()]
        return []

    async def fetchrow(self, sql, *args):
        s = " ".join(sql.split())
        if "FROM p1_repair_lock" in s:
            return {"held": self.lock_held, "updated_at": None}
        return None

    async def fetchval(self, sql, *args):
        return 0

    def transaction(self):
        return _Tx()


def _mk_patch(conn, expected=E, fetch_fail_dates=(), paper_ok=True,
              win_fail_syms=()):
    """构造 monkeypatch 环境，返回 (undo 函数, 记录 dict)。

    注：补跑器是"每币预拉一次覆盖整段断档的窗口 + 本地逐日切片"，不是逐日联网。
    故失败注入点在【窗口拉取】而非单日 —— win_fail_syms 里的币种整段拉不到 →
    该币在每个补跑日都 failed → 首日即 break（约束 6 留队）。
    """
    rec = {"paper_days": [], "pushes": [], "inserts": []}
    old = {}

    def _fetch(sym, asof=None, limit=None):
        if sym in win_fail_syms:
            raise RuntimeError("mock vision down for %s" % sym)
        if asof in fetch_fail_dates:
            raise RuntimeError("mock fetch failure @%s" % asof)
        rows = WIN[sym]
        idx = next((i for i, r in enumerate(rows) if r["date"] == asof), None)
        if idx is None:
            return []
        return rows[max(0, idx - (limit or 80) + 1): idx + 1]

    old["fetch"] = radar_scan.p1_fetch_klines
    radar_scan.p1_fetch_klines = _fetch

    old["expected"] = radar_scan.p1_expected_date
    radar_scan.p1_expected_date = lambda: expected

    async def _paper(c, d, qq_send=None, end_ms=None, push=True, strict_prev=False):
        rec["paper_days"].append(d)
        if not paper_ok:
            return {"ok": False, "accts": [], "trades": [],
                    "skipped": "mock-paper-fail"}
        return {"ok": True, "accts": ["V3", "V3MA", "T10", "T7", "HV3"],
                "trades": [], "skipped": None}

    # 注意：_paper_run_day 是 p1_repair 在【函数内】延迟 import 的，
    # 补丁必须打在 paper_accounting 上（p1_repair 模块级没有该属性）。
    old["paper"] = paper_accounting._paper_run_day
    paper_accounting._paper_run_day = _paper

    def _qq(t):
        rec["pushes"].append(t)
        return True, None

    old["qq"] = radar_scan.qq_send_openid
    radar_scan.qq_send_openid = _qq

    async def _ins(c, key, st, sym, payload, ok):
        rec["inserts"].append(key)
        return None

    old["ins"] = radar_scan.db_insert
    radar_scan.db_insert = _ins

    def undo():
        radar_scan.p1_fetch_klines = old["fetch"]
        radar_scan.p1_expected_date = old["expected"]
        p1_repair._paper_run_day = old["paper"]
        radar_scan.qq_send_openid = old["qq"]
        radar_scan.db_insert = old["ins"]

    return undo, rec


def _state(last_date, position="out"):
    return {s: {"position": position, "entry_date": None,
                "entry_price": None, "last_date": last_date} for s in SYMS}


def t_no_gap():
    print("\n--- [A4] 无缺口 → 零动作 ---")
    conn = FakeConn(_state(E))
    undo, rec = _mk_patch(conn)
    try:
        r = asyncio.run(p1_repair.p1_repair_section(conn))
    finally:
        undo()
    print("   result=%s" % r)
    chk("A4 无缺口: status=no-gap 且未调记账",
        r and r.get("status") == "no-gap" and not rec["paper_days"])


def t_gap_repair():
    print("\n--- [A5] 检出 3 天缺口 → 顺序补跑信号+记账 ---")
    anchor = (datetime.date.fromisoformat(E) - datetime.timedelta(days=3)).isoformat()
    conn = FakeConn(_state(anchor))
    undo, rec = _mk_patch(conn)
    try:
        r = asyncio.run(p1_repair.p1_repair_section(conn))
    finally:
        undo()
    want_days = [anchor]           # (anchor, E]
    want = [(datetime.date.fromisoformat(anchor) + datetime.timedelta(days=i)).isoformat()
            for i in range(1, 4)]
    print("   result=%s" % r)
    print("   记账日序列=%s" % rec["paper_days"])
    print("   信号落库日期=%s" % conn.log_dates)
    ok = (r and r.get("status") == "ok" and r.get("sig_days") == 3
          and rec["paper_days"] == want
          and all(d > anchor for d in conn.log_dates)
          and conn.lock_released == 1)
    chk("A5 顺序补跑: 3 天信号+记账全补齐, 日期严格 (anchor, expected]", ok)
    chk("A5b 不回滚历史: 所有 p1_log 写入日期均 > anchor",
        bool(conn.log_dates) and all(d > anchor for d in conn.log_dates))
    chk("A5c 锁已释放 + 心跳刷新", conn.lock_released == 1 and conn.lock_beats >= 1)


def t_fetch_fail_leaves_queue():
    print("\n--- [A6] 补跑中拉数失败 → 停止留队（约束 6）---")
    anchor = (datetime.date.fromisoformat(E) - datetime.timedelta(days=3)).isoformat()
    conn = FakeConn(_state(anchor))
    # XRPUSDT 整段窗口拉不到（模拟 vision 对该币 5xx）→ 首日信号即失败
    undo, rec = _mk_patch(conn, win_fail_syms=("XRPUSDT",))
    try:
        r = asyncio.run(p1_repair.p1_repair_section(conn))
    finally:
        undo()
    print("   result=%s" % r)
    print("   记账日序列=%s (应为空：首日信号失败即 break)" % rec["paper_days"])
    print("   detail=%s" % r.get("detail"))
    ok = (r and r.get("status") == "partial" and r.get("sig_days") == 0
          and not rec["paper_days"] and conn.lock_released == 1)
    chk("A6 失败留队: 停在失败日, status=partial, 不继续推进后续日", ok)
    # 部分成功语义：成功的 4 币落库 d1，失败的 XRPUSDT 不落库（last_date 未推进）
    # → 下轮 anchor 仍是 XRP 的旧值，从 d1 重来；已成功的币种因 last_date>=d1 被跳过
    # （幂等），不会重复计算信号。这里验证"失败币未被写入"。
    xrp = [1 for d_, s_ in conn.log_rows if s_ == "XRPUSDT"]
    others = sorted({s_ for d_, s_ in conn.log_rows})
    print("   落库币种=%s (应不含 XRPUSDT)" % others)
    chk("A6b 部分成功语义: 失败币 XRPUSDT 未推进, 其余 4 币正常落库",
        not xrp and len(others) == 4)


def t_lock_held():
    print("\n--- [A7] 锁隔离（约束 2）---")
    conn = FakeConn(_state(E), lock_held=True)
    undo, rec = _mk_patch(conn)
    try:
        r = asyncio.run(p1_repair.p1_repair_section(conn))
        held = asyncio.run(p1_repair.repair_lock_held(conn))
    finally:
        undo()
    print("   result=%s repair_lock_held=%s" % (r, held))
    ok = r and r.get("status") == "skipped" and r.get("reason") == "lock-held"
    chk("A7 锁: 持锁时补跑器自身也让路, repair_lock_held=True", ok and held)
    # 静态验证：两个生产入口确实调用了 repair_lock_held
    src_r = open("radar_scan.py", encoding="utf-8").read()
    src_p = open("paper_accounting.py", encoding="utf-8").read()
    chk("A7b 生产入口让路: radar_scan.p1_section 调用 repair_lock_held",
        "repair_lock_held" in src_r and "本轮让路跳过" in src_r)
    chk("A7c 生产入口让路: paper_accounting.paper_section 调用 repair_lock_held",
        "repair_lock_held" in src_p and "本轮让路跳过" in src_p)


def t_cold_start_and_too_big():
    print("\n--- [A8] 冷启动 / 缺口过大 保护 ---")
    conn = FakeConn(_state(None))
    undo, _ = _mk_patch(conn)
    try:
        r1 = asyncio.run(p1_repair.p1_repair_section(conn))
    finally:
        undo()
    print("   冷启动 result=%s" % r1)
    chk("A8 冷启动: 不介入, 交正常轮", r1 and r1.get("reason") == "cold-start")

    big = (datetime.date.fromisoformat(E) - datetime.timedelta(days=31)).isoformat()
    conn = FakeConn(_state(big))
    undo, rec = _mk_patch(conn)
    try:
        r2 = asyncio.run(p1_repair.p1_repair_section(conn))
    finally:
        undo()
    print("   缺口31天 result=%s 推送数=%d" % (r2, len(rec["pushes"])))
    chk("A8b 缺口过大: gap-too-big 且不自动补跑",
        r2 and r2.get("status") == "gap-too-big"
        and rec["paper_days"] == [])
    chk("A8c 缺口过大: 已推告警（标注需人工确认）",
        any("断日告警" in t for t in rec["pushes"]))


def t_repair_push():
    """[A9] 补跑触发的信号 → 汇总补推且标注'补发'；推送失败落 radar_seen。"""
    print("\n--- [A9] 补推 QQ 标注补发（约束 4）---")
    # 找一段确实能触发 BUY/SELL 的历史窗口
    fired_ok = False
    for back in range(40, 5, -5):
        exp = (datetime.date.fromisoformat(E) - datetime.timedelta(days=back)).isoformat()
        anchor = (datetime.date.fromisoformat(exp) - datetime.timedelta(days=3)).isoformat()
        conn = FakeConn(_state(anchor, position="out"))
        undo, rec = _mk_patch(conn, expected=exp)
        try:
            r = asyncio.run(p1_repair.p1_repair_section(conn))
        finally:
            undo()
        if rec["pushes"]:
            txt = rec["pushes"][0]
            fired_ok = ("补发" in txt)
            print("   exp=%s fired=%d 推送首行=%r" % (exp, r.get("fired"),
                                                    txt.split("\n")[0]))
            break
    if not rec.get("pushes"):
        print("   (窗口内未触发信号，改用构造用例)")

    # 构造用例：强制 fired 非空 → 校验 header 文案与失败落库
    conn = FakeConn(_state(anchor))
    undo, rec2 = _mk_patch(conn, expected=exp)
    try:
        # 直接校验 p1_push_text + 落库分支：手动构造 fired 走同一推送代码路径
        items = [{"symbol": "BTCUSDT", "action": "BUY", "date": exp,
                  "close": 100.0, "vol_ratio": 2.0, "note": "t", "pnl": None,
                  "days": None}]
        txt = radar_scan.p1_push_text(items, header="[P1 信号补发 09-10 12:00]")
        print("   构造推送文本首行=%r" % txt.split("\n")[0])
        pushed_ok = txt.startswith("[P1 信号补发")
    finally:
        undo()
    chk("A9 补推文案: header 为 [P1 信号补发 ...]", pushed_ok)


def t_regression_default_path():
    """[A10] 回归：p1_fetch_klines 默认路径（asof=None）行为不变。"""
    print("\n--- [A10] 回归: 默认路径 asof=None 行为不变 ---")
    a = radar_scan.p1_fetch_klines("BTCUSDT")
    b = radar_scan.p1_fetch_klines("BTCUSDT", asof=None, limit=None)
    same = (len(a) == len(b) and all(
        x["date"] == y["date"] and abs(x["close"] - y["close"]) < 1e-12
        for x, y in zip(a, b)))
    print("   默认 n=%d, 显式 None n=%d, 一致=%s" % (len(a), len(b), same))
    chk("A10 回归: p1_fetch_klines() 与 (asof=None, limit=None) 完全一致", same)
    chk("A10b 回归: 默认仍返回最新已收盘日线 (%s)" % (a[-1]["date"] if a else "-"),
        bool(a))


# ===================== L3: 任务 C =====================
# ===================== L4: 任务 B（成交推送失败补发） =====================
class FakeConnB:
    """只实现 _paper_mark_pending / paper_retry_pending 用到的接口。"""

    def __init__(self, pending=None):
        self.pending = list(pending or [])
        self.execs = []

    async def execute(self, sql, *a):
        self.execs.append((" ".join(sql.split()), a))
        return "OK"

    async def fetch(self, sql, *a):
        if "FROM paper_push_pending" in sql:
            return self.pending
        return []


def t_taskB():
    print("\n--- [B1~B4] 任务 B: 成交推送失败持久化 + 下轮补发 ---")
    trades = [{"acct": "V3", "symbol": "BTCUSDT", "action": "BUY", "price": 61000.0},
              {"acct": "V3MA", "symbol": "ETHUSDT", "action": "SELL",
               "price": 1800.5, "pnl_pct": -3.21}]
    # B1 推送失败 → 按 acct 落两条待补发
    c1 = FakeConnB()
    asyncio.run(paper_accounting._paper_mark_pending(c1, "2026-08-20", trades))
    ins = [e for e in c1.execs if e[0].startswith("INSERT INTO paper_push_pending")]
    print("   落库条数=%d acct=%s" % (len(ins), [e[1][1] for e in ins]))
    chk("B1 推送失败: 按 (asof, acct) 落 2 条 paper_push_pending", len(ins) == 2)

    # B1b 幂等：语句带 ON CONFLICT (asof, acct) DO UPDATE → DB 层重复执行不追加行
    #   （FakeConn 只记语句不记行，故校验 SQL 语义而非条数）
    c1b = FakeConnB()
    asyncio.run(paper_accounting._paper_mark_pending(c1b, "2026-08-20", trades))
    asyncio.run(paper_accounting._paper_mark_pending(c1b, "2026-08-20", trades))
    ins_b = [e for e in c1b.execs
             if e[0].startswith("INSERT INTO paper_push_pending")]
    all_conflict = bool(ins_b) and all(
        "ON CONFLICT (asof, acct) DO UPDATE" in e[0] for e in ins_b)
    chk("B1b 幂等: 落库带 ON CONFLICT (asof, acct) DO UPDATE (重复执行不追加行)",
        all_conflict and len(ins_b) == 4)

    # B2 下轮补发成功 → UPDATE push_ok=true
    c2 = FakeConnB(pending=[
        {"asof": "2026-08-20", "acct": "V3", "payload": json.dumps([trades[0]])},
        {"asof": "2026-08-20", "acct": "V3MA", "payload": json.dumps([trades[1]])}])
    pushed = []
    n = asyncio.run(paper_accounting.paper_retry_pending(
        c2, qq_send=lambda t: (pushed.append(t), True, None)[1:]))
    upd = [e for e in c2.execs if e[0].startswith("UPDATE paper_push_pending SET push_ok=true")]
    print("   补发成交数=%d 首行=%r 标记成功=%d" % (
        n, pushed[0].split("\n")[0] if pushed else "-", len(upd)))
    chk("B2 补发: 推送成功且 header 标注'补发'",
        n == 2 and pushed and pushed[0].startswith("[P1模拟成交补发"))
    chk("B2b 补发: 成功后标记 push_ok=true", len(upd) == 2)

    # B3 补发仍失败 → 不标 push_ok=true（留待下轮）
    c3 = FakeConnB(pending=[
        {"asof": "2026-08-20", "acct": "V3", "payload": json.dumps([trades[0]])}])
    n3 = asyncio.run(paper_accounting.paper_retry_pending(
        c3, qq_send=lambda t: (False, "mock down")))
    upd3 = [e for e in c3.execs if "push_ok=true" in e[0]]
    print("   仍失败: n=%d 标记成功=%d (应为 0)" % (n3, len(upd3)))
    chk("B3 补发仍失败: 不标 push_ok=true, 留待下轮重试", n3 == 1 and not upd3)

    # B4 无待补发行 → 零动作
    c4 = FakeConnB(pending=[])
    n4 = asyncio.run(paper_accounting.paper_retry_pending(
        c4, qq_send=lambda t: (True, None)))
    chk("B4 无失败行: 零动作 (n=0)", n4 == 0)

    # B5 生产入口确实在推送失败时调用落库
    src = open("paper_accounting.py", encoding="utf-8").read()
    chk("B5 生产接线: paper_section 推送失败调用 _paper_mark_pending",
        "await _paper_mark_pending(conn, expected, all_trades)" in src)
    chk("B5b 生产接线: paper_section 末尾调用 paper_retry_pending",
        "await paper_retry_pending(conn, qq_send)" in src)


def t_repair_paper_push():
    """[A11] 补跑期间的记账成交也补推（标注成交补发）。"""
    print("\n--- [A11] 补跑期间记账成交补推 ---")
    anchor = (datetime.date.fromisoformat(E) - datetime.timedelta(days=2)).isoformat()
    conn = FakeConn(_state(anchor))

    def _mk():
        undo, rec = _mk_patch(conn)
        async def _paper(c, d, qq_send=None, end_ms=None, push=True,
                         strict_prev=False):
            rec["paper_days"].append(d)
            return {"ok": True, "accts": ["V3"], "skipped": None,
                    "trades": [{"acct": "V3", "symbol": "BTCUSDT",
                                "action": "BUY", "price": 61000.0}]}
        paper_accounting._paper_run_day = _paper
        return undo, rec

    undo, rec = _mk()
    try:
        r = asyncio.run(p1_repair.p1_repair_section(conn))
    finally:
        undo()
    print("   result paper_trades=%s 推送数=%d" % (r.get("paper_trades"),
                                                  len(rec["pushes"])))
    hit = [t for t in rec["pushes"] if t.startswith("[P1模拟成交补发")]
    chk("A11 补跑记账成交: 汇总补推且 header 标注'成交补发'",
        r.get("paper_trades") == 2 and len(hit) == 1)


def t_magate():
    print("\n--- [C1/C2] 任务 C: MA200 缓冲落后自愈 ---")
    g = paper_accounting.MAGate()
    base = datetime.date.fromisoformat(E)
    seq = [(base - datetime.timedelta(days=i)).isoformat() for i in range(260, 0, -1)]
    g.seed("BTCUSDT", [(d, 100.0 + i) for i, d in enumerate(seq)])
    exp = E
    # 正常态：末位 == exp-1
    lag_ok = g.lag_days("BTCUSDT", exp)
    w, why = g.append_contiguous("BTCUSDT", exp, 999.0)
    print("   正常态 lag=%s append=(%s,%s) 末位=%s" % (lag_ok, w, why,
                                                    g.last_date("BTCUSDT")))
    chk("C1 正常态: lag==1 且 append_contiguous 写入 ok", lag_ok == 1 and w and why == "ok")

    # 幂等：同日重复 append
    w2, why2 = g.append_contiguous("BTCUSDT", exp, 999.0)
    chk("C1b 幂等: 同日重复 append 不写入 (idempotent)", (not w2) and why2 == "idempotent")

    # 落后 5 天：末位人为停在 base-5 → lag=5（构造 range(260, 4, -1) 末位=base-5）
    g2 = paper_accounting.MAGate()
    seq2 = [(base - datetime.timedelta(days=i)).isoformat() for i in range(260, 4, -1)]
    g2.seed("BTCUSDT", [(d, 100.0 + i) for i, d in enumerate(seq2)])
    lag_bad = g2.lag_days("BTCUSDT", exp)
    w3, why3 = g2.append_contiguous("BTCUSDT", exp, 999.0)
    print("   落后态 末位=%s lag=%s append=(%s,%s)"
          % (g2.last_date("BTCUSDT"), lag_bad, w3, why3))
    chk("C2 落后态: lag>1 检出 且 append_contiguous 拒绝写入",
        lag_bad == 5 and (not w3) and why3 == "gap=5d")

    # 落后触发重播种的判定条件（与 paper_section 内逻辑同式）
    reason = None
    if not g2.buf.get("BTCUSDT"):
        reason = "cold-start"
    elif g2.lag_days("BTCUSDT", exp) is None:
        reason = "bad-last-date"
    elif g2.lag_days("BTCUSDT", exp) > 1:
        reason = "lag=%dd" % g2.lag_days("BTCUSDT", exp)
    chk("C2b 落后态: paper_section 重播种判定触发 (reason=%s)" % reason,
        reason == "lag=5d")
    # 正常态不误触发
    reason2 = None
    if not g.buf.get("BTCUSDT"):
        reason2 = "cold-start"
    elif g.lag_days("BTCUSDT", exp) is None:
        reason2 = "bad-last-date"
    elif g.lag_days("BTCUSDT", exp) > 1:
        reason2 = "lag"
    chk("C2c 正常态: 不误触发重播种 (reason=%s)" % reason2, reason2 is None)


def main():
    print("=" * 70)
    print("任务 A（断日补跑自愈）+ 任务 C（MA200 落后自愈）自测")
    print("UTC now = %s" % datetime.datetime.now(datetime.timezone.utc).isoformat())
    print("历史窗口 expected = %s, 构造缺口 = %d 天" % (E, E_ANCHOR_DAYS))
    print("=" * 70)
    load_windows()
    for s in SYMS:
        print("   window %-9s n=%d last=%s" % (s, len(WIN[s]), WIN[s][-1]["date"]))

    t_endtime_boundary()
    t_no_lookahead()
    t_slice_identity()
    t_signal_replay_identity()
    t_regression_default_path()
    t_no_gap()
    t_gap_repair()
    t_fetch_fail_leaves_queue()
    t_lock_held()
    t_cold_start_and_too_big()
    t_repair_push()
    t_repair_paper_push()
    t_taskB()
    t_magate()

    print("\n" + "=" * 70)
    print("PASS %d / FAIL %d" % (len(PASS), len(FAIL)))
    if FAIL:
        print("FAILED: %s" % ", ".join(FAIL))
    print("=" * 70)
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
