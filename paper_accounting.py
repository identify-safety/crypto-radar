#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
P1 三方案模拟实盘记账模块（2026-09-08，蓬蒿2号，任务书 HERMES_REQUEST_三方案模拟实盘.md）

定位：在 crypto-radar 云端雷达的 P1 信号段之后，并行模拟三套仓位方案的纸面账户。
      - 不改动 P1 信号层（radar_scan.py 的 p1_*，commit b556c39）
      - 只读取 p1_state 的当日 position 变化来驱动本模块
      - 会计数学与 quant_portfolio_sizing.run_engine 严格一致（见 self-test）

三方案（离散写死，与主测/变体一致）：
      V3 : BTC/ETH/SOL/XRP/DOGE 各 20% | cap 100% | 再平衡 band 50%（涨超目标×1.5 卖回目标, 只减不买）
      T10: BTC30/ETH15/SOL6/XRP3/DOGE6（归一后 ×cap60）| cap 60% | 无再平衡
      T7 : BTC 33% | cap 33% | 无再平衡

实时 vs 回测的关键差异（两段式）：
      回测:  T 日收盘信号 → T+1 开盘价成交（价格已知，close-to-open 一步完成）
      实时:  T 日信号确认只记 pending；下一 gate 轮（T+1 新日线开盘）才用开盘价执行 pending
      → 本模块每个记账轮：先执行上一轮留下的 pending（用本轮开盘价），再检测本轮 p1_state
        变化写新 pending（下轮执行），再做 V3 再平衡，最后计息记 NAV。

执行/成本/计息/再平衡数学（必须与 run_engine 逐字一致，否则 self-test <0.1% 不通过）：
      - 成交价 = 本轮日线开盘价（与 run_engine 的 open[j] 一致）
      - 买入 N = weights[s] × V_pre（V_pre = 本轮开盘价口径的总权益，含现金+持仓市值）
      - 总敞口约束 room = cap × V_pre − used；N>room 且非部分成交 → 整笔跳过（n_skip++）
      - 现金不足 N>cash → N 压到 cash（不透支，与 run_engine 一致）
      - 成本 taker 双边 0.15%/边：买入 cash-=N 且 units+=N*(1-c)/p；卖出 cash+=P*(1-c)
      - 现金计息：逐轮几何 cash *= (1+5%)^(dt/365)，dt=相邻记账轮自然日差
      - 再平衡（仅 V3）：持仓市值权重 > 目标×(1+band) 时卖回目标权重（cost 照扣），只减不买
      - NAV = cash + Σ持仓市值（用本轮开盘价标记，与 run_engine 的 V[j] 标记口径一致）
      - 重叠处理：先卖后买；买入按【目标权重降序 → 同权重按 SYMBOLS5 顺序】确定性优先

幂等：同一记账轮（同一 asof 日）重复跑不重复成交——pending 执行后即删；NAV 主键
      (acct,date) 冲突跳过；信号检测基于 paper_pos 与 p1_state 的状态差，天然去重。
"""

import os
import json
import math
import datetime

# ---------------- 常量（与 run_engine / 任务书一致）----------------
COST_PER_SIDE = 0.0015          # taker 双边 0.15%/边
CASH_RATE = 0.05               # 现金年化 5% 计息
INIT_CASH = 100000.0           # 三账户初始各 10 万 USDT 名义，全现金空仓
PAPER_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT"]

# 三方案定义（离散写死，不得寻优；与 quant_portfolio_sizing 主测/变体口径一致）
SCHEMES = {
    "V3": {
        "name": "V3 等权+再平衡",
        "weights": {s: 0.20 for s in PAPER_SYMBOLS},
        "cap": 1.00, "rebalance_band": 0.50, "allow_partial": False,
    },
    "T10": {
        "name": "#10 分层cap60",
        "weights": {"BTCUSDT": 0.30, "ETHUSDT": 0.15, "SOLUSDT": 0.06,
                    "XRPUSDT": 0.03, "DOGEUSDT": 0.06},
        "cap": 0.60, "rebalance_band": None, "allow_partial": False,
    },
    "T7": {
        "name": "#7 33%BTC",
        "weights": {"BTCUSDT": 0.33},
        "cap": 0.33, "rebalance_band": None, "allow_partial": False,
    },
}
ACCT_ORDER = ["V3", "T10", "T7"]   # 推送/表格固定顺序


def _iso_date(dt):
    if isinstance(dt, str):
        return dt[:10]
    return dt.strftime("%Y-%m-%d")


# ===================== 纯逻辑账户（可离线测试）=====================
class PaperAccount:
    """单账户内存状态机。round_step 与 run_engine 单 bar 数学严格对应。

    self.cash       : 现金（USDT 名义）
    self.pos        : {symbol: {"units": float, "entry_price": float, "entry_date": str}}
    self.pending    : {symbol: {"action": "BUY"/"SELL", "signal_date": str, "signal_price": float}}
    self.last_date  : 最近一次记账日（ISO date），用于计息 dt
    """

    def __init__(self, acct, scheme_name, weights, cap, rebalance_band,
                 allow_partial, cash=INIT_CASH, pos=None, pending=None, last_date=None):
        self.acct = acct
        self.scheme_name = scheme_name
        self.weights = dict(weights)
        self.cap = cap
        self.rebalance_band = rebalance_band
        self.allow_partial = allow_partial
        self.cash = cash
        self.pos = pos or {}                 # symbol -> dict
        self.pending = pending or {}         # symbol -> dict
        self.last_date = last_date
        # 买入优先级：目标权重降序 → 同权重按 SYMBOLS5 顺序（确定性）
        self.order = sorted(self.weights.keys(),
                            key=lambda s: (-self.weights[s], s))

    # ---- 开盘价获取（live: binance.vision；测试可注入）----
    @staticmethod
    def fetch_open(symbol, asof, price_fn=None):
        if price_fn is not None:
            return price_fn(symbol, asof)
        # live: 取 asof 日线开盘价（最新已收盘日线即 asof 当日）
        try:
            import urllib.request
            url = ("https://data-api.binance.vision/api/v3/klines?symbol=%s"
                   "&interval=1d&limit=5" % symbol)
            op = urllib.request.build_opener(urllib.request.ProxyHandler({}))
            req = urllib.request.Request(url,
                                        headers={"User-Agent": "p1-paper/1.0"})
            with op.open(req, timeout=20) as r:
                raw = json.loads(r.read().decode())
            for k in raw:
                d = datetime.datetime.fromtimestamp(int(k[0]) / 1000,
                        datetime.timezone.utc).strftime("%Y-%m-%d")
                if d <= asof:               # 取 <=asof 的最新已收盘日线开盘
                    return float(k[1])
        except Exception:
            pass
        return float("nan")

    # ---- 一个记账轮（对应 run_engine 单 bar j）----
    def round_step(self, opens, p1_state, asof):
        """执行一个记账轮，返回 (trades, nav, cash_ratio)。

        opens    : {symbol: 本轮开盘价}
        p1_state : {symbol: {"position": "in"/"out", "entry_price":.., "entry_date":..}}
                   来自 p1_state 表（P1 信号段已更新）
        asof     : 本轮记账日（ISO date），用于计息 dt 与 NAV 主键
        """
        trades = []
        # (0) 计息（与 run_engine: 轮首、j>0 时按 dt 几何累加）
        if self.last_date:
            try:
                dt = (datetime.date.fromisoformat(asof) -
                      datetime.date.fromisoformat(self.last_date)).days
            except Exception:
                dt = 0
            if dt > 0 and self.cash > 0:
                self.cash *= (1.0 + CASH_RATE) ** (dt / 365.0)
        else:
            dt = 0

        # (1) V_pre = 本轮开盘价口径总权益
        def v_pre():
            v = self.cash
            for s, h in self.pos.items():
                p = opens.get(s)
                if p and math.isfinite(p) and p > 0:
                    v += h["units"] * p
            return v
        Vp = v_pre()

        # (2) 执行 pending（两段式：上一轮信号本轮开盘价成交）
        #     先卖（回笼现金）后买（按优先级，受 cap 约束）—— 与 run_engine 顺序一致
        sells, buys = [], []
        for s, pd in self.pending.items():
            if pd["action"] == "SELL":
                sells.append(s)
            else:
                buys.append(s)
        # 卖
        for s in sells:
            h = self.pos.get(s)
            p = opens.get(s)
            if not h or not (p and math.isfinite(p) and p > 0):
                continue
            pv = h["units"] * p
            self.cash += pv * (1.0 - COST_PER_SIDE)
            trades.append({
                "acct": self.acct, "date": asof, "symbol": s, "action": "SELL",
                "price": p, "notional": pv,
                "cost": pv * COST_PER_SIDE,
                "pnl_pct": ((p / h["entry_price"] - 1.0) * 100.0
                            if h.get("entry_price") else None),
                "note": "pending exec @open",
            })
            self.pos.pop(s, None)
            self.pending.pop(s, None)
        # 买（用 V_pre 与 room 约束，与 run_engine 完全一致）
        used = 0.0
        for s in self.pos:
            p = opens.get(s)
            if p and math.isfinite(p) and p > 0:
                used += self.pos[s]["units"] * p
        for s in self.order:
            if s not in buys:
                continue
            p = opens.get(s)
            if not (p and math.isfinite(p) and p > 0):
                continue
            N = self.weights[s] * Vp
            room = self.cap * Vp - used
            if room <= 1e-12:
                self.pending.pop(s, None)   # 整笔跳过，不再留 pending
                continue
            if N > room + 1e-12:
                if self.allow_partial:
                    N = max(room, 0.0)
                else:
                    self.pending.pop(s, None)   # 整笔跳过
                    continue
            if N > self.cash:                  # 无杠杆，不透支
                N = self.cash
            if N <= 1e-15:
                self.pending.pop(s, None)
                continue
            self.cash -= N
            units = N * (1.0 - COST_PER_SIDE) / p
            ep = (p1_state.get(s) or {}).get("entry_price")
            ed = (p1_state.get(s) or {}).get("entry_date")
            self.pos[s] = {"units": self.pos.get(s, {}).get("units", 0.0) + units,
                           "entry_price": ep if ep else p,
                           "entry_date": ed if ed else asof}
            trades.append({
                "acct": self.acct, "date": asof, "symbol": s, "action": "BUY",
                "price": p, "notional": N, "cost": N * COST_PER_SIDE,
                "pnl_pct": None, "note": "pending exec @open",
            })
            used += N * (1.0 - COST_PER_SIDE)
            self.pending.pop(s, None)

        # (3) 检测 p1_state 当日变化 → 写新 pending（下轮执行，本轮不成交）
        for s in PAPER_SYMBOLS:
            st = p1_state.get(s) or {}
            target = st.get("position", "out")
            have = "in" if (self.pos.get(s, {}).get("units", 0.0) > 0) else "out"
            if s in self.pending:
                continue
            if target == "in" and have == "out":
                self.pending[s] = {"action": "BUY",
                                   "signal_date": asof,
                                   "signal_price": st.get("entry_price")}
            elif target == "out" and have == "in":
                self.pending[s] = {"action": "SELL",
                                   "signal_date": asof,
                                   "signal_price": st.get("exit_price")}

        # (4) V3 再平衡（仅 rebalance_band 设定者）：涨超目标×(1+band) 卖回目标，只减不买
        if self.rebalance_band and self.rebalance_band > 0:
            v_now = self.cash
            for s, h in self.pos.items():
                p = opens.get(s)
                if p and math.isfinite(p) and p > 0:
                    v_now += h["units"] * p
            for s, h in list(self.pos.items()):
                p = opens.get(s)
                if not (p and math.isfinite(p) and p > 0):
                    continue
                cur = h["units"] * p
                tgt = self.weights[s] * v_now
                if cur > tgt * (1.0 + self.rebalance_band) + 1e-12:
                    sell_amt = cur - tgt
                    if sell_amt <= 1e-15:
                        continue
                    self.cash += sell_amt * (1.0 - COST_PER_SIDE)
                    trades.append({
                        "acct": self.acct, "date": asof, "symbol": s,
                        "action": "REBAL", "price": p, "notional": sell_amt,
                        "cost": sell_amt * COST_PER_SIDE, "pnl_pct": None,
                        "note": "rebal band=%.2f" % self.rebalance_band,
                    })
                    h["units"] -= sell_amt / p

        # (5) 记 NAV（开盘价口径，与 run_engine V[j] 一致）
        nav = self.cash
        for s, h in self.pos.items():
            p = opens.get(s)
            if p and math.isfinite(p) and p > 0:
                nav += h["units"] * p
        cash_ratio = (self.cash / nav) if nav > 0 else 1.0
        self.last_date = asof
        return trades, nav, cash_ratio


# ===================== asyncpg 持久层（云端用，需 asyncpg）=====================
async def paper_ensure_tables(conn):
    """建表幂等 + 三账户种子（全现金 100000、空仓、无 pending）。"""
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS paper_acct(
            acct TEXT PRIMARY KEY, scheme TEXT, cash DOUBLE PRECISION,
            last_date TEXT, updated_at TIMESTAMPTZ DEFAULT now());
        CREATE TABLE IF NOT EXISTS paper_pos(
            acct TEXT, symbol TEXT, units DOUBLE PRECISION,
            entry_price DOUBLE PRECISION, entry_date TEXT,
            PRIMARY KEY(acct, symbol));
        CREATE TABLE IF NOT EXISTS paper_pending(
            acct TEXT, symbol TEXT, action TEXT,
            signal_date TEXT, signal_price DOUBLE PRECISION,
            PRIMARY KEY(acct, symbol, signal_date));
        CREATE TABLE IF NOT EXISTS paper_trade(
            id BIGSERIAL PRIMARY KEY, acct TEXT, date TEXT, symbol TEXT,
            action TEXT, price DOUBLE PRECISION, notional DOUBLE PRECISION,
            cost DOUBLE PRECISION, pnl_pct REAL, note TEXT);
        CREATE TABLE IF NOT EXISTS paper_nav(
            acct TEXT, date TEXT, nav DOUBLE PRECISION, cash_ratio REAL,
            PRIMARY KEY(acct, date));
    """)
    for acct, sc in SCHEMES.items():
        await conn.execute(
            "INSERT INTO paper_acct(acct, scheme, cash) VALUES($1,$2,$3) "
            "ON CONFLICT (acct) DO NOTHING", acct, sc["name"], INIT_CASH)


async def paper_load(conn, acct):
    """从 DB 载入单账户到 PaperAccount。"""
    sc = SCHEMES[acct]
    row = await conn.fetchrow(
        "SELECT cash, last_date FROM paper_acct WHERE acct=$1", acct)
    cash = float(row["cash"]) if row else INIT_CASH
    last = row["last_date"] if row and row["last_date"] else None
    pos = {}
    for r in await conn.fetch(
            "SELECT symbol, units, entry_price, entry_date FROM paper_pos "
            "WHERE acct=$1", acct):
        if r["units"] and r["units"] > 0:
            pos[r["symbol"]] = {"units": float(r["units"]),
                               "entry_price": float(r["entry_price"]) if r["entry_price"] else None,
                               "entry_date": r["entry_date"]}
    pending = {}
    for r in await conn.fetch(
            "SELECT symbol, action, signal_date, signal_price FROM paper_pending "
            "WHERE acct=$1", acct):
        pending[r["symbol"]] = {"action": r["action"],
                                "signal_date": r["signal_date"],
                                "signal_price": float(r["signal_price"]) if r["signal_price"] else None}
    return PaperAccount(acct, sc["name"], sc["weights"], sc["cap"],
                        sc["rebalance_band"], sc["allow_partial"],
                        cash=cash, pos=pos, pending=pending, last_date=last)


async def paper_save(conn, acct, obj, trades, nav, cash_ratio, asof):
    """持久化一个记账轮的结果（事务内）。"""
    async with conn.transaction():
        await conn.execute(
            "UPDATE paper_acct SET cash=$1, last_date=$2, updated_at=now() WHERE acct=$3",
            obj.cash, asof, acct)
        # pos upsert / delete
        for s, h in obj.pos.items():
            if h["units"] > 0:
                await conn.execute(
                    "INSERT INTO paper_pos(acct, symbol, units, entry_price, entry_date) "
                    "VALUES($1,$2,$3,$4,$5) ON CONFLICT (acct, symbol) DO UPDATE "
                    "SET units=$3, entry_price=$4, entry_date=$5",
                    acct, s, h["units"], h.get("entry_price"), h.get("entry_date"))
        # 清掉已平仓
        held = set(obj.pos.keys())
        await conn.execute(
            "DELETE FROM paper_pos WHERE acct=$1 AND symbol <> ALL($2::text[])",
            acct, list(held) or [""])
        # pending：删除全部再重写（pending 已在 round_step 内增删完毕）
        await conn.execute("DELETE FROM paper_pending WHERE acct=$1", acct)
        for s, pd in obj.pending.items():
            await conn.execute(
                "INSERT INTO paper_pending(acct, symbol, action, signal_date, signal_price) "
                "VALUES($1,$2,$3,$4,$5) ON CONFLICT (acct, symbol, signal_date) DO NOTHING",
                acct, s, pd["action"], pd["signal_date"], pd.get("signal_price"))
        # trades
        for t in trades:
            await conn.execute(
                "INSERT INTO paper_trade(acct, date, symbol, action, price, "
                "notional, cost, pnl_pct, note) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)",
                t["acct"], t["date"], t["symbol"], t["action"], t["price"],
                t["notional"], t["cost"], t["pnl_pct"], t["note"])
        # nav（主键冲突即跳过 → 幂等）
        await conn.execute(
            "INSERT INTO paper_nav(acct, date, nav, cash_ratio) VALUES($1,$2,$3,$4) "
            "ON CONFLICT (acct, date) DO NOTHING",
            acct, asof, nav, cash_ratio)


def _fmt_px(v):
    a = abs(v)
    if a >= 1000:
        return "%.2f" % v
    if a >= 1:
        return "%.4f" % v
    return "%.6f" % v


def _push_text(all_trades):
    """三账户成交合并为一条 QQ；无成交返回 None（静默）。"""
    if not all_trades:
        return None
    lines = ["[P1模拟 %s]" % datetime.datetime.now(
        datetime.timezone(datetime.timedelta(hours=8))).strftime("%Y-%m-%d")]
    for acct in ACCT_ORDER:
        ts = [t for t in all_trades if t["acct"] == acct]
        if not ts:
            continue
        for t in ts:
            if t["action"] == "BUY":
                lines.append("%s BUY %s @%s" % (acct, t["symbol"], _fmt_px(t["price"])))
            elif t["action"] == "SELL":
                pnl = (" pnl=%+.2f%%" % t["pnl_pct"]) if t.get("pnl_pct") is not None else ""
                lines.append("%s SELL %s @%s%s" % (acct, t["symbol"], _fmt_px(t["price"]), pnl))
            else:
                lines.append("%s REBAL %s @%s" % (acct, t["symbol"], _fmt_px(t["price"])))
    return "\n".join(lines)


async def paper_section(conn, qq_send=None):
    """云端主入口：在 P1 信号段之后调用，独立 try/except 包裹（异常不影响费率段）。

    qq_send: 可选推送函数(text)->(ok,err)；默认用 radar_scan.qq_send_openid（延迟导入避免耦合）。
    """
    if conn is None:
        print("[PAPER] skipped: no DATABASE_URL")
        return
    # 避免与 radar_scan 的循环依赖：按需导入
    if qq_send is None:
        try:
            from radar_scan import qq_send_openid
            qq_send = qq_send_openid
        except Exception:
            qq_send = None

    # 动态 import p1_expected_date（同仓库）
    try:
        from radar_scan import p1_expected_date
        expected = p1_expected_date()
    except Exception:
        expected = _iso_date(datetime.date.today() - datetime.timedelta(days=1))

    await paper_ensure_tables(conn)
    rows = await conn.fetch(
        "SELECT acct, scheme, cash, last_date FROM paper_acct")
    if not rows:
        await paper_ensure_tables(conn)
        rows = await conn.fetch("SELECT acct, scheme, cash, last_date FROM paper_acct")

    # gate：仅对 last_date != expected 的账户记账（其余轮跳过，幂等）
    need = []
    for r in rows:
        last = r["last_date"] if r["last_date"] else None
        if last != expected:
            need.append(r["acct"])
    if not need:
        print("[PAPER] gate: all accounted for %s → skip" % expected)
        return
    print("[PAPER] gate: 记账 %s @expected=%s" % (",".join(need), expected))

    # 取本轮各币开盘价（live: binance.vision；可注入 price_fn 测试）
    price_fn = getattr(paper_section, "_price_fn", None)
    opens = {s: PaperAccount.fetch_open(s, expected, price_fn) for s in PAPER_SYMBOLS}

    # 读 p1_state 当日 position
    st = await conn.fetch(
        "SELECT symbol, position, entry_price, entry_date, exit_price, last_date "
        "FROM p1_state WHERE symbol = ANY($1)", PAPER_SYMBOLS)
    p1 = {}
    for r in st:
        p1[r["symbol"]] = {"position": r["position"] or "out",
                           "entry_price": float(r["entry_price"]) if r["entry_price"] else None,
                           "entry_date": r["entry_date"],
                           "exit_price": float(r["exit_price"]) if r["exit_price"] else None,
                           "last_date": r["last_date"]}

    # 🟡 守护: 仅当 5 个币 p1_state.last_date 全部 == expected 才记账，
    #          否则 fetch_open 可能拉到滞后日开盘价当成交价（对齐 P1 段 lag 自愈）。
    #          未对齐时整段跳过、不写 last_date，下一 gate 轮重试。
    if len(st) < len(PAPER_SYMBOLS) or any(
            (p1.get(s, {}).get("last_date") or "") != expected for s in PAPER_SYMBOLS):
        print("[PAPER] guard: p1_state 尚未全部对齐 expected=%s → skip (retry next gate)" % expected)
        return

    all_trades = []
    for acct in ACCT_ORDER:
        if acct not in need:
            continue
        obj = await paper_load(conn, acct)
        trades, nav, cr = obj.round_step(opens, p1, expected)
        await paper_save(conn, acct, obj, trades, nav, cr, expected)
        all_trades.extend(trades)
        print("[PAPER] %s nav=%.2f cash_ratio=%.3f trades=%d" %
              (acct, nav, cr, len(trades)))

    text = _push_text(all_trades)
    if text and qq_send:
        ok, e = qq_send(text)
        print("[PAPER] push ok=%s %s" % (ok, e))
    elif text:
        print("[PAPER] (no push fn) %s" % text.replace("\n", " | "))


# 供测试注入价格源（云端默认 None → 走 binance.vision 实时开盘价）
def set_price_fn(fn):
    paper_section._price_fn = fn
