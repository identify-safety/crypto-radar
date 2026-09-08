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

# ---- HYPE 观察线（HV3 账户，仅池子 DOGE→HYPE 替换）----
# HYPE(即 Hyperliquid 代币 HYPE)信号在 paper_accounting 内独立自算（不扩展 P1_SYMBOLS、不动信号段/推送段）。
# 冻结参数与 P1 段完全一致：20日高突破 + 量≥1.5×20日均量 → BUY；破10日低 → SELL。
# 数据源（任务书 2026-09-08 更新）：Binance 无 HYPEUSDT（现货/合约均 400/-1121）→ 走数据不足兜底；
#   优先 OKX 现货 HYPE-USDT(/api/v5/market/candles?instId=HYPE-USDT&bar=1D)，
#   备选 Hyperliquid 原生 API(/info candleSnapshot)。拉数失败或 <60 根 → 数据不足，4 币照跑。
HYPE_SYMBOL = "HYPEUSDT"    # 观察标的（任务书原符号；Binance 未上架 → 常态数据不足兜底）
HV3_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", HYPE_SYMBOL]
P1_HH = 20           # 前 20 日最高收（不含当日）
P1_LL = 10           # 前 10 日最低收（不含当日）
P1_VM = 1.5          # 量 ≥ 1.5×20日均量
HYPE_MIN_KLINES = 60 # 不足 60 根 K 线 → 数据不足（任务书要求）

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
    "HV3": {
        "name": "HV3 等权+再平衡(观察·HYPE替DOGE)",
        "weights": {s: 0.20 for s in HV3_SYMBOLS},
        "cap": 1.00, "rebalance_band": 0.50, "allow_partial": False,
    },
}
ACCT_ORDER = ["V3", "T10", "T7", "HV3"]   # 推送/表格固定顺序（HV3 末位，标注观察）
ACCT_DISPLAY = {"HV3": "HV3(观察)"}        # 推送文本里 HV3 的展示名（任务书要求标注）


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
    def round_step(self, opens, p1_state, asof, symbols=None):
        """执行一个记账轮，返回 (trades, nav, cash_ratio)。

        opens    : {symbol: 本轮开盘价}
        p1_state : {symbol: {"position": "in"/"out", "entry_price":.., "entry_date":..}}
                   来自 p1_state 表（P1 信号段已更新）
        asof     : 本轮记账日（ISO date），用于计息 dt 与 NAV 主键
        symbols  : 本账户参与记账的币种列表（默认 PAPER_SYMBOLS；
                   HV3 传 HV3_SYMBOLS，使 HYPE 取代 DOGE 进入信号检测）
        """
        if symbols is None:
            symbols = PAPER_SYMBOLS
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
        for s in symbols:
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


# ===================== HYPE 观察线信号（独立自算，仅 HV3 用）=====================
def _hype_fetch_okx():
    """OKX 现货 HYPE-USDT 日线（优先源）。返回升序 rows[{date,open,close,volume}] 或 None。

    OKX 返回 data 数组最新在前，元素：[ts_ms(开盘), open, high, low, close, vol, ...]
    ⚠️ bar 必须用 1Dutc 而非 1D：OKX 的 1D 是香港时间(UTC+8)对齐
       （实测 1D 末根 ts=2026-09-07T16:00Z，1Dutc 末根 ts=2026-09-08T00:00Z）。
       用 1D 会把 HKT 自然日错标成前一 UTC 日（差 1 天），且最新一个已完成的 UTC
       自然日会被"未收盘"过滤误删 → HYPE 信号比 P1 其余币慢一天，破坏与 V3 的可比性。
    """
    try:
        import time, urllib.request
        url = ("https://www.okx.com/api/v5/market/candles"
               "?instId=HYPE-USDT&bar=1Dutc&limit=400")   # 1Dutc: UTC 日界，与 P1 口径一致
        op = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        req = urllib.request.Request(url, headers={"User-Agent": "p1-paper/1.0"})
        with op.open(req, timeout=20) as r:
            raw = json.loads(r.read().decode())
        if not isinstance(raw, dict) or raw.get("code") not in (None, "0"):
            return None
        data = raw.get("data") or []
        if not data:
            return None
        now_ms = time.time() * 1000
        rows = []
        for c in data:                            # 最新在前
            ot = int(c[0])                        # 开盘时间 ms(UTC)
            if ot + 86400000 > now_ms:            # 未收盘当天 K 线丢弃
                continue
            d = datetime.datetime.fromtimestamp(ot / 1000, datetime.timezone.utc)
            rows.append({"date": d.strftime("%Y-%m-%d"),
                         "open": float(c[1]), "close": float(c[4]),
                         "volume": float(c[5])})
        rows.sort(key=lambda x: x["date"])
        return rows
    except Exception:
        return None


def _hype_fetch_hl():
    """Hyperliquid 原生 candleSnapshot（备选源）。返回升序 rows[{date,open,close,volume}] 或 None。

    请求体必须嵌套在 "req" 内，否则 Hyperliquid 报 "Failed to deserialize"。
    返回对象数组：{t:ms开盘, T:ms收盘, s, o,h,l,c,v(均为字符串), n}
    """
    try:
        import time, urllib.request
        end = int(time.time() * 1000)
        start = end - 365 * 3 * 86400000          # 近 3 年，足够 >60 日线
        body = json.dumps({"type": "candleSnapshot",
                           "req": {"coin": "HYPE", "interval": "1d",
                                   "startTime": start, "endTime": end}}).encode()
        req = urllib.request.Request(
            "https://api.hyperliquid.xyz/info", data=body,
            headers={"Content-Type": "application/json", "User-Agent": "p1-paper/1.0"})
        op = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with op.open(req, timeout=20) as r:
            data = json.loads(r.read().decode())
        if not data:
            return None
        now_ms = time.time() * 1000
        rows = []
        for c in data:
            t = int(c["t"])
            if t + 86400000 > now_ms:             # 未收盘当天 K 线丢弃
                continue
            d = datetime.datetime.fromtimestamp(t / 1000, datetime.timezone.utc)
            rows.append({"date": d.strftime("%Y-%m-%d"),
                         "open": float(c["o"]), "close": float(c["c"]),
                         "volume": float(c["v"])})
        rows.sort(key=lambda x: x["date"])
        return rows
    except Exception:
        return None


HYPE_LAST = {"source": None, "n": 0}   # 最近一次 HYPE 拉数的实际来源/根数（仅用于日志可观测）


def _hype_fetch_klines(asof, src_fn=None):
    """拉 HYPE 日线（OKX 优先 + Hyperliquid 备选），返回升序 rows[{date,open,close,volume}] 或 None。

    None 情形：两源均异常 / HYPE 未上架 / 已收盘日线 < HYPE_MIN_KLINES(60)。
    后者触发 HV3 的"数据不足"兜底：账户照跑其他4币，HYPE 视为 out。
    统一丢弃未收盘当天及 >asof 的 K 线，仅保留 date<=asof 的已收盘日线（与 P1 口径一致）。
    src_fn(asof)->rows 可注入（自测）；注入数据同样执行 asof 过滤与 <60 根阈值判定。
    """
    if src_fn is not None:
        rows = src_fn(asof)
        HYPE_LAST["source"], HYPE_LAST["n"] = "inject", (len(rows) if rows else 0)
    else:
        rows = _hype_fetch_okx()
        if rows:
            HYPE_LAST["source"] = "okx"
        else:
            rows = _hype_fetch_hl()
            HYPE_LAST["source"] = "hyperliquid" if rows else None
        HYPE_LAST["n"] = len(rows) if rows else 0
    if not rows:
        return None
    rows = [r for r in rows if r.get("date", "") <= asof]   # 仅保留 <=asof 的已收盘日线
    if len(rows) < HYPE_MIN_KLINES:
        return None
    return rows


def _hype_open_for(asof, rows):
    """取 asof(或最近 <=asof)当天开盘价作 HYPE 执行价，与 P1 fetch_open 口径一致。

    rows 已升序且已过滤 <=asof → 末位即最近 <=asof 的那根，取其 open。
    """
    if not rows:
        return float("nan")
    return float(rows[-1]["open"])


def _hype_compute_signal(rows):
    """与 radar_scan.p1_compute_signal 同口径：窗口严格不含当日。返回 dict 或 None(样本不足)。"""
    if not rows or len(rows) < 21:
        return None
    cur = rows[-1]
    prev = rows[:-1]
    if len(prev) < 20:
        return None
    hh20 = max(r["close"] for r in prev[-P1_HH:])
    ll10 = min(r["close"] for r in prev[-P1_LL:])
    vma20 = sum(r["volume"] for r in prev[-20:]) / 20.0
    vol_ratio = (cur["volume"] / vma20) if vma20 > 0 else float("nan")
    return {"date": cur["date"], "close": cur["close"], "hh20": hh20, "ll10": ll10,
            "vma20": vma20, "vol_ratio": vol_ratio,
            "buy": bool(cur["close"] > hh20 and cur["volume"] >= P1_VM * vma20),
            "sell": bool(cur["close"] < ll10)}


def _hype_decide(position, sig):
    """状态机：out 只查买、in 只查卖（与 radar_scan.p1_decide 一致）。"""
    if position == "in":
        return "SELL" if sig["sell"] else "none"
    return "BUY" if sig["buy"] else "none"


async def _hype_read_state(conn):
    """读 p1_state.HYPEUSDT 行（不存在则建行）。仅 HV3 维护该行。"""
    await conn.execute(
        "INSERT INTO p1_state(symbol, position) VALUES($1,'out') "
        "ON CONFLICT (symbol) DO NOTHING", HYPE_SYMBOL)
    r = await conn.fetchrow(
        "SELECT position, entry_price, entry_date, exit_price, last_date "
        "FROM p1_state WHERE symbol=$1", HYPE_SYMBOL)
    if not r:
        return {"position": "out", "entry_price": None, "entry_date": None,
                "exit_price": None, "last_date": None}
    return {"position": r["position"] or "out",
            "entry_price": float(r["entry_price"]) if r["entry_price"] else None,
            "entry_date": r["entry_date"],
            "exit_price": float(r["exit_price"]) if r["exit_price"] else None,
            "last_date": r["last_date"]}


async def _hype_write_state(conn, asof, st):
    """写回 p1_state.HYPEUSDT（position/进出价/last_date）。P1 信号段不处理该行。"""
    await conn.execute(
        "INSERT INTO p1_state(symbol, position, entry_date, entry_price, "
        "exit_date, exit_price, last_date, updated_at) "
        "VALUES($1,$2,$3,$4,$5,$6,$7,now()) "
        "ON CONFLICT (symbol) DO UPDATE SET position=$2, entry_date=$3, "
        "entry_price=$4, exit_date=$5, exit_price=$6, last_date=$7, updated_at=now()",
        HYPE_SYMBOL, st.get("position", "out"), st.get("entry_date"),
        st.get("entry_price"), st.get("exit_date"), st.get("exit_price"), asof)


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
        disp = ACCT_DISPLAY.get(acct, acct)
        ts = [t for t in all_trades if t["acct"] == acct]
        if not ts:
            continue
        for t in ts:
            if t["action"] == "BUY":
                lines.append("%s BUY %s @%s" % (disp, t["symbol"], _fmt_px(t["price"])))
            elif t["action"] == "SELL":
                pnl = (" pnl=%+.2f%%" % t["pnl_pct"]) if t.get("pnl_pct") is not None else ""
                lines.append("%s SELL %s @%s%s" % (disp, t["symbol"], _fmt_px(t["price"]), pnl))
            else:
                lines.append("%s REBAL %s @%s" % (disp, t["symbol"], _fmt_px(t["price"])))
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

    # ---- HV3 观察账户：独立算 HYPE 信号（不碰现有三账户/信号段）----
    hv3_extras = {}
    if "HV3" in need:
        try:
            hype_state = await _hype_read_state(conn)
            hype_rows = _hype_fetch_klines(expected)
            if hype_rows is None or len(hype_rows) < HYPE_MIN_KLINES:
                print("[PAPER] HV3 HYPE 数据不足(src=%s n=%d <%d根) → 该币跳过, 照跑其他4币"
                      % (HYPE_LAST["source"], HYPE_LAST["n"], HYPE_MIN_KLINES))
                hype_p1 = {"position": "out", "entry_price": None,
                           "entry_date": None, "exit_price": None}
                hype_open = float("nan")
            else:
                sig = _hype_compute_signal(hype_rows)
                action = _hype_decide(hype_state["position"], sig) if sig else "none"
                if action == "BUY":
                    hype_p1 = {"position": "in", "entry_price": sig["close"],
                               "entry_date": sig["date"], "exit_price": None}
                elif action == "SELL":
                    hype_p1 = {"position": "out", "entry_price": None,
                               "entry_date": None, "exit_price": sig["close"]}
                else:
                    hype_p1 = {"position": hype_state["position"],
                               "entry_price": hype_state.get("entry_price"),
                               "entry_date": hype_state.get("entry_date"),
                               "exit_price": hype_state.get("exit_price")}
                # HYPE 成交价来自同一数据源（OKX/HL 日线的 open），
                # 不能走 fetch_open：Binance 无 HYPEUSDT，会返回 nan 导致买不进。
                hype_open = _hype_open_for(expected, hype_rows)
                _sigd = sig if sig else {}
                print("[PAPER] HV3 HYPE src=%s n=%d asof=%s close=%s hh20=%s ll10=%s "
                      "vol_ratio=%s action=%s open=%s"
                      % (HYPE_LAST["source"], len(hype_rows), expected,
                         _sigd.get("close"), _sigd.get("hh20"), _sigd.get("ll10"),
                         (_sigd.get("vol_ratio") if _sigd.get("vol_ratio") is not None else "-"),
                         action if sig else "none", hype_open))
            await _hype_write_state(conn, expected, hype_p1)
            opens[HYPE_SYMBOL] = hype_open
            p1_hv3 = dict(p1)
            p1_hv3[HYPE_SYMBOL] = hype_p1
            hv3_extras["HV3"] = (HV3_SYMBOLS, p1_hv3)
        except Exception as e:
            print("[PAPER] HV3 HYPE 状态读写异常 → 该币跳过: %s" % e)
            p1_hv3 = dict(p1)
            p1_hv3[HYPE_SYMBOL] = {"position": "out", "entry_price": None,
                                   "entry_date": None, "exit_price": None}
            opens[HYPE_SYMBOL] = float("nan")
            hv3_extras["HV3"] = (HV3_SYMBOLS, p1_hv3)

    all_trades = []
    for acct in ACCT_ORDER:
        if acct not in need:
            continue
        obj = await paper_load(conn, acct)
        syms, p1d = (hv3_extras[acct] if acct in hv3_extras else (PAPER_SYMBOLS, p1))
        trades, nav, cr = obj.round_step(opens, p1d, expected, symbols=syms)
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
