#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
crypto 机会雷达 OKX/Actions 版 (2026-09-07, 蓬蒿1号 编写, 接口按 2号 crypto-radar.yml + schema.sql)
运行环境: GitHub Actions ubuntu-latest, Python 3.13, 已装 asyncpg
数据源: OKX 公开 API (美国 runner 可直连; 币安 451 不可用)

信号:
  NEW_LISTING       instruments 里 listTime 距今 <6h 的新上线 SWAP
  UPCOMING_LISTING  state=preopen 且 listTime 在未来 48h 内 (提前预警, 比币安强)
  FUNDING_ANOMALY   fundingRate 年化 >25%, 来源两类:
                    (a) 固定 WATCHLIST 25 币(沿用旧逻辑)
                    (b) 上线 <72h 的 live linear SWAP 新合约(覆盖 48h+ 拉盘期;
                    补 WATCHLIST 覆盖不到的新币; instType=SWAP & ctType=linear
                    排除币本位逆合约 BTC-USD-SWAP 类)
                    结算周期实测(funding-rate-history 最近两期; 不足两期退回 funding-rate
                    排期时间差), 不再写死 8h
状态: Neon Postgres (radar_seen 去重+重试, radar_runs 每轮日志), 无本地文件
推送: QQ bot REST API (api.sgroup.qq.com)
环境变量: DATABASE_URL / QQ_APP_ID / QQ_TOKEN(clientSecret) / FORCE_PUSH(可选)
退出码: 0=成功(含无信号), 1=严重失败

---
2026-09-07 2号 review 后修复 3 个致命 bug (原版本机无 DATABASE_URL, 数据库分支未被执行故未暴露):
  1. 原 main() 里 asyncio.run() 被调用 5 次 —— asyncpg 连接绑定首个 event loop, 该 loop
     结束后连接即失效, 第二次调用必然 "Event loop is closed". 改为单一 async main + 一次 asyncio.run
  2. sig_text() 中 payload 误取为 sig 本身, 导致 payload['fr'] / payload['lt'] KeyError.
     改为 sig["payload"]
  3. asyncpg.connect() 未传 ssl="require" —— asyncpg 不解析 DSN 里的 sslmode 参数,
     Neon 强制 SSL 会直接连不上. 改为剥离 sslmode + 显式 ssl="require"
  另: db_insert 的 jsonb 参数加显式 ::jsonb 转换, 避免 asyncpg 类型推断抖动

2026-09-08 2号 按 1号 排查修复 FUNDING_ANOMALY 两处缺陷:
  1. 原 scan_funding 只扫固定 25 币 WATCHLIST, 新合约永不触发 → 新增 scan_new_contracts_funding:
     每轮从 instruments 筛 state=live 且上线 <24h 的 SWAP, 逐个查费率, 年化>阈值即触发 FUNDING_ANOMALY
     (复用现有 sig_key/去重/QQ 推送链路, type 仍用 FUNDING_ANOMALY)
  2. 原 FUND_INTERVAL_H 写死 8h → 改为 get_interval_h 实测结算周期:
     funding-rate-history 最近两期之差优先; 不足两期(新合约常如此)退回 funding-rate 的
     nextFundingTime-fundingTime 之差; 都没有才回退 8h。CNPY 4h 合约应能按 ~199% 年化出信号。
  文案: NEW_LISTING/UPCOMING_LISTING 的"费率/溢价异动我会再推"收敛为"费率异动"(溢价盯梢本期未做)
"""
import asyncio, json, os, re, sys, time, datetime, urllib.request, urllib.error

OKX = "https://www.okx.com"
QQ_TOKEN_URL = "https://bots.qq.com/app/getAppAccessToken"
QQ_API = "https://api.sgroup.qq.com"
WATCHLIST = [  # 主流+高流动性 SWAP, funding-rate 单查, 25 个约 10s
    "BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP", "XRP-USDT-SWAP",
    "DOGE-USDT-SWAP", "BNB-USDT-SWAP", "ADA-USDT-SWAP", "AVAX-USDT-SWAP",
    "LINK-USDT-SWAP", "TRX-USDT-SWAP", "DOT-USDT-SWAP", "LTC-USDT-SWAP",
    "SUI-USDT-SWAP", "NEAR-USDT-SWAP", "ARB-USDT-SWAP", "OP-USDT-SWAP",
    "TIA-USDT-SWAP", "APT-USDT-SWAP", "INJ-USDT-SWAP", "SEI-USDT-SWAP",
    "WIF-USDT-SWAP", "PEPE-USDT-SWAP", "BONK-USDT-SWAP", "TON-USDT-SWAP",
    "UNI-USDT-SWAP",
]
FUND_YEARLY_MIN = 25.0   # 年化 % 阈值 (2号实测 15% 在 OKX 上太敏感会刷屏, 2026-09-07 调 25)
FUND_INTERVAL_FALLBACK_H = 8.0  # 实测不到结算周期时的回退值(OKX 主流 8h 近似)
NEW_LISTING_WIN = 6 * 3600 * 1000       # 上线 6h 内
NEW_FUNDING_WIN = 72 * 3600 * 1000      # 新合约费率盯梢: 上线 72h 内(live linear SWAP,
                                           # 覆盖 48h+ 拉盘期; 613c441 24h 窗口太短会漏第 2-3 天)
UPCOMING_WIN = 48 * 3600 * 1000          # preopen 未来 48h
_HDRS = {"User-Agent": "Mozilla/5.0 radar-actions/1.0"}


def get_json(url, timeout=20):
    req = urllib.request.Request(url, headers=_HDRS)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode())


def post_json(url, payload, timeout=20, headers=None):
    h = {"Content-Type": "application/json", "User-Agent": "Mozilla/5.0 radar-actions/1.0"}
    if headers:
        h.update(headers)
    req = urllib.request.Request(url, data=json.dumps(payload).encode(), headers=h, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode())


# ---------- Neon ----------
async def db_fetch(conn, sig_key):
    return await conn.fetchrow("SELECT sig_key, push_ok FROM radar_seen WHERE sig_key=$1", sig_key)


async def db_insert(conn, sig_key, stype, inst, payload, ok):
    await conn.execute(
        "INSERT INTO radar_seen(sig_key, signal_type, inst_id, payload, pushed_at, push_ok) "
        "VALUES($1,$2,$3,$4::jsonb,now(),$5) "
        "ON CONFLICT (sig_key) DO UPDATE SET push_ok=$5, pushed_at=now()",
        sig_key, stype, inst, json.dumps(payload, ensure_ascii=False), ok)


async def db_run(conn, ok, ninst, nscan, nsig, err):
    await conn.execute(
        "INSERT INTO radar_runs(finished_at, ok, instruments, scanned, signals, error) "
        "VALUES(now(), $1, $2, $3, $4, $5)", ok, ninst, nscan, nsig, err)


async def db_connect():
    url = os.environ.get("DATABASE_URL")
    if not url:
        return None
    import asyncpg
    # asyncpg 不解析 DSN 里的 sslmode, 必须剥掉并显式传 ssl —— 否则连不上 Neon
    url = re.sub(r"[?&]sslmode=[^&]*", "", url)
    return await asyncpg.connect(url, ssl="require", timeout=20)


# ---------- QQ 推送 ----------
_qq_token_cache = {"tok": None, "exp": 0}


def qq_token():
    if _qq_token_cache["tok"] and time.time() < _qq_token_cache["exp"] - 60:
        return _qq_token_cache["tok"], None
    try:
        d = post_json(QQ_TOKEN_URL, {"appId": os.environ["QQ_APP_ID"],
                                     "clientSecret": os.environ["QQ_TOKEN"]})
        tok = d.get("access_token")
        if not tok:
            return None, f"token 响应异常: {str(d)[:120]}"
        _qq_token_cache["tok"] = tok
        _qq_token_cache["exp"] = time.time() + int(d.get("expires_in", 7200))
        return tok, None
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"


def qq_send_openid(text):
    openid = os.environ.get("QQ_OPENID", "10C1ECD726D7C5251E216B49A4665C61")
    tok, err = qq_token()
    if err:
        return False, err
    try:
        post_json(f"{QQ_API}/v2/users/{openid}/messages", {"content": text, "msg_type": 0},
                  headers={"Authorization": f"QQBot {tok}"})
        return True, ""
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "replace")[:150]
        if e.code in (401, 403):
            _qq_token_cache["tok"] = None
        return False, f"HTTP {e.code}: {body}"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def now_bj():
    return datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8))).strftime("%m-%d %H:%M")


# ---------- 检测 ----------
def scan_listings(insts):
    """instruments → (NEW_LISTING | UPCOMING_LISTING) 候选"""
    now_ms = time.time() * 1000
    out = []
    for it in insts:
        if it.get("instType") != "SWAP":
            continue
        lt = int(it.get("listTime") or 0)
        st = it.get("state", "")
        inst = it.get("instId", "")
        if not inst or not lt:
            continue
        if 0 < now_ms - lt < NEW_LISTING_WIN:
            out.append(("NEW_LISTING", inst, lt, st))
        elif st == "preopen" and 0 < lt - now_ms < UPCOMING_WIN:
            out.append(("UPCOMING_LISTING", inst, lt, st))
    return out


def get_interval_h(inst, fr_row=None):
    """实测结算周期(小时). 优先级:
    ① fr_row(优先复用) → 用 fundingTime/nextFundingTime 或 fundingTime/prevFundingTime
       排期差直接算周期(零额外请求; scan_funding_for 已查 funding-rate, 直接复用 row);
    ② fr_row 缺失或排期信息不全 → 现拉 funding-rate 取排期;
    ③ 还不行才发 funding-rate-history 取最近两期(精度最高但多 1 个请求).
    老坑: 613c441 版本对每个币无条件发 history, 即便 fr_row 已有排期也发, 注释
    "传 row 避免重复请求"名不副实 —— 本版把 fr_row 路径提到首位, history 退到兜底."""
    # ①/② fr_row 优先 (复用 / 现拉 funding-rate 取排期)
    row = fr_row
    if row is None:
        try:
            d = get_json(f"{OKX}/api/v5/public/funding-rate?instId={inst}")
            row = d["data"][0] if d.get("code") == "0" and d.get("data") else None
        except Exception:
            row = None
    if row:
        try:
            ft = int(row.get("fundingTime") or 0)
            nft = int(row.get("nextFundingTime") or 0)
            pft = int(row.get("prevFundingTime") or 0)
            if nft > ft > 0:
                return (nft - ft) / 3600.0 / 1000.0
            if ft > pft > 0:
                return (ft - pft) / 3600.0 / 1000.0
        except Exception:
            pass
    # ③ row 拿不到 / 排期信息不全 → 发 history 取最近两期差(精度兜底)
    try:
        d = get_json(f"{OKX}/api/v5/public/funding-rate-history?instId={inst}&limit=2")
        if d.get("code") == "0" and len(d.get("data") or []) >= 2:
            t0 = int(d["data"][0].get("fundingTime") or 0)
            t1 = int(d["data"][1].get("fundingTime") or 0)
            if t0 and t1 and abs(t0 - t1) > 0:
                return abs(t0 - t1) / 3600.0 / 1000.0
    except Exception:
        pass
    return FUND_INTERVAL_FALLBACK_H


def scan_funding_for(inst_list):
    """给定 inst_ids → FUNDING_ANOMALY 候选(年化>FUND_YEARLY_MIN). 每合约实测结算周期."""
    out = []
    for inst in inst_list:
        try:
            d = get_json(f"{OKX}/api/v5/public/funding-rate?instId={inst}")
            if d.get("code") != "0" or not d.get("data"):
                continue
            row = d["data"][0]
            fr = float(row.get("fundingRate") or 0)
            iv = get_interval_h(inst, row)   # 传 row 避免重复请求
            ann = fr * (24.0 / iv) * 365.0 * 100.0
            if ann > FUND_YEARLY_MIN:
                out.append((inst, fr, ann, row.get("fundingTime"), iv))
        except Exception:
            continue
    return out


def scan_funding(insts):
    """WATCHLIST 25 币 → FUNDING_ANOMALY 候选(沿用旧逻辑, 现在用实测周期)"""
    inst_ids = {i.get("instId") for i in insts}
    watch = [w for w in WATCHLIST if w in inst_ids]
    return scan_funding_for(watch)


def scan_new_contracts_funding(insts):
    """上线 <72h 的 live linear SWAP → FUNDING_ANOMALY 候选(覆盖 48h+ 拉盘期,
    补 WATCHLIST 覆盖不到的新币; ctType=linear 排除币本位逆合约)"""
    now_ms = time.time() * 1000
    watch = set(WATCHLIST)
    cand = []
    for it in insts:
        if it.get("instType") != "SWAP":
            continue
        if it.get("ctType") != "linear":  # 排除币本位逆合约 (BTC-USD-SWAP 等), 文案面向 USDT 本位
            continue
        if it.get("state") != "live":
            continue
        lt = int(it.get("listTime") or 0)
        inst = it.get("instId", "")
        if not inst or not lt:
            continue
        if 0 < now_ms - lt < NEW_FUNDING_WIN:
            if inst in watch:
                continue  # WATCHLIST 已在扫, 避免重复请求/信号
            cand.append(inst)
    if not cand:
        return []
    return scan_funding_for(cand)


def sig_text(sig):
    typ, inst, payload = sig["type"], sig["inst"], sig["payload"]
    if typ == "NEW_LISTING":
        return (f"[机会雷达 A级 {now_bj()}]\nOKX 新合约上线: {inst}\n"
                f"state={payload.get('state')} | 面值 {payload.get('ctVal')} | 杠杆 {payload.get('lever')}\n"
                f"上线初期流动性浅, 先观察 1-2h 再决定(费率异动我会再推)")
    if typ == "UPCOMING_LISTING":
        return (f"[机会雷达 B级 {now_bj()}]\nOKX 预告新合约: {inst}\n"
                f"上线时间 {datetime.datetime.fromtimestamp(payload['lt']/1000, datetime.timezone(datetime.timedelta(hours=8))).strftime('%m-%d %H:%M')} (北京时间)\n"
                f"上线后我盯首日费率")
    if typ == "FUNDING_ANOMALY":
        iv = payload.get("interval_h")
        iv_s = f"{iv:.0f}h" if iv else f"{FUND_INTERVAL_FALLBACK_H:.0f}h"
        return (f"[机会雷达 A级 {now_bj()}]\nOKX 费率异动, 吃费率窗口:\n"
                f"{inst} {payload['fr']*100:+.4f}%/期 ({iv_s}结算) → 年化约 {payload['ann']:.0f}%\n"
                f"操作: OKX 现货买入 + 合约同量开空(方向中性). 想进回复 1号 算配比")
    return f"[机会雷达 {now_bj()}]\n{typ}: {inst}"


# ===================== P1 日级信号（2026-09-08 并入云端雷达） =====================
# 口径与本机 p1_track.py / quant_pool_backtest.py 的 P1 严格一致：
#   买: close > max(close[i-20:i]) 且 volume >= 1.5 * mean(volume[i-20:i])
#   卖: close < min(close[i-10:i])
#   窗口全部"前 N 根、不含当日"（等价 rolling(N).shift(1)），无前视。
#   买/卖互斥（hh20 >= ll10）→ "out 只查买、in 只查卖" 与回测 if/elif 完全等价。
# 门控：p1_state.last_date == UTC当日-1 → 跳过该币且不拉 K 线（否则 48 轮/天重复拉均线）。
# 数据源：data-api.binance.vision 现货日线直连（币安主站对美国 IP 451，vision 实测 200）。
P1_SYMBOLS = ["BTCUSDT", "ETHUSDT", "XRPUSDT", "SOLUSDT", "DOGEUSDT"]
P1_VISION = "https://data-api.binance.vision"
P1_HH, P1_VM, P1_LL = 20, 1.5, 10
P1_KLINE_LIMIT = 80
P1_DAY_MS = 86400000

# 注：任务书写 REAL，此处用 DOUBLE PRECISION（8 字节）。REAL 是 float4，只有 ~7 位有效
# 数字，79112.01 这类价格会存成 79112.008 影响 1号 拉表格核对——这是唯一一处刻意偏离。
P1_DDL_STATE = """CREATE TABLE IF NOT EXISTS p1_state(
  symbol       TEXT PRIMARY KEY,
  position     TEXT NOT NULL DEFAULT 'out',
  entry_date   TEXT,
  entry_price  DOUBLE PRECISION,
  exit_date    TEXT,
  exit_price   DOUBLE PRECISION,
  last_date    TEXT,
  updated_at   TIMESTAMPTZ DEFAULT now())"""

P1_DDL_LOG = """CREATE TABLE IF NOT EXISTS p1_log(
  date      TEXT,
  symbol    TEXT,
  close     DOUBLE PRECISION,
  hh20      DOUBLE PRECISION,
  ll10      DOUBLE PRECISION,
  vol_ratio DOUBLE PRECISION,
  signal    TEXT,
  position  TEXT,
  note      TEXT,
  PRIMARY KEY(date, symbol))"""


def p1_expected_date():
    """最新已收盘日线日期 = UTC 当日 - 1 天。

    UTC 日线在 UTC 00:00（北京 08:00）收盘；当前 UTC 日期 D 的那根尚未走完，
    故最新完整日线 = D-1。与 p1_track.py 过滤未收盘那根后的结果一致。
    """
    d = datetime.datetime.now(datetime.timezone.utc).date()
    return (d - datetime.timedelta(days=1)).isoformat()


def p1_get_json(url, timeout=20):
    """vision 直连：显式 ProxyHandler({}) 禁代理，避免 runner 环境代理干扰。"""
    op = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    req = urllib.request.Request(url, headers={"User-Agent": "p1-tracking/1.0"})
    with op.open(req, timeout=timeout) as r:
        return json.loads(r.read().decode())


def p1_fetch_klines(symbol):
    """现货日线，只保留已收盘那根；返回升序 rows(date/close/volume)。"""
    url = "%s/api/v3/klines?symbol=%s&interval=1d&limit=%d" % (
        P1_VISION, symbol, P1_KLINE_LIMIT)
    raw = p1_get_json(url)
    now_ms = time.time() * 1000
    rows = []
    for k in raw:
        ot = int(k[0])
        if ot + P1_DAY_MS > now_ms:          # 该日线尚未走完
            continue
        rows.append({
            "date": datetime.datetime.fromtimestamp(
                ot / 1000, datetime.timezone.utc).strftime("%Y-%m-%d"),
            "close": float(k[4]),
            "volume": float(k[5]),
        })
    rows.sort(key=lambda r: r["date"])
    return rows


def p1_compute_signal(rows):
    """与 p1_track.py 的 compute_signal 同口径：窗口严格不含当日。"""
    if len(rows) < 21:
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


def p1_decide(position, sig):
    """状态机：out 只查买、in 只查卖。返回 BUY / SELL / none。"""
    if position == "out":
        return "BUY" if sig["buy"] else "none"
    return "SELL" if sig["sell"] else "none"


async def p1_ensure_tables(conn):
    """建表幂等（IF NOT EXISTS）+ 5 币种子行（ON CONFLICT DO NOTHING）。每轮 3 条查询。"""
    await conn.execute(P1_DDL_STATE)
    await conn.execute(P1_DDL_LOG)
    await conn.execute(
        "INSERT INTO p1_state(symbol, position) SELECT unnest($1::text[]), 'out' "
        "ON CONFLICT (symbol) DO NOTHING", P1_SYMBOLS)


async def p1_load_state(conn):
    rows = await conn.fetch(
        "SELECT symbol, position, entry_date, entry_price, last_date "
        "FROM p1_state WHERE symbol = ANY($1::text[])", P1_SYMBOLS)
    return {r["symbol"]: {"position": r["position"] or "out",
                          "entry_date": r["entry_date"],
                          "entry_price": r["entry_price"],
                          "last_date": r["last_date"]} for r in rows}


async def p1_save_state(conn, sym, st):
    await conn.execute(
        "INSERT INTO p1_state(symbol, position, entry_date, entry_price, "
        "exit_date, exit_price, last_date, updated_at) "
        "VALUES($1,$2,$3,$4,$5,$6,$7,now()) "
        "ON CONFLICT (symbol) DO UPDATE SET position=$2, entry_date=$3, entry_price=$4, "
        "exit_date=$5, exit_price=$6, last_date=$7, updated_at=now()",
        sym, st.get("position", "out"), st.get("entry_date"), st.get("entry_price"),
        st.get("exit_date"), st.get("exit_price"), st.get("last_date"))


async def p1_log_row(conn, rec):
    """主键 (date, symbol) 去重：同一触发日重复写入不会追加。"""
    await conn.execute(
        "INSERT INTO p1_log(date, symbol, close, hh20, ll10, vol_ratio, "
        "signal, position, note) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) "
        "ON CONFLICT (date, symbol) DO NOTHING",
        rec["date"], rec["symbol"], rec["close"], rec["hh20"], rec["ll10"],
        rec["vol_ratio"], rec["signal"], rec["position"], rec["note"])


def p1_fmt_px(v):
    """价格自适应精度：BTC 79112.01 / XRP 1.4012 / DOGE 0.093521 都要可读。"""
    a = abs(v)
    if a >= 1000:
        return "%.2f" % v
    if a >= 1:
        return "%.4f" % v
    return "%.6f" % v


def p1_push_text(items, header=None):
    """多币信号合并成一条文本（避免同日 5 条刷屏）。"""
    lines = [header or ("[P1 信号 %s]" % now_bj())]
    for it in items:
        if it.get("action") == "BUY":
            lines.append("%s BUY  @%s  (%s)  突破20日高+量%.2fx"
                         % (it["symbol"], p1_fmt_px(it["close"]), it["date"],
                            it.get("vol_ratio", 0)))
        else:
            extra = ""
            if it.get("pnl") is not None:
                extra = "  pnl=%+.2f%% hold=%sd" % (it["pnl"], it.get("days"))
            lines.append("%s SELL @%s  (%s)%s  破10日低"
                         % (it["symbol"], p1_fmt_px(it["close"]), it["date"], extra))
    return "\n".join(lines)


async def p1_retry_pending(conn):
    """补发上一轮推送失败的 P1 信号（radar_seen 里 push_ok=false，近 3 天）。

    为什么需要：P1 段有 last_date 门控，推送失败的那一轮之后 last_date 已推进，
    下一轮会被门控跳过 —— 不补发就会永久丢一条买卖信号。这里复用现有 radar_seen
    做去重/重试（与费率信号同一套机制），无失败行时只是 1 条轻查询，不拉 K 线。
    """
    try:
        rows = await conn.fetch(
            "SELECT sig_key, payload FROM radar_seen WHERE signal_type='P1' "
            "AND push_ok IS NOT TRUE AND pushed_at > now() - interval '3 days'")
    except Exception as e:
        print("[P1] retry query failed: %s" % e, file=sys.stderr)
        return
    items = []
    for r in rows:
        p = r["payload"]
        if isinstance(p, str):
            try:
                p = json.loads(p)
            except Exception:
                continue
        if isinstance(p, dict):
            items.append(p)
    if not items:
        return
    text = p1_push_text(items, header="[P1 信号补发 %s]" % now_bj())
    ok, e = qq_send_openid(text)
    print("[P1] retry push %d signal(s) ok=%s %s" % (len(items), ok, e))
    if ok:
        for r in rows:
            try:
                await conn.execute(
                    "UPDATE radar_seen SET push_ok=true, pushed_at=now() WHERE sig_key=$1",
                    r["sig_key"])
            except Exception:
                pass


async def p1_section(conn):
    """P1 段主流程。整体由 amain 的 try/except 包住，异常不得影响费率扫描。"""
    if conn is None:
        print("[P1] skipped: no DATABASE_URL (状态权威在 Neon，无库无法门控)")
        return
    expected = p1_expected_date()
    await p1_ensure_tables(conn)
    st_map = await p1_load_state(conn)
    todo = [s for s in P1_SYMBOLS
            if (st_map.get(s) or {}).get("last_date") != expected]
    if not todo:
        print("[P1] gate: 全部最新 (last_date=%s) → 零拉数" % expected)
    else:
        print("[P1] gate: 触发 %d/%d expected=%s → %s"
              % (len(todo), len(P1_SYMBOLS), expected, ",".join(todo)))

    fired = []
    for sym in todo:
        try:
            rows = p1_fetch_klines(sym)
        except Exception as e:
            print("[P1] %s fetch failed: %s" % (sym, e), file=sys.stderr)
            continue
        sig = p1_compute_signal(rows)
        if sig is None:
            print("[P1] %s bars 不足 (%d)" % (sym, len(rows)), file=sys.stderr)
            continue
        if sig["date"] != expected:
            # 数据滞后：按真实数据日处理并写 last_date=实际日，下一轮会继续重试拉取
            print("[P1] %s 数据滞后: got %s expected %s (按实际数据日处理)"
                  % (sym, sig["date"], expected), file=sys.stderr)

        st = dict(st_map.get(sym) or {"position": "out", "entry_date": None,
                                      "entry_price": None, "last_date": None})
        action = p1_decide(st["position"], sig)
        note, pnl, days = "", None, None

        if action == "BUY":
            st.update({"position": "in", "entry_date": sig["date"],
                       "entry_price": sig["close"], "exit_date": None, "exit_price": None})
            note = "entry %s @ %.2f" % (sig["date"], sig["close"])
        elif action == "SELL":
            st.update({"position": "out", "exit_date": sig["date"],
                       "exit_price": sig["close"]})
            if st.get("entry_date") and st.get("entry_price"):
                pnl = (sig["close"] / st["entry_price"] - 1.0) * 100.0
                try:
                    days = (datetime.date.fromisoformat(sig["date"])
                            - datetime.date.fromisoformat(st["entry_date"])).days
                except Exception:
                    days = None
                note = "exit %s @ %s pnl=%+.2f%% hold=%s" % (
                    sig["date"], p1_fmt_px(sig["close"]), pnl, days)
            else:
                note = "exit %s @ %s (no entry record)" % (
                    sig["date"], p1_fmt_px(sig["close"]))
            st.update({"entry_date": None, "entry_price": None})

        st["last_date"] = sig["date"]
        st_map[sym] = st
        try:
            async with conn.transaction():     # 状态与日志同成功同失败
                await p1_save_state(conn, sym, st)
                await p1_log_row(conn, {
                    "date": sig["date"], "symbol": sym, "close": sig["close"],
                    "hh20": sig["hh20"], "ll10": sig["ll10"],
                    "vol_ratio": sig["vol_ratio"], "signal": action,
                    "position": st["position"], "note": note})
        except Exception as e:
            print("[P1] %s db write failed: %s" % (sym, e), file=sys.stderr)
            continue

        print("[P1] %s %s close=%.2f hh20=%.2f ll10=%.2f vr=%.2f "
              "buy=%s sell=%s action=%s pos=%s"
              % (sym, sig["date"], sig["close"], sig["hh20"], sig["ll10"],
                 sig["vol_ratio"], sig["buy"], sig["sell"], action, st["position"]))

        if action in ("BUY", "SELL"):
            fired.append({"symbol": sym, "action": action, "date": sig["date"],
                          "close": sig["close"], "vol_ratio": sig["vol_ratio"],
                          "note": note, "pnl": pnl, "days": days})

    if fired:
        text = p1_push_text(fired)
        ok, e = qq_send_openid(text)
        print("[P1] push %d signal(s) ok=%s %s" % (len(fired), ok, e))
        if not ok:
            # 记入 radar_seen（push_ok=false）→ 下轮 p1_retry_pending 补发
            for it in fired:
                try:
                    await db_insert(conn, "P1:%s:%s:%s" % (it["symbol"], it["date"], it["action"]),
                                    "P1", it["symbol"], it, False)
                except Exception as ex:
                    print("[P1] mark pending failed: %s" % ex, file=sys.stderr)
    await p1_retry_pending(conn)


async def amain():
    err = None
    conn = None
    nscan = 0  # 默认值; 无 DB 时 print 不 NameError (613c441 在 if conn: 内才赋值, 本地无 DB 会 exit 1)
    try:
        conn = await db_connect()
        # 0. P1 日级信号（放在费率扫描之前）
        #    - 独立 try/except：P1 任何异常只记日志，不得影响费率主功能
        #    - 放在前面：反过来若费率段抛错，P1 信号也已经推出去了，不会漏单
        try:
            await p1_section(conn)
        except Exception as e:
            print(f"[P1] section failed: {type(e).__name__}: {e}", file=sys.stderr)
        # 1. instruments
        d = get_json(f"{OKX}/api/v5/public/instruments?instType=SWAP")
        if d.get("code") != "0":
            raise RuntimeError(f"instruments code={d.get('code')}")
        insts = d["data"]
        # 2. 新合约候选
        cands = scan_listings(insts)
        # 3. 费率候选: WATCHLIST 25 币 + 上线<72h 新合约(覆盖 48h+ 拉盘期)
        fund = scan_funding(insts)
        new_fund = scan_new_contracts_funding(insts)
        fund += new_fund
        nscan = len(WATCHLIST) + len(new_fund)  # 实际扫的币数(与是否落 DB 无关)
        signals = []
        for typ, inst, lt, st in cands:
            signals.append({"type": typ, "inst": inst, "sig_key": f"{typ}:{inst}:{lt}",
                            "payload": {"instId": inst, "listTime": lt, "state": st, "lt": lt}})
        for inst, fr, ann, ft, iv in fund:
            ftk = ft if ft is not None else ""
            signals.append({"type": "FUNDING_ANOMALY", "inst": inst,
                            "sig_key": f"FUNDING_ANOMALY:{inst}:{ftk}",
                            "payload": {"instId": inst, "fr": fr, "ann": ann,
                                        "fundingTime": ft, "interval_h": iv}})

        n_sent = 0
        force = os.environ.get("FORCE_PUSH") == "true"
        for sig in signals:
            key = sig["sig_key"]
            row = await db_fetch(conn, key) if conn else None
            if row and row["push_ok"] is True and not force:
                continue
            text = ("[机会雷达 测试 FORCE_PUSH]\n" + sig_text(sig)) if force else sig_text(sig)
            ok, e = qq_send_openid(text)
            if conn:
                await db_insert(conn, key, sig["type"], sig["inst"], sig["payload"], ok)
            if ok:
                n_sent += 1
            elif e:
                print(f"push fail {key}: {e}")
        if conn:
            await db_run(conn, True, len(insts), nscan, len(signals), None)
        print(f"OK scanned={len(insts)} fund_scan={nscan} signals={len(signals)} sent={n_sent}")
    except Exception as e:
        err = f"{type(e).__name__}: {e}"
        print(f"ERROR {err}", file=sys.stderr)
        if conn:
            try:
                await db_run(conn, False, 0, 0, 0, err[:500])
            except Exception:
                pass
        sys.exit(1)
    finally:
        if conn:
            try:
                await conn.close()
            except Exception:
                pass


# ---------- P1 自测（--p1-selftest）：离线可跑，不写真实库、不发真实推送 ----------
class _FakeTxn:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


class _FakeConn:
    """最小 asyncpg 假连接：记录 SQL，不落真实库。"""

    def __init__(self, state_rows=None, seen_rows=None):
        self.state_rows = state_rows or []
        self.seen_rows = seen_rows or []
        self.executed = []

    def transaction(self):
        return _FakeTxn()

    async def execute(self, sql, *args):
        self.executed.append((" ".join(sql.split()), args))
        return "OK"

    async def fetch(self, sql, *args):
        if "p1_state" in sql:
            return self.state_rows
        if "radar_seen" in sql:
            return self.seen_rows
        return []

    async def fetchrow(self, sql, *args):
        return None


def p1_selftest():
    ok = True
    exp = p1_expected_date()
    real_fetch = globals()["p1_fetch_klines"]
    real_push = globals()["qq_send_openid"]
    real_decide = globals()["p1_decide"]
    calls, pushed = [], []

    def counting_fetch(sym):
        calls.append(sym)
        return real_fetch(sym)

    def fake_push(text):
        pushed.append(text)
        return True, ""

    globals()["qq_send_openid"] = fake_push
    print("=" * 64)
    print("P1 SELFTEST (radar_scan.py)   expected_date=%s" % exp)
    print("=" * 64)

    def run_section(state_rows):
        calls.clear()
        globals()["p1_fetch_klines"] = counting_fetch
        try:
            asyncio.run(p1_section(_FakeConn(state_rows=state_rows)))
        finally:
            globals()["p1_fetch_klines"] = real_fetch

    try:
        # ---- (0) 数据源 + 窗口不含当日 ----
        print("\n[0] 数据源 / 窗口语义")
        try:
            rows = real_fetch("BTCUSDT")
            sig = p1_compute_signal(rows)
            exp_hh = max(r["close"] for r in rows[-21:-1])
            exp_ll = min(r["close"] for r in rows[-11:-1])
            exp_vm = sum(r["volume"] for r in rows[-21:-1]) / 20.0
            win_ok = (sig is not None and abs(sig["hh20"] - exp_hh) < 1e-9
                      and abs(sig["ll10"] - exp_ll) < 1e-9
                      and abs(sig["vma20"] - exp_vm) < 1e-9)
            print("    bars=%d range=%s~%s close=%.2f hh20=%.2f ll10=%.2f vr=%.2f"
                  % (len(rows), rows[0]["date"], rows[-1]["date"], sig["close"],
                     sig["hh20"], sig["ll10"], sig["vol_ratio"]))
            print("    vision 直连 + shift(1) 窗口(不含当日) → %s" % ("PASS" if win_ok else "FAIL"))
        except Exception as e:
            win_ok = False
            print("    FAIL fetch: %s" % e)
        ok = ok and win_ok

        # ---- (1) 门控 ----
        print("\n[1] last_date 门控")
        upto = [{"symbol": s, "position": "out", "entry_date": None,
                 "entry_price": None, "last_date": exp} for s in P1_SYMBOLS]
        run_section(upto)
        gate_skip = (len(calls) == 0)
        print("    1a 跳过: last_date=%s → 拉数 %d 次  %s"
              % (exp, len(calls), "PASS" if gate_skip else "FAIL"))
        prev_day = (datetime.date.fromisoformat(exp) - datetime.timedelta(days=1)).isoformat()
        stale = [{"symbol": s, "position": "out", "entry_date": None,
                  "entry_price": None, "last_date": prev_day} for s in P1_SYMBOLS]
        run_section(stale)
        gate_fire = (len(calls) == len(P1_SYMBOLS))
        print("    1b 触发: last_date=%s → 拉数 %d/%d 次  %s"
              % (prev_day, len(calls), len(P1_SYMBOLS), "PASS" if gate_fire else "FAIL"))
        ok = ok and gate_skip and gate_fire

        # ---- (2) 建表幂等 + p1_log 主键去重 ----
        print("\n[2] SQL 幂等 / 去重")
        c2 = _FakeConn()
        asyncio.run(p1_ensure_tables(c2))
        asyncio.run(p1_ensure_tables(c2))          # 连跑两次不得报错
        ddl = [s for s, _ in c2.executed if "CREATE TABLE" in s]
        ddl_ok = len(ddl) == 4 and all("IF NOT EXISTS" in s for s in ddl)
        seed = [s for s, _ in c2.executed if "INSERT INTO p1_state" in s]
        seed_ok = len(seed) == 2 and all("ON CONFLICT (symbol) DO NOTHING" in s for s in seed)
        c3 = _FakeConn()
        asyncio.run(p1_log_row(c3, {"date": exp, "symbol": "BTCUSDT", "close": 1.0,
                                    "hh20": 1.0, "ll10": 1.0, "vol_ratio": 1.0,
                                    "signal": "none", "position": "out", "note": ""}))
        dedup_ok = any("ON CONFLICT (date, symbol) DO NOTHING" in s for s, _ in c3.executed)
        print("    DDL×%d 全含 IF NOT EXISTS → %s ; 种子行×%d 幂等 → %s ; p1_log 主键去重 → %s"
              % (len(ddl), "PASS" if ddl_ok else "FAIL", len(seed),
                 "PASS" if seed_ok else "FAIL", "PASS" if dedup_ok else "FAIL"))
        ok = ok and ddl_ok and seed_ok and dedup_ok

        # ---- (3) 与 p1_track.py 逐币口径对照 ----
        print("\n[3] 信号口径 vs p1_track.py")
        path = os.environ.get("P1_TRACK_PATH", "")
        if path and os.path.exists(path):
            import importlib.util
            spec = importlib.util.spec_from_file_location("p1_track", path)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            bad = []
            for sym in P1_SYMBOLS:
                a = mod.compute_signal(mod.fetch_klines(sym)[0])
                b = p1_compute_signal(real_fetch(sym))
                if a is None or b is None:
                    bad.append((sym, "None"))
                    continue
                for k in ("date", "close", "hh20", "ll10", "vma20", "vol_ratio", "buy", "sell"):
                    va, vb = a[k], b[k]
                    if isinstance(va, float) and isinstance(vb, float):
                        nan_both = (va != va) and (vb != vb)      # NaN == NaN
                        if abs(va - vb) > 1e-9 and not nan_both:
                            bad.append((sym, k, va, vb))
                    elif va != vb:
                        bad.append((sym, k, va, vb))
            print("    5 币全字段比对 → %s" % ("PASS 完全一致" if not bad else "FAIL %s" % (bad,)))
            ok = ok and (not bad)
        else:
            print("    SKIP（设环境变量 P1_TRACK_PATH 指向 p1_track.py 可启用；部署前必跑）")

        # ---- (4) 推送合并 / 无切换零推送 ----
        print("\n[4] 推送合并 + 无切换零推送")
        pushed.clear()
        globals()["p1_decide"] = lambda pos, sig: "BUY"     # 强制 5 币同日触发
        try:
            run_section(stale)
        finally:
            globals()["p1_decide"] = real_decide
        merge_ok = (len(pushed) == 1) and all(s in pushed[0] for s in P1_SYMBOLS)
        print("    4a 合并: 5 币同日触发 → 推送 %d 条（含全部 symbol=%s）  %s"
              % (len(pushed), all(s in pushed[0] for s in P1_SYMBOLS) if pushed else False,
                 "PASS" if merge_ok else "FAIL"))
        if pushed:
            for ln in pushed[0].split("\n"):
                print("    | " + ln)

        pushed.clear()
        globals()["p1_decide"] = lambda pos, sig: "none"    # 强制全部无信号
        try:
            run_section(stale)
        finally:
            globals()["p1_decide"] = real_decide
        zero_ok = (len(pushed) == 0)
        print("    4b 静默: 5 币均 none → 推送 %d 条  %s"
              % (len(pushed), "PASS" if zero_ok else "FAIL"))
        ok = ok and merge_ok and zero_ok
    finally:
        globals()["qq_send_openid"] = real_push
        globals()["p1_fetch_klines"] = real_fetch
        globals()["p1_decide"] = real_decide

    print("\n" + "=" * 64)
    print("P1 SELFTEST %s" % ("ALL PASS" if ok else "FAILED"))
    print("=" * 64)
    return ok


def main():
    if "--p1-selftest" in sys.argv:
        sys.exit(0 if p1_selftest() else 1)
    asyncio.run(amain())


if __name__ == "__main__":
    main()
