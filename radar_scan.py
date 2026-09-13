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

2026-09-11 2号 新增 C 方案: FUNDING_ANOMALY 推送带「对冲可行性标注」(纯只读, 无任何交易动作):
  FUNDING_ANOMALY 的旧文案固定写"OKX 现货买入 + 合约同量开空", 但 OKX 大量合约没有现货
  (实测 463 个 SWAP 基础币中 246 个无 OKX 现货), 其中 174 个是股票/指数永续(instCategory=3),
  根本不存在"买币现货"一说 —— 旧文案会误导。现在每条信号按下列顺序判定对冲路径并写进文案:
    ① instCategory=3/4 (股票·指数/商品·外汇永续) → [不可对冲] 无现货概念, 仅观察
       (若 HL 有同标的合约, 追加"仅可跨所 perp 对冲"及费率差)
    ② OKX 有该币现货 → [可对冲] OKX 现货买 + 合约空(方向中性)
    ③ HL 有同标的 perp → [可对冲(跨所)] OKX 空收 + HL 多付, 费率差 = OKX年化 - HL年化(非到手收益)
    ④ HL 有该币现货 → [待确认] HL 现货深度待查(实测 HL 现货多为长尾 @N 对, 滑点大)
    ⑤ 确证两侧都无覆盖 → [不可对冲] 无对冲腿, 仅观察
       (注意: 仅当 HL 合约清单 与 现货清单 均已成功探测才标 NONE;
        HL 合约清单不可用 → UNKNOWN(见 v3 修法 A); HL 现货清单不可用 → 同款 UNKNOWN(见 v4 P3))
  对冲上下文每轮只取 3 个请求(OKX SPOT instruments + HL metaAndAssetCtxs + HL spotMeta),
  三个数据源各自独立 try/except 并分别置 okx_spot_ok / hl_meta_ok / hl_spot_ok 标志
  (2026-09-11 v2 修复 1号/异源复核 F-1~F-4/F-7):
    - OKX 现货清单失败/限流/空 → 整段降级 UNKNOWN(绝不把 OKX_SPOT 标的翻成 HL_PERP/NONE)
    - HL spotMeta 半挂不连坐 hl_perp(MNT 等真·HL_PERP 仍标得出)
    - 任一数据源失败都记 err(不再静默吞成空集)
    - universe 与 asset_ctxs 长度不一致显式报错(不静默截断)
  (2026-09-11 v3 修法 A, 1号 复验):
    - HL 合约清单不可用(hl_meta_ok=False) → 标 UNKNOWN(不得写"无对冲腿/无同名合约"确定性结论);
      runner→HL 可达性至今未实测, 这是保护核心功能(标出跨所路径)不被静默误标的兜底
  (2026-09-11 v4 同款修复, 1号 复验 P3, 不阻塞):
    - 缺陷换到现货维度: spotMeta 挂(hl_spot_ok=False)时, 本会对 H 等真·HL_SPOT 标的输出确定性
      "无对冲腿/无同名合约" → 与 detail 自相矛盾(同款缺陷, 只是维度从合约换成现货).
      修法(同款模式): hl_spot_ok=False 时, 现货维度不得断言"无覆盖", 降级 UNKNOWN;
      仅当 HL 合约清单 与 现货清单 均已成功探测且确无覆盖, 才标稳态 NONE.
      影响面: 5 个 HL_SPOT 标的 × spotMeta 故障期(方向保守, 故不卡部署).
  绝不影响费率扫描主流程(与 P1 段同策)。
  依据: 同目录 HYPERLIQUID_HEDGE_FEASIBILITY_20260911.md
"""
import asyncio, json, os, re, sys, time, datetime, urllib.request, urllib.error

OKX = "https://www.okx.com"
HL_API = "https://api.hyperliquid.xyz/info"
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


# ===================== HEDGE 对冲路径标注（2026-09-11 新增，纯只读） =====================
# 目标：让每条 FUNDING_ANOMALY 自己说清楚"能不能对冲、走哪条路"，不再无脑写
# "OKX 现货买入 + 合约开空"。本段不产生任何交易/下单动作。
#
# 关键判别位（实测 2026-09-11，OKX instruments?instType=SWAP）:
#   instCategory=1 → 币类永续（含币本位逆合约 BTC-USD-SWAP）
#   instCategory=3 → 股票/指数永续（AAPL/TSLA/SPY ... 共 174 个）→ 无"买币现货"概念
#   instCategory=4 → 商品/外汇永续（XAU/BZ/CL ... 共 8 个）
# 旧逻辑不区分这三类，是"对冲不了"误报的主要来源。
HEDGE_CAT_CRYPTO = "1"
HEDGE_CAT_EQUITY = "3"
HEDGE_CAT_COMMODITY = "4"
HEDGE_TAG_LABEL = {
    "OKX_SPOT": "[可对冲] OKX 现货",
    "HL_PERP": "[可对冲(跨所)] HL 合约",
    "HL_SPOT": "[待确认] HL 现货",
    "NONE": "[不可对冲] 无对冲腿",
    "UNKNOWN": "[未知] 对冲路径待确认",
}


def hl_post(body, timeout=15):
    """Hyperliquid info API。本机 + 美国 runner 均实测可直连(2026-09-11 本机确认; runner 待实测)。"""
    req = urllib.request.Request(
        HL_API, data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "User-Agent": "Mozilla/5.0 radar-actions/1.0"},
        method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode())


def hedge_build_context():
    """每轮构建一个对冲上下文（3 个请求）。三个数据源各自独立 try/except，绝不抛错到主流程。

    三个独立标志分别记录可用性（2026-09-11 v2 修复 1号/异源复核 F-1/F-2/F-3/F-4/F-7）：
      okx_spot_ok : OKX 现货清单是否成功取到（code=="0" 且 data 有效且非空）
      hl_meta_ok  : HL metaAndAssetCtxs 是否成功（hl_perp 可用）
      hl_spot_ok  : HL spotMeta 是否成功（hl_spot 可用；半挂不影响 hl_perp）
      err         : 失败原因摘要（任一数据源失败都记，不再静默吞成空集）

    返回 dict:
      okx_spot: set   OKX live 现货基础币
      hl_perp : dict  HL 合约名 -> 年化费率 % (HL funding 字段是每小时, *24*365*100 转年化)
      hl_spot : set   HL 现货基础币名
    """
    ctx = {"okx_spot": set(), "hl_perp": {}, "hl_spot": set(),
           "okx_spot_ok": False, "hl_meta_ok": False, "hl_spot_ok": False, "err": ""}

    # 1) OKX 现货清单（判断有没有"同所现货腿"）
    #    F-1/F-3 修复：显式查 code=="0" 并覆盖 data:null/[]/缺字段/空；
    #    失败/限流 → okx_spot_ok=False 且 err 必须落日志（不再静默吞成空集把主流币翻成 HL_PERP）。
    try:
        d = get_json(f"{OKX}/api/v5/public/instruments?instType=SPOT")
        if not isinstance(d, dict) or d.get("code") != "0":
            raise RuntimeError("OKX_SPOT code=%s msg=%s"
                               % (d.get("code") if isinstance(d, dict) else "n/a",
                                  (d.get("msg") if isinstance(d, dict) else "")[:80]))
        rows = d.get("data")
        if not isinstance(rows, list):
            raise RuntimeError("OKX_SPOT data 非数组")
        if len(rows) == 0:
            raise RuntimeError("OKX_SPOT data 为空(疑似失败)")
        ctx["okx_spot"] = {str(i.get("instId", "")).split("-")[0]
                           for i in rows
                           if isinstance(i, dict) and i.get("state") == "live" and i.get("instId")}
        ctx["okx_spot_ok"] = True
    except Exception as e:
        ctx["okx_spot_ok"] = False
        ctx["err"] = "OKX_SPOT:%s:%s" % (type(e).__name__, e)

    # 2) Hyperliquid 合约（metaAndAssetCtxs）—— 独立 try/except
    #    F-2 修复：与 spotMeta 分开，meta 成功即 hl_perp 可用，不因 spotMeta 单点失败连坐。
    #    F-7 修复：universe 与 asset_ctxs 长度不一致时显式报错（不再静默截断）。
    try:
        ctxs = hl_post({"type": "metaAndAssetCtxs"})
        universe = (ctxs[0].get("universe") if (isinstance(ctxs, (list, tuple)) and len(ctxs) >= 2
                                                 and isinstance(ctxs[0], dict)) else None)
        asset_ctxs = ctxs[1] if (isinstance(ctxs, (list, tuple)) and len(ctxs) >= 2) else None
        if universe is None or asset_ctxs is None:
            raise ValueError("HL meta shape mismatch")
        if len(universe) != len(asset_ctxs):
            raise ValueError("HL meta len mismatch: universe=%d asset_ctxs=%d"
                             % (len(universe), len(asset_ctxs)))
        for u, a in zip(universe, asset_ctxs):
            if not isinstance(u, dict) or u.get("isDelisted"):
                continue
            try:
                name = u.get("name")
                if not name:
                    continue
                fr = float(a.get("funding") or 0)        # HL 口径: 每小时
                ctx["hl_perp"][name] = fr * 24 * 365 * 100.0
            except Exception:
                continue
        ctx["hl_meta_ok"] = True
    except Exception as e:
        ctx["hl_meta_ok"] = False
        ctx["err"] = (ctx["err"] + " " if ctx["err"] else "") + "HL_META:%s:%s" % (type(e).__name__, e)

    # 3) Hyperliquid 现货（spotMeta）—— 独立 try/except，失败不影响 hl_perp
    try:
        sp = hl_post({"type": "spotMeta"})
        tokens = sp.get("tokens") or [] if isinstance(sp, dict) else []
        tokmap = {t["index"]: t["name"] for t in tokens if isinstance(t, dict) and "index" in t}
        for p in (sp.get("universe") or []):
            try:
                tk = p.get("tokens") or []
                if len(tk) >= 2:
                    ctx["hl_spot"].add(tokmap[int(tk[0])])
            except Exception:
                continue
        ctx["hl_spot_ok"] = True
    except Exception as e:
        ctx["hl_spot_ok"] = False
        ctx["err"] = (ctx["err"] + " " if ctx["err"] else "") + "HL_SPOT:%s:%s" % (type(e).__name__, e)

    return ctx


def hedge_eval(ctx, inst, inst_category, okx_ann):
    """判定单个合约的对冲路径 → (tag, detail)。纯计算，不联网。

    标签语义（v2 修复 F-1/F-2/F-4）：
      OKX_SPOT : OKX 确有余币现货 → 方向中性对冲（最稳）
      HL_PERP  : OKX 无现货、HL 有同标的合约 → 仅可跨所 perp 对冲（费率差，非到手收益）
      HL_SPOT  : OKX 无现货、HL 有现货（长尾 @N 对，深度待查）
      NONE     : 已确证两侧都无对冲腿（OKX 侧成功 且 HL 合约清单已成功探测且无覆盖）→ 稳态
      UNKNOWN  : 数据源不可用→无法给出确定性结论（标签行与 detail 必须同调）:
                 · OKX 现货清单失败（无法确认 OKX 侧）
                 · HL 合约清单失败(hl_meta_ok=False)（无法确认跨所 perp 路径, 不得写"无同名合约"）
                 · HL 现货清单失败(hl_spot_ok=False)（无法确认 HL 现货对冲路径, 不得写"无覆盖"）
    """
    base = inst.split("-")[0]
    cat = str(inst_category or HEDGE_CAT_CRYPTO)
    okx_spot_ok = ctx.get("okx_spot_ok", False)
    hl_meta_ok = ctx.get("hl_meta_ok", False)
    hl_spot_ok = ctx.get("hl_spot_ok", False)

    def _hl_perp_note(prefix, name_collision_risk=False):
        hl_ann = ctx["hl_perp"].get(base)     # F-5 防御：缺失 → UNKNOWN
        if hl_ann is None:
            return "UNKNOWN", f"{prefix}HL 费率缺失, 对冲路径未知, 先别进"
        net = okx_ann - hl_ann                  # 费率差 = OKX 空收 − HL 多付
        tail = ""
        if name_collision_risk:
            tail = ("; ⚠ 跨所同名标的未必同一 underlying(如 OKX PURR 是股票类、HL PURR 是 "
                    "memecoin), 人工确认是同一标的后再考虑")
        return ("HL_PERP",
                f"{prefix}HL 有同标的合约, 仅可跨所 perp 对冲(OKX 空收 + HL 多付); "
                f"HL 年化≈{hl_ann:+.0f}% → 费率差≈{net:+.0f}%"
                f"(未扣手续费/滑点/基差波动, 非到手收益){tail}")

    # cat=3/4：股票/指数/商品/外汇永续 → 无"买币现货"概念（F-4/V3：HL 不可用时降级但
    # 不升格 UNKNOWN，也不写"不存在现货对冲路径"）
    if cat in (HEDGE_CAT_EQUITY, HEDGE_CAT_COMMODITY):
        kind = "股票/指数永续" if cat == HEDGE_CAT_EQUITY else "商品/外汇永续"
        if hl_meta_ok and base in ctx["hl_perp"]:
            tag, detail = _hl_perp_note("", name_collision_risk=True)
            return tag, f"{kind}, 无币现货概念; " + detail
        # 修法 A (v3): HL 合约清单不可用 → 无法确认跨所路径, 不得写"无同名合约"假断言,
        # 降级 UNKNOWN（标签行与 detail 同调）
        if not hl_meta_ok:
            return "UNKNOWN", f"{kind}, 无币现货概念; HL 合约清单不可用, 跨所路径未知, 先别进"
        # v4 (P3 同款): HL 现货清单不可用 → 无法确认是否 HL 现货代币, 不得写"无同名合约",
        # 降级 UNKNOWN（标签行与 detail 同调）
        if not hl_spot_ok:
            return "UNKNOWN", f"{kind}, 无币现货概念; HL 现货清单不可用, 跨所路径未知, 先别进"
        return "NONE", f"{kind}, 无币现货概念; HL 无同名合约, 仅观察"

    # crypto（cat=1 或未知分类 → 按币类处理，F-8 future-proofing）
    # ① OKX 现货可用且命中 → 方向中性对冲（最稳）
    if okx_spot_ok and base in ctx["okx_spot"]:
        return "OKX_SPOT", "OKX 有该币现货, 可现货买入 + 合约同量开空(方向中性)"
    # ② OKX 现货清单自身失败 → 无法确认 OKX 侧，整段 UNKNOWN（F-1 核心修复：不把
    #    OKX_SPOT 标的翻成 HL_PERP/NONE，err 已落日志）
    if not okx_spot_ok:
        return "UNKNOWN", f"OKX 现货清单获取失败({ctx['err'] or 'unknown'}), 对冲路径未知, 先别进"
    # 到此：OKX 侧已确认无现货，看 HL
    # ③ HL perp（即使 spotMeta 半挂也不影响，F-2）
    if hl_meta_ok and base in ctx["hl_perp"]:
        return _hl_perp_note("OKX 无该币现货; ")
    # ④ HL spot（仅当现货清单可用时才能确证）
    if hl_spot_ok and base in ctx["hl_spot"]:
        return "HL_SPOT", ("OKX 无该币现货; HL 有现货但多为长尾 @N 对, 深度与滑点需人工确认后再进")
    # ⑤ HL perp / spot 均不可用或已探测无覆盖。
    #    关键(v3 修法 A): 若 HL 合约清单本身不可用(hl_meta_ok=False), 则"无同名合约"是假断言
    #    —— 此时不得给确定性的 NONE, 降级 UNKNOWN(标签行与 detail 同调);
    #    仅当 HL 合约清单已成功探测(确证无 perp 覆盖)时, 才标稳态 NONE。
    if not hl_meta_ok:
        return "UNKNOWN", "OKX 无该币现货; HL 合约清单不可用, 跨所路径未知, 先别进"
    # v4 (P3 同款): 现货清单不可用(hl_spot_ok=False) → 无法确认是否 HL 现货对冲腿,
    #    "HL 亦无覆盖"是假断言 —— 降级 UNKNOWN(标签行与 detail 同调);
    #    仅当 HL 现货清单也已成功探测(确证无 spot 覆盖)时, 才标稳态 NONE。
    if not hl_spot_ok:
        return "UNKNOWN", "OKX 无该币现货; HL 现货清单不可用, 现货对冲路径未探明, 先别进"
    return "NONE", "OKX 无该币现货, HL 合约与现货均无覆盖 → 无对冲腿, 仅观察(方向不中性, 不建议裸吃)"


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
        # 对冲路径标注（纯只读；旧文案无条件写"OKX 现货买入+合约开空"，对无现货腿的合约是误导）
        hed = payload.get("hedge") or {}
        tag = hed.get("tag") or "UNKNOWN"
        detail = hed.get("detail") or "对冲路径未判定"
        label = HEDGE_TAG_LABEL.get(tag, "[未知]")
        line = (f"[机会雷达 A级 {now_bj()}]\nOKX 费率异动, 吃费率窗口:\n"
                f"{inst} {payload['fr']*100:+.4f}%/期 ({iv_s}结算) → 年化约 {payload['ann']:.0f}%\n"
                f"对冲: {label}\n{detail}")
        if tag == "OKX_SPOT":
            line += "\n操作: OKX 现货买入 + 合约同量开空(方向中性). 想进回复 1号 算配比"
        elif tag in ("HL_PERP", "HL_SPOT"):
            line += "\n操作: 需跨所两腿, 基差与双边保证金自担; 想进先找 1号 算配比"
        else:
            line += "\n操作: 无可对冲腿, 建议只观察不下单"
        return line
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


def p1_day_end_ms(asof):
    """asof(ISO date) 当日 23:59:59.999 的 UTC 毫秒 —— 用作 klines endTime。

    Binance/vision 的 endTime 语义：返回 openTime <= endTime 的 K 线。
    取当日 23:59:59.999 → 最后一根恰好是 asof 当日那根（实测 2026-09-10 验证，
    三个随机历史日 BTCUSDT 末根 openTime 严格 == asof）。
    """
    d = datetime.datetime.strptime(asof, "%Y-%m-%d").replace(tzinfo=datetime.timezone.utc)
    return int(d.timestamp() * 1000) + P1_DAY_MS - 1


def p1_get_json(url, timeout=20):
    """vision 直连：显式 ProxyHandler({}) 禁代理，避免 runner 环境代理干扰。"""
    op = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    req = urllib.request.Request(url, headers={"User-Agent": "p1-tracking/1.0"})
    with op.open(req, timeout=timeout) as r:
        return json.loads(r.read().decode())


def p1_fetch_klines(symbol, asof=None, limit=None):
    """现货日线，只保留已收盘那根；返回升序 rows(date/close/volume)。

    asof=None（默认，正常轮）：与历史逐字等价 —— URL 不带 endTime，用 now_ms
        剔除未走完那根。
    asof='YYYY-MM-DD'（补跑轮）：URL 带 &endTime=<当日 23:59:59.999>，且截断
        基准改为该 ms 而非 now_ms → 严格只返回 <=asof 的日线，无前视。
        实测（2026-09-10）：vision 完整支持 endTime，三个随机历史日末根 openTime
        严格 == asof，五币均返回满 80 根。
    """
    url = "%s/api/v3/klines?symbol=%s&interval=1d&limit=%d" % (
        P1_VISION, symbol, limit or P1_KLINE_LIMIT)
    if asof:
        url += "&endTime=%d" % p1_day_end_ms(asof)
    raw = p1_get_json(url)
    now_ms = time.time() * 1000
    cutoff_ms = p1_day_end_ms(asof) if asof else now_ms
    rows = []
    for k in raw:
        ot = int(k[0])
        if asof:
            # 截断基准已是 asof 当日 23:59:59.999，asof 当日那根的 openTime
            # (=asof 00:00) 必须保留；只剔除 openTime 严格晚于 asof 的。
            # ⚠️ 不能用 `ot + P1_DAY_MS > cutoff`：那会把 asof 当日那根误杀
            #    (asof+1 00:00 > asof 23:59:59.999)，窗口少一根、信号整段错位。
            if ot > cutoff_ms:
                continue
        elif ot + P1_DAY_MS > cutoff_ms:      # 该日线尚未走完
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
    # 补跑锁（任务 A 约束 2）：补跑器正在顺序补跑时，正常 30min 轮必须让路，
    # 否则正常轮会直接去处理 expected 最新日，与补跑的逐日推进打架。
    # 锁不可用（表未建 / 查询异常）→ 不阻塞正常轮，fail-open。
    try:
        from p1_repair import repair_lock_held
        if await repair_lock_held(conn):
            print("[P1] gate: 补跑进行中(lock held) → 本轮让路跳过")
            return
    except Exception as _e:
        print("[P1] repair lock check skipped: %s" % _e, file=sys.stderr)
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
        # 0. P1 断日补跑（任务 A，2026-09-10 新增，独立隔离）
        #    - 放在 p1_section 之前：先补齐断档日，再跑正常轮 —— 否则正常轮会
        #      先把 last_date 推到 expected，补跑器就再也无法识别缺口（锚点被抹掉）。
        #    - 独立 try/except：补跑任何异常只记日志，正常轮照跑，费率主功能不受影响。
        #    - 无缺口时只是 2 条轻查询（锁 + p1_state），零拉数。
        try:
            from p1_repair import p1_repair_section
            await p1_repair_section(conn)
        except Exception as e:
            print(f"[P1-REPAIR] section failed: {type(e).__name__}: {e}", file=sys.stderr)
        # 0. P1 日级信号（放在费率扫描之前）
        #    - 独立 try/except：P1 任何异常只记日志，不得影响费率主功能
        #    - 放在前面：反过来若费率段抛错，P1 信号也已经推出去了，不会漏单
        try:
            await p1_section(conn)
        except Exception as e:
            print(f"[P1] section failed: {type(e).__name__}: {e}", file=sys.stderr)
        # 0.5 P1 三方案模拟实盘记账（2026-09-08 新增，独立隔离）
        #     - 仅在 P1 信号段之后调用，读取 p1_state 的当日变化驱动三账户纸面记账
        #     - 独立 try/except：记账任何异常只记日志，绝不影响信号段与费率主功能
        #     - 信号层(p1_*) 完全不动；本段逻辑全在 paper_accounting.py
        try:
            from paper_accounting import paper_section as _paper_section
            await _paper_section(conn)
        except Exception as e:
            print(f"[PAPER] section failed: {type(e).__name__}: {e}", file=sys.stderr)
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
        # 3.5 对冲上下文（C 方案，纯只读；失败只降级不影响主流程）
        try:
            hedge_ctx = hedge_build_context()
            print(f"[HEDGE] okx_spot={len(hedge_ctx['okx_spot'])} "
                  f"hl_perp={len(hedge_ctx['hl_perp'])} hl_spot={len(hedge_ctx['hl_spot'])} "
                  f"okx_spot_ok={hedge_ctx['okx_spot_ok']} "
                  f"hl_meta_ok={hedge_ctx['hl_meta_ok']} hl_spot_ok={hedge_ctx['hl_spot_ok']} "
                  f"err={hedge_ctx['err'] or '-'}")
        except Exception as e:
            hedge_ctx = {"okx_spot": set(), "hl_perp": {}, "hl_spot": set(),
                         "okx_spot_ok": False, "hl_meta_ok": False, "hl_spot_ok": False,
                         "err": f"CTX:{type(e).__name__}"}
            print(f"[HEDGE] context failed: {type(e).__name__}: {e}", file=sys.stderr)
        cat_map = {i.get("instId"): i.get("instCategory") for i in insts}
        signals = []
        for typ, inst, lt, st in cands:
            signals.append({"type": typ, "inst": inst, "sig_key": f"{typ}:{inst}:{lt}",
                            "payload": {"instId": inst, "listTime": lt, "state": st, "lt": lt}})
        for inst, fr, ann, ft, iv in fund:
            ftk = ft if ft is not None else ""
            # 对冲路径判定: 需要 instCategory 区分股票/商品永续(无现货概念) 与 币类
            try:
                tag, detail = hedge_eval(hedge_ctx, inst, cat_map.get(inst), ann)
            except Exception as e:
                tag, detail = "UNKNOWN", f"对冲判定异常({type(e).__name__}), 先别进"
            signals.append({"type": "FUNDING_ANOMALY", "inst": inst,
                            "sig_key": f"FUNDING_ANOMALY:{inst}:{ftk}",
                            "payload": {"instId": inst, "fr": fr, "ann": ann,
                                        "fundingTime": ft, "interval_h": iv,
                                        "instCategory": cat_map.get(inst),
                                        "hedge": {"tag": tag, "detail": detail}}})

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
