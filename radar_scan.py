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
                    (b) 上线 <24h 的 live SWAP 新合约(首日费率盯梢, 补 WATCHLIST 覆盖不到的新币)
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
NEW_FUNDING_WIN = 24 * 3600 * 1000      # 新合约首日费率盯梢: 上线 24h 内(live SWAP)
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
    ① funding-rate-history 最近两期 fundingTime 之差(请求要求, 最贴近真实);
    ② 退回 funding-rate 的 nextFundingTime-fundingTime(新合约上线不久常只有 1 期历史,
       但排期时间已给出, 比回退 8h 准); 都取不到才回退 FUND_INTERVAL_FALLBACK_H=8h.
    fr_row 可传入已查到的 funding-rate 响应, 避免重复请求."""
    # ① funding-rate-history 最近两期
    try:
        d = get_json(f"{OKX}/api/v5/public/funding-rate-history?instId={inst}&limit=2")
        if d.get("code") == "0" and len(d.get("data") or []) >= 2:
            t0 = int(d["data"][0].get("fundingTime") or 0)
            t1 = int(d["data"][1].get("fundingTime") or 0)
            if t0 and t1 and abs(t0 - t1) > 0:
                return abs(t0 - t1) / 3600.0 / 1000.0
    except Exception:
        pass
    # ② funding-rate 排期时间(优先用已取的 fr_row, 否则现拉)
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
    """上线 <24h 的 live SWAP → FUNDING_ANOMALY 候选(首日费率盯梢, 补 WATCHLIST 覆盖不到的新币)"""
    now_ms = time.time() * 1000
    watch = set(WATCHLIST)
    cand = []
    for it in insts:
        if it.get("instType") != "SWAP":
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


async def amain():
    err = None
    conn = None
    try:
        conn = await db_connect()
        # 1. instruments
        d = get_json(f"{OKX}/api/v5/public/instruments?instType=SWAP")
        if d.get("code") != "0":
            raise RuntimeError(f"instruments code={d.get('code')}")
        insts = d["data"]
        # 2. 新合约候选
        cands = scan_listings(insts)
        # 3. 费率候选: WATCHLIST 25 币 + 上线<24h 新合约(首日费率盯梢)
        fund = scan_funding(insts)
        new_fund = scan_new_contracts_funding(insts)
        fund += new_fund
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
            nscan = len(WATCHLIST) + len(new_fund)
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


def main():
    asyncio.run(amain())


if __name__ == "__main__":
    main()
