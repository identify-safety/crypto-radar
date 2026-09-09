# -*- coding: utf-8 -*-
"""P1 断日检测 + 顺序补跑自愈（任务 A，2026-09-10；派活方：蓬蒿1号）

背景（审计风险 D1/D2）
    p1_section 的 last_date 门控隐含假设"每天 48 轮至少成功一轮"。若某天全轮失败
    （Actions 故障 / vision 断 / 10min 超时），恢复后代码直接跳到最新已收盘日线，
    断档那天的 BUY/SELL 被永久跳过；paper_section 同理漏记一天。模拟盘价值在连续
    忠实跟踪，断档即永久脱轨 —— 故方案定为【检测 + 顺序补跑自愈】，不是只告警。

核心思路：以 last_date 为锚点（写入原子、锚点可信，无需递归回溯），
         从 last_date+1 顺序补跑到 expected，等价于"递归直到正常"但一次定位。

6 条硬性约束的落点
    1. 只补 last_date 之后，绝不回滚重算已确认历史
       → 循环区间严格为 (anchor, expected]；记账用 strict_prev=True 只补 last_date
         的下一天，中间绝不跳日。
    2. 并发隔离：补跑期间置锁，正常轮见锁跳过
       → p1_repair_lock 表（CAS 获取 + 陈旧自动过期）；radar_scan.p1_section 与
         paper_accounting.paper_section 开头均调用 repair_lock_held() 让路。
    3. 信号与记账联动，复用幂等主键，可重入
       → 逐日交错：补 d 日信号 → 补 d 日记账（与正常轮 amain 的次序一致，因为
         记账要用 d 日的 p1_state 写 pending、用 d 日开盘价执行 d-1 留下的 pending）。
         幂等主键：p1_log(date,symbol) / paper_nav(acct,date)，重入不重复写。
    4. 补推 QQ 并标注"补发"
       → 补跑期间不单推（push=False），汇总后一次性推，header 为 [P1 信号补发]。
         推送失败落到 radar_seen(push_ok=false)，由既有 p1_retry_pending 续补发。
    5. vision endTime 先实测
       → 2026-09-10 实测：data-api.binance.vision 完整支持 endTime，三个随机历史日
         末根 openTime 严格 == asof，五币均返回满窗。故补跑按 asof 拉历史 K 线。
    6. 补跑失败留队次日重试，禁止死循环
       → 单日拉数失败即 break（不再推进后续日），释放锁；下轮重新检测，锚点自然前移。
         缺口 > MAX_REPAIR_DAYS → 不补、只告警（防几十天历史被静默重写）。

⚠️ 已知限制（如实标注，不掩盖）
    - 混合断档（各币 last_date 不一致）时，中间日的记账无法忠实回放：某币已推进到
      更新日期，其 p1_state 是"未来"状态，倒推会用错。此时本模块【只补信号】，
      记账如实记为 noncontiguous 并告警，绝不编造成交。
    - HV3/HYPE：历史 K 线来自 OKX/Hyperliquid 全量拉取后按 asof 过滤，若源端窗口
      不足 60 根会走既有"数据不足"兜底（HYPE 视为 out），与其他四币口径不同。
"""

import sys
import json
import datetime

# ---- 可调常量 ----
MAX_REPAIR_DAYS = 30      # 单次补跑最大天数；超过 → 告警人工介入，不自动补（约束 6）
LOCK_STALE_MIN = 120      # 锁陈旧阈值(分钟)：持有者崩溃未释放 → 自动过期（约束 2/6）
WIN_EXTRA_BARS = 85       # 预拉窗口 = 断档天数 + 本值（需 ≥ 20 根均量窗 + 20 日高窗 + 余量）

DDL_LOCK = """CREATE TABLE IF NOT EXISTS p1_repair_lock(
  id          INT PRIMARY KEY,
  held        BOOLEAN NOT NULL DEFAULT false,
  owner       TEXT,
  anchor      TEXT,
  target      TEXT,
  started_at  TIMESTAMPTZ,
  updated_at  TIMESTAMPTZ DEFAULT now())"""

DDL_LOG = """CREATE TABLE IF NOT EXISTS p1_repair_log(
  id          BIGSERIAL PRIMARY KEY,
  detected_at TIMESTAMPTZ DEFAULT now(),
  anchor      TEXT,
  target      TEXT,
  days        INT,
  sig_days    INT,
  acct_days   INT,
  status      TEXT,
  detail      TEXT)"""


# ===================== 日期工具 =====================
def _d(s):
    return datetime.date.fromisoformat(s)


def _diff(a, b):
    """b - a，自然日差。"""
    return (_d(b) - _d(a)).days


def _range_after(anchor, target):
    """(anchor, target] 的日期列表（升序）。"""
    out = []
    cur = _d(anchor) + datetime.timedelta(days=1)
    end = _d(target)
    while cur <= end:
        out.append(cur.isoformat())
        cur += datetime.timedelta(days=1)
    return out


# ===================== 锁（约束 2 / 6） =====================
async def repair_ensure_tables(conn):
    await conn.execute(DDL_LOCK)
    await conn.execute(DDL_LOG)
    await conn.execute(
        "INSERT INTO p1_repair_lock(id, held) VALUES(1,false) "
        "ON CONFLICT (id) DO NOTHING")


async def repair_lock_held(conn):
    """正常轮调用：补跑是否正在进行中。

    锁表缺失 / 查询异常 → 返回 False（fail-open，绝不因补跑器故障阻塞正常轮）。
    陈旧锁（持有者崩溃未释放）→ 视为已释放。
    """
    try:
        await repair_ensure_tables(conn)
        r = await conn.fetchrow(
            "SELECT held, updated_at FROM p1_repair_lock WHERE id=1")
        if not r or not r["held"]:
            return False
        if r["updated_at"]:
            age = (datetime.datetime.now(datetime.timezone.utc)
                   - r["updated_at"])
            if age > datetime.timedelta(minutes=LOCK_STALE_MIN):
                print("[P1-REPAIR] 发现陈旧锁(age=%s > %dmin) → 视为已释放"
                      % (age, LOCK_STALE_MIN), file=sys.stderr)
                return False
        return True
    except Exception as e:
        print("[P1-REPAIR] lock query failed (fail-open): %s" % e, file=sys.stderr)
        return False


async def _lock_acquire(conn, owner, anchor, target):
    """CAS 获取补跑锁。成功 True；已被他人持有（且未陈旧）→ False。"""
    res = await conn.execute(
        "UPDATE p1_repair_lock SET held=true, owner=$1, anchor=$2, target=$3, "
        "started_at=now(), updated_at=now() WHERE id=1 AND "
        "(held=false OR updated_at < now() - interval '%d minutes')"
        % LOCK_STALE_MIN, owner, anchor, target)
    n = str(res).split()[-1] if res else "0"
    return n == "1"


async def _lock_beat(conn):
    """心跳：多日补跑期间刷新 updated_at，避免被误判为陈旧锁。"""
    try:
        await conn.execute(
            "UPDATE p1_repair_lock SET updated_at=now() WHERE id=1")
    except Exception:
        pass


async def _lock_release(conn):
    try:
        await conn.execute(
            "UPDATE p1_repair_lock SET held=false, owner=NULL, updated_at=now() "
            "WHERE id=1")
    except Exception as e:
        print("[P1-REPAIR] lock release failed: %s" % e, file=sys.stderr)


async def _log(conn, anchor, target, days, sig_days, acct_days, status, detail):
    try:
        await conn.execute(
            "INSERT INTO p1_repair_log(anchor, target, days, sig_days, acct_days, "
            "status, detail) VALUES($1,$2,$3,$4,$5,$6,$7)",
            anchor, target, days, sig_days, acct_days, status,
            (detail or "")[:4000])
    except Exception as e:
        print("[P1-REPAIR] repair_log write failed: %s" % e, file=sys.stderr)


# ===================== 信号补跑（单日单币） =====================
async def _repair_apply_signal(conn, sym, st, sig):
    """把 d 日的信号应用到状态机并落库。

    ⚠️ 本函数是对 radar_scan.p1_section 主循环中【状态转移 + 落库】段落的复刻。
       之所以复刻而不是抽取公用函数，是因为任务书硬性要求"不碰 p1_* 信号层"。
       分叉风险由自测兜底：SELFTEST 会校验"同一输入下补跑路径与正常路径生成的
       p1_state 行 / p1_log 行逐字段一致"。两处任一方改动而另一方未同步 → 自测红。
    返回 (action, fired_item_or_None)。
    """
    from radar_scan import p1_decide, p1_save_state, p1_log_row, p1_fmt_px

    action = p1_decide(st["position"], sig)
    note, pnl, days = "", None, None

    if action == "BUY":
        st.update({"position": "in", "entry_date": sig["date"],
                   "entry_price": sig["close"], "exit_date": None,
                   "exit_price": None})
        note = "entry %s @ %.2f" % (sig["date"], sig["close"])
    elif action == "SELL":
        st.update({"position": "out", "exit_date": sig["date"],
                   "exit_price": sig["close"]})
        if st.get("entry_date") and st.get("entry_price"):
            pnl = (sig["close"] / st["entry_price"] - 1.0) * 100.0
            try:
                days = _diff(st["entry_date"], sig["date"])
            except Exception:
                days = None
            note = "exit %s @ %s pnl=%+.2f%% hold=%s" % (
                sig["date"], p1_fmt_px(sig["close"]), pnl, days)
        else:
            note = "exit %s @ %s (no entry record)" % (
                sig["date"], p1_fmt_px(sig["close"]))
        st.update({"entry_date": None, "entry_price": None})

    st["last_date"] = sig["date"]
    async with conn.transaction():     # 状态与日志同成功同失败（与 p1_section 一致）
        await p1_save_state(conn, sym, st)
        await p1_log_row(conn, {
            "date": sig["date"], "symbol": sym, "close": sig["close"],
            "hh20": sig["hh20"], "ll10": sig["ll10"],
            "vol_ratio": sig["vol_ratio"], "signal": action,
            "position": st["position"], "note": note})

    print("[P1-REPAIR] %s %s close=%.2f hh20=%.2f ll10=%.2f vr=%.2f "
          "buy=%s sell=%s action=%s pos=%s"
          % (sym, sig["date"], sig["close"], sig["hh20"], sig["ll10"],
             sig["vol_ratio"], sig["buy"], sig["sell"], action, st["position"]))

    if action in ("BUY", "SELL"):
        return action, {"symbol": sym, "action": action, "date": sig["date"],
                        "close": sig["close"], "vol_ratio": sig["vol_ratio"],
                        "note": note, "pnl": pnl, "days": days}
    return action, None


def _slice_window(rows, d, need=80):
    """从升序 rows 中切出"截至 d 日、恰好 need 根"的窗口（d 必须在 rows 内）。

    返回 None 表示 d 不在 rows 中（数据缺失/停牌）。
    """
    if not rows:
        return None
    idx = None
    for i, r in enumerate(rows):
        if r["date"] == d:
            idx = i
            break
    if idx is None:
        return None
    return rows[max(0, idx - need + 1): idx + 1]


# ===================== 主入口 =====================
async def p1_repair_section(conn):
    """断日检测 + 顺序补跑。由 radar_scan.amain 在 p1_section 之前调用。

    返回 dict（供自测与日志）：
      {"status": "ok"|"partial"|"skipped"|"gap-too-big"|"no-gap"|"error", ...}
    """
    if conn is None:
        print("[P1-REPAIR] skipped: no DATABASE_URL")
        return {"status": "skipped", "reason": "no-db"}

    # 延迟 import：避免与 radar_scan / paper_accounting 的顶层循环依赖
    from radar_scan import (P1_SYMBOLS, p1_expected_date, p1_day_end_ms,
                            p1_fetch_klines, p1_compute_signal, p1_load_state,
                            p1_ensure_tables, p1_push_text, qq_send_openid,
                            db_insert)
    from paper_accounting import _paper_run_day

    await repair_ensure_tables(conn)
    await p1_ensure_tables(conn)

    # (0) 已有补跑在跑 → 让路（约束 2）
    if await repair_lock_held(conn):
        print("[P1-REPAIR] 上一次补跑仍在进行 → 本轮跳过")
        return {"status": "skipped", "reason": "lock-held"}

    expected = p1_expected_date()
    st_map = await p1_load_state(conn)
    lasts = {s: (st_map.get(s) or {}).get("last_date") for s in P1_SYMBOLS}

    # (1) 冷启动：存在 last_date 为空的币 → 由正常轮完成首轮，补跑器不介入
    cold = [s for s in P1_SYMBOLS if not lasts.get(s)]
    if cold:
        print("[P1-REPAIR] 冷启动（%s 无 last_date）→ 补跑器不介入，交正常轮"
              % ",".join(cold))
        return {"status": "skipped", "reason": "cold-start"}

    anchor = min(lasts.values())
    gap = _diff(anchor, expected)

    # (2) 无缺口 → 零动作（正常态每轮只多 2 条轻查询，零拉数）
    if gap <= 0:
        print("[P1-REPAIR] 无缺口 (last=%s expected=%s) → 零动作" % (anchor, expected))
        return {"status": "no-gap", "gap": 0}

    # (3) 缺口过大 → 不自动补，只告警（约束 6：禁止静默重写几十天历史）
    if gap > MAX_REPAIR_DAYS:
        msg = ("缺口 %d 天 > 上限 %d (anchor=%s expected=%s) → 不自动补跑，"
               "需人工确认。各币 last_date=%s"
               % (gap, MAX_REPAIR_DAYS, anchor, expected,
                  json.dumps(lasts, ensure_ascii=False)))
        print("[P1-REPAIR] " + msg, file=sys.stderr)
        try:
            dup = await conn.fetchval(
                "SELECT COUNT(*) FROM p1_repair_log WHERE status='gap-too-big' "
                "AND anchor=$1 AND detected_at > now() - interval '1 day'", anchor)
        except Exception:
            dup = 0
        if not dup:
            try:
                qq_send_openid("[P1 断日告警]\n" + msg)
            except Exception as e:
                print("[P1-REPAIR] 告警推送失败: %s" % e, file=sys.stderr)
        await _log(conn, anchor, expected, gap, 0, 0, "gap-too-big", msg)
        return {"status": "gap-too-big", "gap": gap, "anchor": anchor}

    days = _range_after(anchor, expected)
    print("[P1-REPAIR] 检出缺口 anchor=%s expected=%s 需补 %d 天: %s"
          % (anchor, expected, len(days), ",".join(days)))
    print("[P1-REPAIR] 各币 last_date=%s" % json.dumps(lasts, ensure_ascii=False))

    # (4) 抢锁（约束 2）
    owner = "repair-%s" % datetime.datetime.now(
        datetime.timezone.utc).strftime("%Y%m%dT%H%M%S")
    if not await _lock_acquire(conn, owner, anchor, expected):
        print("[P1-REPAIR] 抢锁失败（并发补跑）→ 本轮跳过")
        return {"status": "skipped", "reason": "lock-contend"}

    fired_all, sig_done, acct_done = [], [], []
    paper_days = []          # [(asof, trades)] 补跑期间产生的记账成交
    detail, status = [], "ok"
    acct_note_added = False
    try:
        # (5) 预拉窗口：每币 1 次请求覆盖整段断档（endTime=expected，无前视）
        win = {}
        for s in P1_SYMBOLS:
            try:
                win[s] = p1_fetch_klines(s, asof=expected,
                                         limit=WIN_EXTRA_BARS + gap)
            except Exception as e:
                win[s] = None
                print("[P1-REPAIR] %s 预拉窗口失败: %s" % (s, e), file=sys.stderr)

        # (6) 逐日顺序补跑：先信号 → 后记账（约束 3，与正常轮次序一致）
        for d in days:
            failed = []
            for s in P1_SYMBOLS:
                if (st_map.get(s, {}).get("last_date") or "") >= d:
                    continue          # 该币已推进到 d 或更后（幂等 / 未断档）
                rows = win.get(s)
                window = _slice_window(rows, d) if rows else None
                if not window:
                    failed.append(s)
                    continue
                sig = p1_compute_signal(window)
                if sig is None or sig["date"] != d:
                    failed.append(s)
                    continue
                st = dict(st_map.get(s) or {"position": "out",
                                            "entry_date": None,
                                            "entry_price": None,
                                            "last_date": None})
                try:
                    _, item = await _repair_apply_signal(conn, s, st, sig)
                except Exception as e:
                    print("[P1-REPAIR] %s %s 信号落库失败: %s" % (s, d, e),
                          file=sys.stderr)
                    failed.append(s)
                    continue
                if item:
                    fired_all.append(item)
                st_map[s] = st

            if failed:
                # 约束 6：拉数未恢复 → 停止，留队下轮重试（锚点已前移，不会死循环）
                msg = "%s 信号补跑失败(%s) → 停止，留队次日重试" % (d, ",".join(failed))
                print("[P1-REPAIR] " + msg, file=sys.stderr)
                detail.append(msg)
                status = "partial"
                break
            sig_done.append(d)

            # 记账：strict_prev=True 只补 last_date 的下一天（约束 1 红线）
            try:
                res = await _paper_run_day(conn, d, qq_send=None,
                                           end_ms=p1_day_end_ms(d),
                                           push=False, strict_prev=True)
            except Exception as e:
                res = None
                print("[P1-REPAIR] %s 记账异常: %s" % (d, e), file=sys.stderr)
            if res and res.get("ok"):
                if res.get("accts"):
                    acct_done.append(d)
                if res.get("trades"):
                    paper_days.append((d, res["trades"]))
            else:
                why = (res or {}).get("skipped") or "None"
                if not acct_note_added:      # 只记一次，避免逐日刷屏
                    detail.append("记账不可忠实回放(%s)，自 %s 起留队：%s"
                                  % (d, d, why))
                    acct_note_added = True
                status = "partial"
            await _lock_beat(conn)
    finally:
        await _lock_release(conn)

    print("[P1-REPAIR] 补跑完成 status=%s 信号 %d/%d 天 记账 %d 天 触发信号 %d 条"
          % (status, len(sig_done), len(days), len(acct_done), len(fired_all)))

    # (7) 补推 QQ（约束 4）
    if fired_all:
        text = p1_push_text(fired_all, header="[P1 信号补发 %s]" % (
            datetime.datetime.now(datetime.timezone.utc)
            + datetime.timedelta(hours=8)).strftime("%m-%d %H:%M"))
        try:
            ok, e = qq_send_openid(text)
        except Exception as e:
            ok, e = False, str(e)
        print("[P1-REPAIR] 补推 %d 条 ok=%s %s" % (len(fired_all), ok, e))
        if not ok:
            # 落 radar_seen(push_ok=false) → 既有 p1_retry_pending 会续补发
            for it in fired_all:
                try:
                    await db_insert(
                        conn, "P1:%s:%s:%s" % (it["symbol"], it["date"], it["action"]),
                        "P1", it["symbol"], it, False)
                except Exception as ex:
                    print("[P1-REPAIR] mark pending failed: %s" % ex, file=sys.stderr)

    # (7b) 记账成交补推：补跑期间的成交同样要让人看见（任务 B 的补发语义）
    if paper_days:
        from paper_accounting import _push_text, _paper_mark_pending
        allp = []
        for _d0, _ts in paper_days:
            allp.extend(_ts)
        ptxt = _push_text(allp)
        if ptxt:
            asofs = sorted({d0 for d0, _ in paper_days})
            span = ("~".join(asofs) if len(asofs) <= 3
                    else "%s...%s" % (asofs[0], asofs[-1]))
            ptxt = "[P1模拟成交补发 %s]\n" % span + ptxt.split("\n", 1)[-1]
            try:
                ok, e = qq_send_openid(ptxt)
            except Exception as e:
                ok, e = False, str(e)
            print("[P1-REPAIR] 成交补推 %d 笔 ok=%s %s" % (len(allp), ok, e))
            if not ok:      # 失败 → 落 paper_push_pending，后续 paper_retry_pending 续补发
                for d0, ts in paper_days:
                    try:
                        await _paper_mark_pending(conn, d0, ts)
                    except Exception as ex:
                        print("[P1-REPAIR] mark paper pending failed: %s" % ex,
                              file=sys.stderr)

    await _log(conn, anchor, expected, len(days), len(sig_done), len(acct_done),
               status, " | ".join(detail) if detail else "")
    return {"status": status, "anchor": anchor, "expected": expected,
            "days": len(days), "sig_days": len(sig_done),
            "acct_days": len(acct_done), "fired": len(fired_all),
            "paper_trades": sum(len(t) for _, t in paper_days),
            "detail": detail}
