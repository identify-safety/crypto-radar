-- P1 三方案模拟实盘 · Neon 表结构（2026-09-08）
-- 用法: psql "$DATABASE_URL" -f schema_paper.sql
-- 幂等: 可重复执行；已有表/行不被覆盖。与 paper_accounting.paper_ensure_tables() 定义保持一致。
-- 注: 不动 p1_state / p1_log / radar_* 表。

-- 账户主表: 现金 + 最近记账日(last_date, 由记账轮写入 asof) + 行更新时间(updated_at, 仅审计)
CREATE TABLE IF NOT EXISTS paper_acct(
    acct        TEXT PRIMARY KEY,           -- 'V3' / 'T10' / 'T7'
    scheme      TEXT,
    cash        DOUBLE PRECISION,
    last_date   TEXT,                        -- 最近一次成功记账的 asof 日(YYYY-MM-DD); 用于 gate 判定与计息 dt
    updated_at  TIMESTAMPTZ DEFAULT now()
);

-- 持仓: 每账户每币一行
CREATE TABLE IF NOT EXISTS paper_pos(
    acct        TEXT,
    symbol      TEXT,
    units       DOUBLE PRECISION,
    entry_price DOUBLE PRECISION,
    entry_date  TEXT,
    PRIMARY KEY(acct, symbol)
);

-- 待成交: 信号日只记，下一 gate 轮开盘价成交（两段式）
CREATE TABLE IF NOT EXISTS paper_pending(
    acct         TEXT,
    symbol       TEXT,
    action       TEXT,                       -- 'BUY' / 'SELL'
    signal_date  TEXT,
    signal_price DOUBLE PRECISION,
    PRIMARY KEY(acct, symbol, signal_date)
);

-- 成交流水
CREATE TABLE IF NOT EXISTS paper_trade(
    id       BIGSERIAL PRIMARY KEY,
    acct     TEXT,
    date     TEXT,
    symbol   TEXT,
    action   TEXT,                          -- 'BUY' / 'SELL' / 'REBAL'
    price    DOUBLE PRECISION,
    notional DOUBLE PRECISION,
    cost     DOUBLE PRECISION,
    pnl_pct  REAL,
    note     TEXT
);

-- 净值曲线（桌面同步源）
CREATE TABLE IF NOT EXISTS paper_nav(
    acct       TEXT,
    date       TEXT,
    nav        DOUBLE PRECISION,
    cash_ratio REAL,
    PRIMARY KEY(acct, date)
);

-- 种子: 三账户全现金 100,000、空仓、无 pending（从部署日空仓起步，不回溯）
INSERT INTO paper_acct(acct, scheme, cash) VALUES
    ('V3',  'V3 等权+再平衡', 100000.0),
    ('T10', '#10 分层cap60',  100000.0),
    ('T7',  '#7 33%BTC',     100000.0)
ON CONFLICT (acct) DO NOTHING;

-- 常用查询
-- 净值:   SELECT acct, date, nav, cash_ratio FROM paper_nav ORDER BY acct, date DESC;
-- 持仓:   SELECT * FROM paper_pos WHERE units > 0 ORDER BY acct, symbol;
-- 待成交: SELECT * FROM paper_pending ORDER BY acct, signal_date;
-- 账户:   SELECT acct, scheme, cash, last_date, updated_at FROM paper_acct;
-- 清库重建(测试用):
--   TRUNCATE paper_trade, paper_nav; DELETE FROM paper_pos; DELETE FROM paper_pending;
--   UPDATE paper_acct SET cash=100000, last_date=NULL, updated_at=now();
