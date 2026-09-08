-- P1 日级信号表（2026-09-08 并入 crypto-radar）
-- 用法: psql "$DATABASE_URL" -f schema_p1.sql
-- 幂等: 可重复执行；已有表不会被覆盖，已有行不会被改动。
-- 注: radar_scan.py 的 p1_ensure_tables() 每轮也会执行同样的 DDL，
--     这份 SQL 只用于人工预建 / 1号 验表结构，两者定义必须保持一致。

-- 状态表: 每币一行，权威持仓状态 + 门控字段 last_date
CREATE TABLE IF NOT EXISTS p1_state(
  symbol       TEXT PRIMARY KEY,
  position     TEXT NOT NULL DEFAULT 'out',   -- 'in' / 'out'
  entry_date   TEXT,                          -- 入场日 (UTC 日线日期)
  entry_price  DOUBLE PRECISION,              -- 入场价 (该日收盘)
  exit_date    TEXT,                          -- 最近一次出场日
  exit_price   DOUBLE PRECISION,
  last_date    TEXT,                          -- 已处理的最新日线日期 = 门控依据
  updated_at   TIMESTAMPTZ DEFAULT now()
);

-- 观测日志: 每触发日每币一行（无信号也记，供复盘/桌面表格同步）
CREATE TABLE IF NOT EXISTS p1_log(
  date      TEXT,
  symbol    TEXT,
  close     DOUBLE PRECISION,
  hh20      DOUBLE PRECISION,                 -- 前 20 日最高收（不含当日）
  ll10      DOUBLE PRECISION,                 -- 前 10 日最低收（不含当日）
  vol_ratio DOUBLE PRECISION,                 -- 当日量 / 前 20 日均量
  signal    TEXT,                             -- 'BUY' / 'SELL' / 'none'
  position  TEXT,                             -- 处理后所处状态
  note      TEXT,
  PRIMARY KEY(date, symbol)                   -- 同 (date,symbol) 重复写入自动去重
);

-- 初始种子: 5 币全 out，last_date 为空（首个触发日自然写入）
-- 与 radar_scan.py 的 P1_SYMBOLS 保持一致
INSERT INTO p1_state(symbol, position)
SELECT unnest(ARRAY['BTCUSDT','ETHUSDT','XRPUSDT','SOLUSDT','DOGEUSDT']::text[]), 'out'
ON CONFLICT (symbol) DO NOTHING;

-- HYPE 观察线标的：由 paper_accounting 运行时写入/维护（HV3 账户用）；
-- 此处预建以便人工验表。注意 Binance 现货/合约均无 HYPEUSDT（未上架），
--   实际数据走 OKX 现货 HYPE-USDT / Hyperliquid（见 paper_accounting）。
INSERT INTO p1_state(symbol, position) VALUES('HYPEUSDT','out')
ON CONFLICT (symbol) DO NOTHING;

-- 常用查询
-- 当前状态:   SELECT * FROM p1_state ORDER BY symbol;
-- 信号历史:   SELECT * FROM p1_log WHERE signal <> 'none' ORDER BY date DESC, symbol;
-- 近期观测:   SELECT * FROM p1_log ORDER BY date DESC, symbol LIMIT 30;
-- 门控体检:   SELECT symbol, last_date FROM p1_state;   -- 应都等于 UTC当日-1
