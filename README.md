# crypto-radar

24/7 加密货币机会雷达。每 30 分钟跑一轮，检测到机会推 QQ。

**状态**：🚧 基础设施已就绪，等 `radar_scan.py` 业务逻辑（1号 提供）+ QQ token 即可上线。

---

## 为什么数据源是 OKX 而不是币安

币安对美国 IP 返回 **HTTP 451**，而 GitHub-hosted runner 全部在美国
（实测定位 Azure `centralus` / `westus3`）。

2026-09-07 从真实 runner 实测：

| 端点 | 结果 |
|---|---|
| `fapi.binance.com`（合约/资金费率） | ❌ 451 |
| `api.binance.com` | ❌ 451 |
| `www.binance.com/bapi`（Launchpool） | ❌ 404/403 |
| **`www.okx.com/api/v5/*`** | ✅ **200** |
| `api.bybit.com` | ❌ 403（也封美国） |

→ 想跑币安只能上海外 VPS（~$3–5/月）。OKX 是美国 runner 上唯一可用的主流所。

数据字段详见交接目录的 `okx-data-contract.md`。

---

## 目录约定

```
crypto-radar/
├── .github/workflows/crypto-radar.yml   # 调度（每 30min）
├── radar_scan.py                        # ⬅ 1号 提供，入口脚本
└── README.md
```

`radar_scan.py` 必须在**仓库根目录**，workflow 直接 `python radar_scan.py`。

---

## radar_scan.py 接口约定

### 环境变量（workflow 已注入，直接 `os.environ` 读）

| 变量 | 来源 | 说明 |
|---|---|---|
| `DATABASE_URL` | Secrets ✅ 已配 | Neon Postgres 连接串 |
| `QQ_APP_ID` | Secrets ⬜ 待配 | QQ bot |
| `QQ_TOKEN` | Secrets ⬜ 待配 | QQ bot |
| `FORCE_PUSH` | workflow_dispatch | `'true'` 时忽略去重强制推送（测试用） |

### 行为要求

1. 拉 `https://www.okx.com/api/v5/public/instruments?instType=SWAP`
2. **新合约**：`listTime` 在近 6h 内 → `NEW_LISTING`；`state=preopen` 且 `listTime` 在未来 48h 内 → `UPCOMING_LISTING`
3. **资金费率**：对 watchlist 查 `funding-rate`，超阈值 → `FUNDING_ANOMALY`
4. **去重**：写 `radar_seen` 表，`sig_key` 建议 `f"{type}:{instId}:{bucket}"`，`UNIQUE` 冲突就跳过
5. **记录运行**：写 `radar_runs` 表
6. **推送**：命中的信号发 QQ

### 依赖

只允许 `asyncpg`（workflow 已 `pip install`）。其余用标准库。
**不要引入 requests / ccxt / pandas** —— 保持启动快，省 Actions 额度。

### 退出码

- `0` 正常（含"本轮无信号"）
- 非 `0` 视为失败，workflow 会写一条 `ok=false` 的 `radar_runs` 记录

---

## 数据库（Neon，已建表）

```sql
radar_seen   -- 去重：sig_key UNIQUE
radar_runs   -- 运行日志
radar_recent -- 视图：最近 24h 信号
```

建表 SQL 见交接目录 `schema.sql`（幂等，可重复执行）。

本机直连 Neon 无需代理：

```bash
psql "$DATABASE_URL" -c "SELECT * FROM radar_recent LIMIT 20;"
```

---

## 额度提醒 ⚠️

本仓库是**私有**的，GitHub Free 私有仓库 Actions 额度 **2000 分钟/月**。

- 每 30 分钟一轮 = 1440 轮/月 × 1 分钟 ≈ **1440 分钟**，占额度 72%
- 若哪天超额，改 `.github/workflows/crypto-radar.yml` 的 cron 为 `0 * * * *`（每小时 → 720 分钟）

单轮控制在 60 秒内最稳。

---

## 常用命令

```bash
gh run list  --repo identify-safety/crypto-radar
gh run watch <run-id> --repo identify-safety/crypto-radar
gh run view  <run-id> --repo identify-safety/crypto-radar --log

# 手动触发（可强制推送测试信号）
gh workflow run crypto-radar.yml --repo identify-safety/crypto-radar -f force_push=true

# 配 secret
gh secret set QQ_APP_ID --repo identify-safety/crypto-radar --body "xxx"
```
