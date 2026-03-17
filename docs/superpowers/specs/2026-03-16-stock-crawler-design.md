# Stock Crawler 设计文档

**日期：** 2026-03-16
**更新：** 2026-03-17
**状态：** 已实现
**范围：** A 股高新行业股票数据爬虫（行情为主，财务/公告为辅）

---

## 1. 背景与目标

构建一个 A 股数据爬虫，专注于 IT/互联网、半导体、卫星/航天、有色金属等高新行业板块。

**目标：**
- 首次运行时同步近 3 年历史日 K 线数据
- 此后每个交易日收盘后增量更新当日行情
- 附带财务数据（季度级）和公告数据（日级）
- 数据以 Parquet 文件存储，为后续分析做准备

**非目标：**
- 实时/分钟级行情
- 全市场覆盖
- 数据库接入（当前阶段）

---

## 2. 技术选型

| 组件 | 选型 | 理由 |
|------|------|------|
| 数据源 | akshare | 免费、无 token、覆盖 A 股行情/财务/公告 |
| 任务编排 | ruflo workflow + terminal | 支持定时触发和任务依赖 |
| 存储格式 | Parquet | 列式压缩，适合时序数据和 pandas 分析 |
| 运行环境 | Python 3.10+ | 现有项目环境 |

---

## 3. 股票池

通过 `akshare.sw_index_third_cons(symbol)` 按**申万三级行业代码**拉取成分股，覆盖以下行业：

- IT 服务、软件开发
- 互联网
- 半导体（6个子行业）
- 卫星航天
- 有色金属（5个子行业）

行业代码映射表维护在 `crawler/config.py` 的 `INDUSTRY_BOARDS` 中（格式如 `"850813.SI"`），可随时增减。多行业股票合并去重后形成最终股票池（约 523 只）。

> **注：** 原设计使用 `stock_board_industry_cons_em`（东方财富CDN），因 push2.eastmoney.com 在部分网络环境下不可访问，已替换为申万行业接口。

---

## 4. 项目结构

```
stock-analysis/
├── crawler/
│   ├── fetchers/
│   │   ├── market.py       # 行情数据（日K历史 + 每日快照）
│   │   ├── financial.py    # 财务数据（利润表/资产负债表/现金流）
│   │   └── news.py         # 公告数据
│   ├── storage/
│   │   └── parquet.py      # 统一 Parquet 读写（幂等，按日期去重）
│   ├── workflow.py         # 工作流编排入口（init/daily/financial）
│   └── config.py           # 行业板块映射（申万代码）、数据路径、参数配置
├── data/
│   ├── market/             # data/market/{stock_code}.parquet
│   ├── financial/          # data/financial/{stock_code}_{report_type}.parquet
│   └── news/               # data/news/{YYYY-MM-DD}.parquet
├── logs/
│   └── errors.log          # 失败记录
├── view_kline.py           # 日K线可视化（mplfinance，保存PNG）
└── test.py                 # 手动验证脚本（6项端到端测试）
```

---

## 5. 数据流

### 5.1 历史初始化工作流（一次性）

```
build_stock_pool
  → 按行业板块拉取成分股列表
  → 合并去重，写入内存/配置

[并发] fetch_history(每只股票)
  → akshare.stock_zh_a_hist(symbol, period="daily", start_date, end_date)
  → start_date = 运行时动态计算：today - 3 years；end_date = today
  → 字段：日期、开、高、低、收、量、额、换手率
  → 写入 data/market/{code}.parquet
```

### 5.2 每日增量工作流（每个交易日收盘后触发）

```
fetch_daily_spot
  → 逐只调用 akshare.stock_zh_a_hist(start_date=today, end_date=today)
  → 并发写入各股票 Parquet 文件（push2his.eastmoney.com，可直连）

fetch_daily_news
  → akshare.stock_notice_report()
  → 过滤股票池，写入 data/news/{today}.parquet
```

> **注：** 原设计使用 `stock_zh_a_spot_em()`（一次性拉取全市场快照），因依赖 push2.eastmoney.com CDN，已改为逐只调用历史K线接口并发拉取当日数据。

### 5.3 财务数据工作流（每季度触发）

触发时机：每个自然季度首次运行时，或手动执行。

```
[并发] fetch_financial(每只股票)
  → akshare.stock_financial_report_sina()
  → 三张报表分别存 Parquet
  → 去重键：报告期（如 "2024-09-30"），而非日历日期
```

---

## 6. 存储设计

`storage/parquet.py` 提供两个公共方法：

- `write(df, path, dedup_key="日期")` — 读取现有文件（若存在），与新数据合并，按 `dedup_key` 去重后写回。行情数据去重键为 `"日期"`，财务数据去重键为 `"报告期"`。幂等安全，重跑不产生脏数据。
- `read(path, start=None, end=None)` — 加载文件，按日期范围过滤后返回 DataFrame。

---

## 7. 错误处理

| 场景 | 处理方式 |
|------|---------|
| 网络超时/请求失败 | 自动重试 3 次（指数退避 1s/2s/4s），失败记录到 `logs/errors.log`，格式：`[timestamp] [stock_code] [error_message] [retry_count]` |
| 返回数据为空 | 跳过写入，打印警告，不中断流程 |
| 重复数据 | write() 内部去重，安全幂等 |
| 单只股票失败 | 不影响并发中其他股票的处理 |

---

## 8. 测试策略

`test.py` 作为手动验证入口：

1. 构建股票池，打印股票数量
2. 抽取 5 只代表性股票（跨不同行业）运行历史初始化
3. 打印每只股票的数据行数、日期范围、文件大小
4. 运行每日增量，验证数据追加正确

当前阶段不写单元测试，脚本验证足够。

---

## 9. 数据可视化

`view_kline.py` 提供日K线图查看工具（基于 mplfinance）：

```bash
python view_kline.py 600036            # 近90天
python view_kline.py 600036 180        # 近N天
python view_kline.py 600036 2024-01-01 2024-06-30  # 指定范围
```

图表内容：K线（红涨绿跌）+ MA5/MA20/MA60 均线 + 成交量柱图，结果保存为 PNG 并自动打开。

---

## 10. 后续扩展（不在当前范围内）

- 接入 SQLite / PostgreSQL
- 添加数据质量检查（缺口检测、异常值过滤）
- 基于爬取数据的分析模块（选股、回测）
