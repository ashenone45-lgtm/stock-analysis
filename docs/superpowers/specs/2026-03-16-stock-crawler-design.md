# Stock Crawler 设计文档

**日期：** 2026-03-16
**状态：** 已批准
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

通过 `akshare.stock_board_industry_cons_em()` 按行业板块拉取成分股，覆盖以下板块：

- IT 服务/软件
- 互联网
- 半导体/芯片
- 卫星/航天
- 有色金属（铜、锂、稀土）

板块名称映射表维护在 `crawler/config.py` 中，可随时增减。多板块股票合并去重后形成最终股票池。

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
│   ├── workflow.py         # ruflo workflow 编排入口
│   └── config.py           # 行业板块映射、数据路径、参数配置
├── data/
│   ├── market/             # data/market/{stock_code}.parquet
│   ├── financial/          # data/financial/{stock_code}_{report_type}.parquet
│   └── news/               # data/news/{YYYY-MM-DD}.parquet
├── logs/
│   └── errors.log          # 失败记录
└── test.py                 # 手动验证脚本
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
  → 字段：日期、开、高、低、收、量、额、换手率
  → 写入 data/market/{code}.parquet
```

### 5.2 每日增量工作流（每个交易日收盘后触发）

```
fetch_daily_spot
  → akshare.stock_zh_a_spot_em()
  → 过滤股票池，追加当日行情到各股票 Parquet 文件

fetch_daily_news
  → akshare.stock_notice_report()
  → 过滤股票池，写入 data/news/{today}.parquet
```

### 5.3 财务数据工作流（每季度触发）

```
[并发] fetch_financial(每只股票)
  → akshare.stock_financial_report_sina()
  → 三张报表分别存 Parquet
```

---

## 6. 存储设计

`storage/parquet.py` 提供两个公共方法：

- `write(df, path)` — 读取现有文件（若存在），与新数据合并，按日期去重后写回。幂等安全，重跑不产生脏数据。
- `read(path, start=None, end=None)` — 加载文件，按日期范围过滤后返回 DataFrame。

---

## 7. 错误处理

| 场景 | 处理方式 |
|------|---------|
| 网络超时/请求失败 | 自动重试 3 次（指数退避 1s/2s/4s），失败记录到 `logs/errors.log` |
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

## 9. 后续扩展（不在当前范围内）

- 接入 SQLite / PostgreSQL
- 添加数据质量检查（缺口检测、异常值过滤）
- 基于爬取数据的分析模块
