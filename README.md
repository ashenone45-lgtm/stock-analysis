# A股数据爬虫

基于 akshare 的 A 股高新行业数据爬虫，覆盖 IT/互联网、半导体、卫星航天、有色金属等行业，约 523 只股票。数据以 Parquet 格式本地存储。

## 快速开始

### 安装依赖

```bash
pip install akshare pandas pyarrow mplfinance
```

### 验证环境

```bash
python test.py
```

6项测试全部 `[OK]` 即表示环境正常。

### 首次初始化

```bash
python -m crawler.workflow init
```

拉取近3年历史日K线数据（约 523 只股票，耗时较长）。

### 每日增量（收盘后运行）

```bash
python -m crawler.workflow daily
```

### 季度财务报表

```bash
python -m crawler.workflow financial
```

## 查看K线图

```bash
python view_kline.py 600036            # 近90天
python view_kline.py 600036 180        # 近180天
python view_kline.py 600036 2024-01-01 2024-06-30  # 指定范围
```

运行后自动保存 PNG 并打开。

## 项目结构

```
stock-analysis/
├── crawler/
│   ├── config.py           # 行业板块（申万三级代码）、路径、并发数
│   ├── stock_pool.py       # 股票池构建
│   ├── workflow.py         # 工作流入口（init/daily/financial）
│   ├── fetchers/
│   │   ├── market.py       # 行情（历史K线 + 每日快照）
│   │   ├── financial.py    # 财务报表（利润表/资产负债表/现金流）
│   │   └── news.py         # 公告数据
│   └── storage/
│       └── parquet.py      # 幂等 Parquet 读写
├── data/
│   ├── market/             # {code}.parquet
│   ├── financial/          # {code}_{income|balance|cashflow}.parquet
│   └── news/               # {YYYY-MM-DD}.parquet
├── view_kline.py           # K线可视化
└── test.py                 # 端到端验证脚本
```

## 自定义行业

修改 `crawler/config.py` 中的 `INDUSTRY_BOARDS`，添加申万三级行业代码即可：

```python
INDUSTRY_BOARDS = {
    "新能源车": ["850832.SI", "850833.SI"],
    "半导体":   ["850812.SI", ...],
}
```

## 数据源说明

| 数据 | 接口 | 服务器 |
|------|------|--------|
| 股票池 | `sw_index_third_cons` | 申万官网 |
| 历史K线 | `stock_zh_a_hist` | push2his.eastmoney.com |
| 财务报表 | `stock_financial_report_sina` | 新浪财务 |
| 公告 | `stock_notice_report` | 东方财富 |
