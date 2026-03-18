# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Running Code

```bash
# 功能验证（每次改动后运行）
python test.py

# 工作流
python -m crawler.workflow init        # 首次历史初始化（一次性）
python -m crawler.workflow daily       # 每日增量（收盘后）
python -m crawler.workflow financial   # 季度财务报表

# K线可视化
python view_kline.py 600036            # 近90天
python view_kline.py 600036 180        # 近180天
python view_kline.py 600036 2024-01-01 2024-06-30  # 指定范围
```

## Data Source

- 股票池：akshare `sw_index_third_cons`（申万三级行业，不走东方财富CDN）
- 历史K线/每日行情：akshare `stock_zh_a_daily`（新浪财经，需 sh/sz 前缀）
- 财务报表：akshare `stock_financial_report_sina`（新浪财务）
- 公告：akshare `stock_notice_report`

## Key Files

| 文件 | 作用 |
|------|------|
| `crawler/config.py` | 行业板块（申万代码）、路径、并发数 |
| `crawler/stock_pool.py` | 构建股票池 |
| `crawler/workflow.py` | 三个工作流入口 |
| `crawler/fetchers/market.py` | 行情爬取 |
| `crawler/fetchers/financial.py` | 财务报表爬取 |
| `crawler/fetchers/news.py` | 公告爬取 |
| `view_kline.py` | 日K线可视化（保存PNG并自动打开） |
| `test.py` | 端到端验证（6项测试） |

## Windows Encoding

Python stdout/stderr 默认 GBK，所有脚本开头已加 UTF-8 强制转换，无需额外配置。

## 报告推送（飞书/钉钉）

```bash
# 推送最新交易日报告
python push_report.py

# 预览消息内容（不实际发送）
python push_report.py --dry-run

# 推送指定日期
python push_report.py --date 2026-03-14

# 全流程自动化（爬取 → 生成报告 → 推送）
run_daily.bat
```

**配置步骤：**
1. 复制 `.env.example` 为 `.env`
2. 填入飞书或钉钉机器人的 Webhook URL
3. 运行 `python push_report.py --dry-run` 验证消息格式

**Windows 任务计划（每工作日 16:30 自动运行）：**
```bat
schtasks /create /tn "StockDaily" /tr "D:\project\claudeCode\.worktrees\stock-analysis\run_daily.bat" /sc weekly /d MON,TUE,WED,THU,FRI /st 16:30 /f
```

## Proxy Helper

`cl.bat` is a Windows batch script that launches Claude CLI with a local HTTP proxy on port 7890 (configured for Shadowrocket or similar tools). Use `cl` instead of `claude` when the proxy is needed.
