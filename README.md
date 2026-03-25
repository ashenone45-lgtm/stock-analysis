# A股数据爬虫 + 日报推送

基于 akshare 的 A 股高新行业数据爬虫，覆盖 IT/互联网、半导体、卫星航天、有色金属等行业，约 523 只股票。每日收盘后自动生成分析报告并推送到飞书/钉钉。

**📊 [历史日报存档](https://ashenone45-lgtm.github.io/stock-analysis/)**

## 安装

```bash
pip install -r requirements.txt
```

## 使用

```bash
python -m crawler.workflow init    # 首次初始化历史数据
python -m crawler.workflow daily   # 每日增量
python gen_report.py               # 生成日报
python push_report.py              # 推送到飞书/钉钉
run_daily.bat                      # 一键全流程
```

推送配置：复制 `.env.example` 为 `.env`，填入 Webhook URL。

> 开发文档详见 [CLAUDE.md](CLAUDE.md)
