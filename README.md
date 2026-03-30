# 三市场股票日报 · A股 / 港股 / 美股

基于 akshare 的多市场量化日报系统，覆盖 A股、港股、美股三大市场，每日收盘后自动爬取行情、生成分析报告，并推送到飞书 / 钉钉。

**📊 [历史日报存档](https://ashenone45-lgtm.github.io/stock-analysis/)**　｜　[A股](https://ashenone45-lgtm.github.io/stock-analysis/index_a.html)　[港股](https://ashenone45-lgtm.github.io/stock-analysis/index_hk.html)　[美股](https://ashenone45-lgtm.github.io/stock-analysis/index_us.html)

---

## 市场覆盖

| 市场 | 股票池 | 数据源 |
|------|--------|--------|
| **A股** | 申万行业成分股 + 指数/主题ETF（共 ~540 只） | akshare `stock_zh_a_daily`（新浪财经） |
| **港股** | 手动维护核心板块 + 指数ETF（共 ~109 只） | akshare `stock_hk_hist`（东方财富） |
| **美股** | 手动维护9大板块 + ETF（共 ~178 只） | akshare `stock_us_hist`（东方财富） |

---

## 安装

```bash
pip install -r requirements.txt
```

---

## 快速开始

### 一键三市场（推荐）

```bat
run_daily_all.bat
```

自动完成：爬取行情 → 生成报告 → 推送飞书 → 更新主页 → Git 提交，覆盖 A股 + 港股 + 美股。

### 单市场运行

```bash
# A股
python -m crawler.workflow daily
python gen_report.py --market a
python push_report.py --market a

# 港股
python -m crawler.hk_workflow daily
python gen_report.py --market hk
python push_report.py --market hk

# 美股
python -m crawler.us_workflow daily
python gen_report.py --market us
python push_report.py --market us
```

### 首次初始化（拉取历史数据）

```bash
# A股（~523只股票，需 10~20 分钟）
python -m crawler.workflow init

# 港股（~109只，需 5~10 分钟）
python -m crawler.hk_workflow init

# 美股（~178只，需先拉代码映射表，约 3~5 分钟 + 爬取）
python -m crawler.us_workflow init
```

---

## 飞书 / 钉钉推送配置

1. 复制 `.env.example` 为 `.env`
2. 填入 Webhook URL：

```env
FEISHU_WEBHOOK=https://open.feishu.cn/open-apis/bot/v2/hook/xxxx
DINGTALK_WEBHOOK=https://oapi.dingtalk.com/robot/send?access_token=xxxx
```

3. 验证（只打印消息，不发送）：

```bash
python push_report.py --market a --dry-run
python push_report.py --market hk --dry-run
python push_report.py --market us --dry-run
```

---

## Windows 任务计划（自动定时运行）

```bat
schtasks /create /tn "StockDailyAll" /tr "D:\project\claudeCode\.worktrees\stock-analysis\run_daily_all.bat" /sc weekly /d MON,TUE,WED,THU,FRI /st 17:30 /f
```

> 建议 17:30 后运行，此时 A股（15:00）、港股（16:00）均已收盘；美股盘前数据也已可用。

---

## 存档页管理

```bash
# 重建三市场存档页和主落地页
python gen_index.py --market all

# 单市场回填（适用于补录历史报告后）
python gen_index.py --market a
python gen_index.py --market hk
python gen_index.py --market us
```

---

## 关键文件

| 文件 | 用途 |
|------|------|
| `run_daily_all.bat` | **三市场一键全流程**（推荐入口） |
| `run_daily.bat` | 仅 A股 |
| `run_daily_hk.bat` | 仅港股 |
| `run_daily_us.bat` | 仅美股 |
| `gen_report.py --market [a\|hk\|us]` | 生成日报 |
| `push_report.py --market [a\|hk\|us]` | 推送飞书/钉钉 |
| `gen_index.py --market [a\|hk\|us\|all]` | 重建存档页 |
| `crawler/config.py` | A股行业板块 + ETF配置 |
| `crawler/hk_config.py` | 港股股票池 |
| `crawler/us_config.py` | 美股股票池 |

> 开发文档详见 [CLAUDE.md](CLAUDE.md)
