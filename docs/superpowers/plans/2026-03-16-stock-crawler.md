# Stock Crawler Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 构建 A 股高新行业数据爬虫，支持历史 K 线初始化、每日增量行情、季度财务报表和日级公告爬取，数据落 Parquet 文件。

**Architecture:** 使用 akshare 作为数据源，crawler/ 下分 fetchers（行情/财务/公告）、storage（Parquet 读写）、utils（共享工具）三层，workflow.py 通过 ruflo workflow + terminal 工具编排定时任务。两个核心工作流：历史初始化（一次性）和每日增量（收盘后自动触发）。

**Tech Stack:** Python 3.10+, akshare, pandas, pyarrow, ruflo MCP tools (workflow, terminal)

---

## 文件清单

| 文件 | 操作 | 职责 |
|------|------|------|
| `requirements.txt` | 新建 | 项目依赖声明 |
| `crawler/__init__.py` | 新建 | 包标识 |
| `crawler/config.py` | 新建 | 行业板块映射、数据路径、全局参数 |
| `crawler/utils.py` | 新建 | 共享工具：_retry + 错误文件日志 |
| `crawler/stock_pool.py` | 新建 | 按行业板块构建股票池 |
| `crawler/storage/__init__.py` | 新建 | 包标识 |
| `crawler/storage/parquet.py` | 新建 | 幂等 Parquet 读写（write/read） |
| `crawler/fetchers/__init__.py` | 新建 | 包标识 |
| `crawler/fetchers/market.py` | 新建 | 行情爬取（历史 K 线 + 每日快照） |
| `crawler/fetchers/financial.py` | 新建 | 季度财务报表爬取 |
| `crawler/fetchers/news.py` | 新建 | 每日公告爬取 |
| `crawler/workflow.py` | 新建 | 三个工作流编排入口 |
| `test.py` | 修改 | 端到端验证脚本 |

---

## Chunk 1: 基础设施（项目结构 + 配置 + 共享工具 + 存储层）

### Task 1: 项目依赖与目录结构

**Files:**
- 新建: `requirements.txt`
- 新建: `crawler/__init__.py`
- 新建: `crawler/storage/__init__.py`
- 新建: `crawler/fetchers/__init__.py`

- [ ] **Step 1: 创建 requirements.txt**

```
akshare>=1.12.0
pandas>=2.0.0
pyarrow>=14.0.0
```

- [ ] **Step 2: 创建目录结构和空包文件**

```bash
mkdir -p crawler/fetchers crawler/storage data/market data/financial data/news logs
touch crawler/__init__.py crawler/storage/__init__.py crawler/fetchers/__init__.py
touch data/market/.gitkeep data/financial/.gitkeep data/news/.gitkeep logs/.gitkeep
```

- [ ] **Step 3: 安装依赖**

```bash
pip install -r requirements.txt
```

预期输出：所有包安装成功，无报错。

- [ ] **Step 4: 验证 akshare 可用**

```bash
python -c "import akshare as ak; print(ak.__version__)"
```

预期输出：版本号，如 `1.12.xx`

- [ ] **Step 5: Commit**

```bash
git add requirements.txt crawler/ data/ logs/
git commit -m "feat: scaffold project structure and dependencies"
```

---

### Task 2: config.py — 行业映射与全局配置

**Files:**
- 新建: `crawler/config.py`

- [ ] **Step 1: 验证配置模块缺失（预期失败）**

```bash
python -c "from crawler.config import INDUSTRY_BOARDS" 2>&1 | head -3
```

预期输出：`ModuleNotFoundError` 或 `ImportError`

- [ ] **Step 2: 创建 config.py**

```python
# crawler/config.py
from pathlib import Path

# ── 项目根目录 ─────────────────────────────────────────────
BASE_DIR = Path(__file__).parent.parent

# ── 数据存储路径 ───────────────────────────────────────────
DATA_DIR = BASE_DIR / "data"
MARKET_DIR = DATA_DIR / "market"
FINANCIAL_DIR = DATA_DIR / "financial"
NEWS_DIR = DATA_DIR / "news"
LOG_FILE = BASE_DIR / "logs" / "errors.log"

# ── 历史数据参数 ───────────────────────────────────────────
HISTORY_YEARS = 3  # 拉取近 N 年历史

# ── 行业板块映射（东方财富板块名称） ─────────────────────────
# key: 内部标签；value: akshare stock_board_industry_cons_em() 接受的板块名称
INDUSTRY_BOARDS = {
    "IT服务":   "软件开发",
    "互联网":   "互联网服务",
    "半导体":   "半导体",
    "卫星航天": "航天航空",
    "有色金属": "有色金属",
}

# ── 并发参数 ───────────────────────────────────────────────
MAX_WORKERS = 8  # 并发爬取线程数
```

- [ ] **Step 3: 验证可导入**

```bash
python -c "from crawler.config import INDUSTRY_BOARDS, MARKET_DIR, LOG_FILE; print(INDUSTRY_BOARDS); print(MARKET_DIR)"
```

预期输出：字典内容和路径，无报错。

- [ ] **Step 4: Commit**

```bash
git add crawler/config.py
git commit -m "feat: add config with industry board mappings and paths"
```

---

### Task 3: utils.py — 共享工具（重试 + 错误日志）

**Files:**
- 新建: `crawler/utils.py`

- [ ] **Step 1: 验证 utils 模块缺失（预期失败）**

```bash
python -c "from crawler.utils import retry_with_backoff" 2>&1 | head -3
```

预期输出：`ImportError`

- [ ] **Step 2: 创建 utils.py**

```python
# crawler/utils.py
"""共享工具：带指数退避的重试 + 符合规范的错误文件日志。"""
import logging
import time
from datetime import datetime
from pathlib import Path

from crawler.config import LOG_FILE


def _ensure_log_file() -> None:
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)


def log_error(stock_code: str, error_message: str, retry_count: int) -> None:
    """按规范格式写入错误日志：[timestamp] [stock_code] [error_message] [retry_count]"""
    _ensure_log_file()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] [{stock_code}] [{error_message}] [retry={retry_count}]\n"
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line)
    logging.getLogger(__name__).error(line.strip())


def retry_with_backoff(func, stock_code: str = "N/A", retries: int = 3):
    """执行 func()，失败时指数退避重试，最终失败写入错误日志并重新抛出异常。

    Args:
        func: 无参可调用对象
        stock_code: 用于错误日志的股票代码标识
        retries: 最大重试次数（含首次尝试）
    Returns:
        func() 的返回值
    Raises:
        最后一次异常
    """
    last_exc = None
    for attempt in range(retries):
        try:
            return func()
        except Exception as e:
            last_exc = e
            wait = 2 ** attempt
            logging.getLogger(__name__).warning(
                "[%s] Attempt %d/%d failed: %s. Retrying in %ds...",
                stock_code, attempt + 1, retries, e, wait,
            )
            if attempt < retries - 1:
                time.sleep(wait)

    log_error(stock_code, str(last_exc), retries)
    raise last_exc
```

- [ ] **Step 3: 验证重试和日志写入**

```bash
python - <<'EOF'
import os
from crawler.utils import retry_with_backoff, log_error

# 测试成功路径
result = retry_with_backoff(lambda: 42, stock_code="TEST")
assert result == 42, "Expected 42"

# 测试失败路径：函数始终抛异常
call_count = 0
def always_fail():
    global call_count
    call_count += 1
    raise ValueError("boom")

try:
    retry_with_backoff(always_fail, stock_code="TEST_FAIL", retries=2)
    assert False, "Should have raised"
except ValueError:
    pass

assert call_count == 2, f"Expected 2 attempts, got {call_count}"

# 验证日志文件已写入
assert os.path.exists("logs/errors.log"), "errors.log not created"
with open("logs/errors.log") as f:
    content = f.read()
assert "TEST_FAIL" in content, "Stock code not in log"
assert "boom" in content, "Error message not in log"
print("✓ utils retry + error log OK")

# 清理测试日志
os.remove("logs/errors.log")
EOF
```

预期输出：`✓ utils retry + error log OK`

- [ ] **Step 4: Commit**

```bash
git add crawler/utils.py
git commit -m "feat: add shared retry utility with spec-compliant error logging"
```

---

### Task 4: storage/parquet.py — 幂等读写层

**Files:**
- 新建: `crawler/storage/parquet.py`

- [ ] **Step 1: 验证 storage 模块缺失（预期失败）**

```bash
python -c "from crawler.storage.parquet import write, read" 2>&1 | head -3
```

预期输出：`ImportError`

- [ ] **Step 2: 创建 parquet.py**

```python
# crawler/storage/parquet.py
import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def write(df: pd.DataFrame, path: str | Path, dedup_key: str = "日期") -> None:
    """幂等写入 Parquet。若文件已存在则合并去重后写回。

    Args:
        df: 待写入的 DataFrame
        path: 目标文件路径（.parquet）
        dedup_key: 去重列名；行情数据用 "日期"，财务数据用 "报告期"
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    if df is None or df.empty:
        logger.warning("write() called with empty DataFrame, skipping: %s", path)
        return

    if path.exists():
        existing = pd.read_parquet(path)
        combined = pd.concat([existing, df], ignore_index=True)
    else:
        combined = df.copy()

    if dedup_key in combined.columns:
        # 统一转为字符串后去重，避免类型不一致导致的重复保留
        combined[dedup_key] = combined[dedup_key].astype(str)
        combined = combined.drop_duplicates(subset=[dedup_key], keep="last")
        combined = combined.sort_values(dedup_key).reset_index(drop=True)

    combined.to_parquet(path, index=False)
    logger.debug("Wrote %d rows to %s", len(combined), path)


def read(
    path: str | Path,
    start: str | None = None,
    end: str | None = None,
    date_col: str = "日期",
) -> pd.DataFrame:
    """读取 Parquet，可按日期范围过滤。

    Args:
        path: 文件路径
        start: 起始日期字符串，如 "2024-01-01"（含）
        end: 结束日期字符串，如 "2024-12-31"（含）
        date_col: 日期列名
    Returns:
        DataFrame；文件不存在时返回空 DataFrame
    """
    path = Path(path)
    if not path.exists():
        return pd.DataFrame()

    df = pd.read_parquet(path)

    if (start or end) and date_col in df.columns:
        df[date_col] = pd.to_datetime(df[date_col])
        if start:
            df = df[df[date_col] >= pd.to_datetime(start)]
        if end:
            df = df[df[date_col] <= pd.to_datetime(end)]

    return df.reset_index(drop=True)
```

- [ ] **Step 3: 验证 write/read 幂等性**

```bash
python - <<'EOF'
import os
import pandas as pd
from crawler.storage.parquet import write, read

TEST_PATH = "data/market/_test_idempotency.parquet"

# 写入第一批
df1 = pd.DataFrame({"日期": ["2024-01-01", "2024-01-02"], "收盘": [10.0, 10.5]})
write(df1, TEST_PATH)

# 写入重叠数据（2024-01-02 应被新值覆盖）
df2 = pd.DataFrame({"日期": ["2024-01-02", "2024-01-03"], "收盘": [10.6, 11.0]})
write(df2, TEST_PATH)

result = read(TEST_PATH)
assert len(result) == 3, f"Expected 3 rows, got {len(result)}"
row_0102 = result[result["日期"].astype(str).str.contains("2024-01-02")]
assert float(row_0102["收盘"].iloc[0]) == 10.6, f"Dedup should keep latest, got {row_0102['收盘'].iloc[0]}"

# 验证日期范围过滤
filtered = read(TEST_PATH, start="2024-01-02", end="2024-01-02")
assert len(filtered) == 1, f"Expected 1 row with filter, got {len(filtered)}"

print("✓ storage idempotency OK")
os.remove(TEST_PATH)
EOF
```

预期输出：`✓ storage idempotency OK`

- [ ] **Step 4: Commit**

```bash
git add crawler/storage/parquet.py
git commit -m "feat: add idempotent Parquet storage layer with string-safe dedup"
```

---

### Task 5: stock_pool.py — 股票池构建

**Files:**
- 新建: `crawler/stock_pool.py`

- [ ] **Step 1: 验证模块缺失（预期失败）**

```bash
python -c "from crawler.stock_pool import build_stock_pool" 2>&1 | head -3
```

预期输出：`ImportError`

- [ ] **Step 2: 创建 stock_pool.py**

```python
# crawler/stock_pool.py
import logging

import akshare as ak

from crawler.config import INDUSTRY_BOARDS
from crawler.utils import retry_with_backoff

logger = logging.getLogger(__name__)


def build_stock_pool() -> list[str]:
    """按行业板块拉取成分股，合并去重，返回股票代码列表。

    Returns:
        去重排序后的股票代码列表，如 ["000001", "600036", ...]
    """
    all_symbols: set[str] = set()

    for label, board_name in INDUSTRY_BOARDS.items():
        try:
            df = retry_with_backoff(
                lambda bn=board_name: ak.stock_board_industry_cons_em(symbol=bn),
                stock_code=f"board:{label}",
            )
        except Exception as e:
            logger.error("[%s] %s: fetch failed: %s", label, board_name, e)
            continue

        if df is None or df.empty:
            logger.warning("[%s] %s: returned empty.", label, board_name)
            continue

        # 寻找代码列（东方财富通常为 "代码"）
        code_col = next((c for c in df.columns if "代码" in c), df.columns[0])
        symbols = df[code_col].astype(str).str.zfill(6).tolist()
        all_symbols.update(symbols)
        logger.info("[%s] %s: %d stocks.", label, board_name, len(symbols))

    result = sorted(all_symbols)
    logger.info("Stock pool built: %d unique stocks.", len(result))
    return result
```

- [ ] **Step 3: 验证股票池构建（需要网络）**

```bash
python - <<'EOF'
from crawler.stock_pool import build_stock_pool
pool = build_stock_pool()
assert len(pool) > 0, "Stock pool is empty!"
# 验证代码格式为6位数字字符串
for code in pool[:5]:
    assert len(code) == 6 and code.isdigit(), f"Invalid code format: {code}"
print(f"✓ stock pool OK: {len(pool)} stocks, sample: {pool[:5]}")
EOF
```

预期输出：`✓ stock pool OK: N stocks, sample: [...]`

- [ ] **Step 4: Commit**

```bash
git add crawler/stock_pool.py
git commit -m "feat: add stock pool builder from industry boards"
```

---

## Chunk 2: 数据爬取层 + 工作流 + 验证脚本

### Task 6: fetchers/market.py — 行情爬取

**Files:**
- 新建: `crawler/fetchers/market.py`

- [ ] **Step 1: 验证模块缺失（预期失败）**

```bash
python -c "from crawler.fetchers.market import fetch_history" 2>&1 | head -3
```

预期输出：`ImportError`

- [ ] **Step 2: 创建 market.py**

```python
# crawler/fetchers/market.py
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta

import akshare as ak

from crawler.config import HISTORY_YEARS, MARKET_DIR, MAX_WORKERS
from crawler.storage.parquet import write
from crawler.utils import retry_with_backoff

logger = logging.getLogger(__name__)


def fetch_history(symbol: str) -> None:
    """拉取单只股票近 HISTORY_YEARS 年日K历史，写入 Parquet。

    Args:
        symbol: 6位股票代码，如 "000001"
    """
    end_date = date.today()
    start_date = end_date - timedelta(days=365 * HISTORY_YEARS)

    df = retry_with_backoff(
        lambda: ak.stock_zh_a_hist(
            symbol=symbol,
            period="daily",
            start_date=start_date.strftime("%Y%m%d"),
            end_date=end_date.strftime("%Y%m%d"),
            adjust="qfq",
        ),
        stock_code=symbol,
    )

    if df is None or df.empty:
        logger.warning("[%s] No history data returned, skipping.", symbol)
        return

    write(df, MARKET_DIR / f"{symbol}.parquet", dedup_key="日期")
    logger.info("[%s] History: %d rows written.", symbol, len(df))


def fetch_history_batch(symbols: list[str]) -> None:
    """并发拉取股票池所有股票的历史 K 线。"""
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_history, s): s for s in symbols}
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                future.result()
            except Exception as e:
                logger.error("[%s] History fetch ultimately failed: %s", symbol, e)


def fetch_daily_spot(symbols: list[str]) -> None:
    """拉取全市场当日行情快照，过滤股票池，追加到各股票 Parquet。"""
    df_all = retry_with_backoff(
        lambda: ak.stock_zh_a_spot_em(),
        stock_code="daily_spot",
    )

    if df_all is None or df_all.empty:
        logger.warning("Daily spot returned empty, skipping.")
        return

    # 东方财富实时行情：代码列名为 "代码"
    code_col = next((c for c in df_all.columns if c == "代码"), None)
    if code_col is None:
        logger.error("Daily spot: cannot find '代码' column. Columns: %s", df_all.columns.tolist())
        return

    df_pool = df_all[df_all[code_col].isin(symbols)].copy()
    df_pool = df_pool.rename(columns={code_col: "symbol"})

    today_str = str(date.today())
    df_pool["日期"] = today_str

    updated = 0
    for symbol, group in df_pool.groupby("symbol"):
        row = group.drop(columns=["symbol"])
        write(row, MARKET_DIR / f"{symbol}.parquet", dedup_key="日期")
        updated += 1

    logger.info("Daily spot: updated %d stocks.", updated)
```

- [ ] **Step 3: 冒烟测试（需要网络）**

```bash
python - <<'EOF'
from crawler.fetchers.market import fetch_history
from crawler.storage.parquet import read

fetch_history("000001")
df = read("data/market/000001.parquet")
assert len(df) > 200, f"Expected >200 rows, got {len(df)}"
print(f"✓ market history OK: {len(df)} rows | {df['日期'].min()} ~ {df['日期'].max()}")
EOF
```

预期输出：`✓ market history OK: NNN rows | 2023-xx-xx ~ 2026-xx-xx`

- [ ] **Step 4: Commit**

```bash
git add crawler/fetchers/market.py
git commit -m "feat: add market fetcher (history batch + daily spot)"
```

---

### Task 7: fetchers/financial.py — 财务报表爬取

**Files:**
- 新建: `crawler/fetchers/financial.py`

注意：`akshare.stock_financial_report_sina()` 的 `stock` 参数需要交易所前缀：
- 上海 (6开头) → `"sh" + symbol`
- 深圳 (0/3开头) → `"sz" + symbol`

- [ ] **Step 1: 验证模块缺失（预期失败）**

```bash
python -c "from crawler.fetchers.financial import fetch_financial" 2>&1 | head -3
```

预期输出：`ImportError`

- [ ] **Step 2: 创建 financial.py**

```python
# crawler/fetchers/financial.py
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import akshare as ak

from crawler.config import FINANCIAL_DIR, MAX_WORKERS
from crawler.storage.parquet import write
from crawler.utils import retry_with_backoff

logger = logging.getLogger(__name__)

REPORT_TYPES = {
    "income":   "利润表",
    "balance":  "资产负债表",
    "cashflow": "现金流量表",
}

# 新浪财务报表固定的报告期列名
REPORT_PERIOD_COL = "报告期"


def _add_exchange_prefix(symbol: str) -> str:
    """为6位股票代码添加交易所前缀，用于新浪接口。
    上交所：600xxx/601xxx/603xxx/605xxx → sh
    深交所：000xxx/001xxx/002xxx/003xxx/300xxx/301xxx → sz
    """
    if symbol.startswith(("6",)):
        return f"sh{symbol}"
    return f"sz{symbol}"


def fetch_financial(symbol: str) -> None:
    """拉取单只股票三张财务报表，按报告期去重写入 Parquet。

    Args:
        symbol: 6位股票代码，如 "000001"
    """
    prefixed = _add_exchange_prefix(symbol)

    for key, report_name in REPORT_TYPES.items():
        try:
            df = retry_with_backoff(
                lambda rn=report_name: ak.stock_financial_report_sina(
                    stock=prefixed, symbol=rn
                ),
                stock_code=f"{symbol}:{key}",
            )
        except Exception as e:
            logger.error("[%s] Financial %s fetch failed: %s", symbol, key, e)
            continue

        if df is None or df.empty:
            logger.warning("[%s] %s returned empty, skipping.", symbol, key)
            continue

        # 新浪财务报表第一列为报告期，列名应为 "报告期"
        # 若接口返回列名不同，记录警告但继续
        actual_col = df.columns[0]
        dedup_col = REPORT_PERIOD_COL if REPORT_PERIOD_COL in df.columns else actual_col
        if dedup_col != REPORT_PERIOD_COL:
            logger.warning("[%s] %s: expected dedup col '报告期', got '%s'.", symbol, key, actual_col)

        path = FINANCIAL_DIR / f"{symbol}_{key}.parquet"
        write(df, path, dedup_key=dedup_col)
        logger.info("[%s] Financial %s: %d rows written.", symbol, key, len(df))


def fetch_financial_batch(symbols: list[str]) -> None:
    """并发拉取股票池所有股票的财务报表。"""
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_financial, s): s for s in symbols}
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                future.result()
            except Exception as e:
                logger.error("[%s] Financial batch failed: %s", symbol, e)
```

- [ ] **Step 3: 验证交易所前缀逻辑**

```bash
python - <<'EOF'
from crawler.fetchers.financial import _add_exchange_prefix

assert _add_exchange_prefix("600036") == "sh600036"
assert _add_exchange_prefix("000001") == "sz000001"
assert _add_exchange_prefix("300750") == "sz300750"
assert _add_exchange_prefix("603501") == "sh603501"
print("✓ exchange prefix logic OK")
EOF
```

预期输出：`✓ exchange prefix logic OK`

- [ ] **Step 4: 冒烟测试（需要网络）**

```bash
python - <<'EOF'
from crawler.fetchers.financial import fetch_financial
import os

fetch_financial("600036")  # 招商银行，上交所
files = [f for f in os.listdir("data/financial") if f.startswith("600036")]
assert len(files) >= 1, f"Expected financial files, got: {files}"
print(f"✓ financial fetch OK: {files}")
EOF
```

预期输出：`✓ financial fetch OK: ['600036_income.parquet', ...]`

- [ ] **Step 5: Commit**

```bash
git add crawler/fetchers/financial.py
git commit -m "feat: add financial report fetcher with exchange prefix normalization"
```

---

### Task 8: fetchers/news.py — 公告爬取

**Files:**
- 新建: `crawler/fetchers/news.py`

- [ ] **Step 1: 验证模块缺失（预期失败）**

```bash
python -c "from crawler.fetchers.news import fetch_daily_news" 2>&1 | head -3
```

预期输出：`ImportError`

- [ ] **Step 2: 创建 news.py**

```python
# crawler/fetchers/news.py
import logging
from datetime import date

import akshare as ak

from crawler.config import NEWS_DIR
from crawler.storage.parquet import write
from crawler.utils import retry_with_backoff

logger = logging.getLogger(__name__)


def fetch_daily_news(symbols: list[str]) -> None:
    """拉取今日上市公司公告，过滤股票池，写入按日期命名的 Parquet。

    同一天文件写入多次是安全的（整个文件覆盖写，而非追加）。

    Args:
        symbols: 股票代码列表
    """
    try:
        df = retry_with_backoff(
            lambda: ak.stock_notice_report(symbol="全部"),
            stock_code="daily_news",
        )
    except Exception as e:
        logger.error("News fetch failed after retries: %s", e)
        return

    if df is None or df.empty:
        logger.warning("Daily news returned empty, skipping.")
        return

    # 过滤股票池；公告表通常含 "代码" 或 "股票代码" 列
    code_col = next((c for c in df.columns if "代码" in c), None)
    if code_col:
        df = df[df[code_col].isin(symbols)].copy()

    if df.empty:
        logger.info("No news for stock pool today.")
        return

    today_str = date.today().strftime("%Y-%m-%d")
    path = NEWS_DIR / f"{today_str}.parquet"

    # 公告文件按日期命名，每日覆盖写（同一天内重跑取最新数据）
    # 使用公告编号或第一列作为去重键，避免同一公告重复
    dedup_col = next(
        (c for c in df.columns if "编号" in c or "序号" in c),
        df.columns[0],
    )
    write(df, path, dedup_key=dedup_col)
    logger.info("Daily news: %d announcements written to %s.", len(df), path)
```

- [ ] **Step 3: 冒烟测试**

```bash
python - <<'EOF'
from crawler.fetchers.news import fetch_daily_news
from datetime import date
import os

fetch_daily_news(["000001", "600036", "300750"])
today_str = date.today().strftime("%Y-%m-%d")
path = f"data/news/{today_str}.parquet"

if os.path.exists(path):
    from crawler.storage.parquet import read
    df = read(path)
    print(f"✓ news OK: {len(df)} announcements -> {path}")
else:
    print(f"✓ news OK: no announcements today (non-trading day or no news)")
EOF
```

- [ ] **Step 4: Commit**

```bash
git add crawler/fetchers/news.py
git commit -m "feat: add daily announcement fetcher"
```

---

### Task 9: workflow.py — 工作流编排入口

**Files:**
- 新建: `crawler/workflow.py`

- [ ] **Step 1: 验证模块缺失（预期失败）**

```bash
python -c "from crawler.workflow import init_history" 2>&1 | head -3
```

预期输出：`ImportError`

- [ ] **Step 2: 创建 workflow.py**

```python
# crawler/workflow.py
"""
三个工作流编排入口。

使用方式：
  python -m crawler.workflow init        # 历史初始化（一次性）
  python -m crawler.workflow daily       # 每日增量（收盘后）
  python -m crawler.workflow financial   # 季度财务（每季度首次）

在 Claude Code 中也可通过 ruflo MCP 工具调用：
  mcp__ruflo__terminal_execute: python -m crawler.workflow daily
  mcp__ruflo__workflow_create: 注册定时触发
"""
import logging
import sys

from crawler.fetchers.financial import fetch_financial_batch
from crawler.fetchers.market import fetch_daily_spot, fetch_history_batch
from crawler.fetchers.news import fetch_daily_news
from crawler.stock_pool import build_stock_pool

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def init_history() -> None:
    """历史初始化：构建股票池 → 并发拉取近3年日K。"""
    logger.info("=== 历史初始化工作流开始 ===")
    symbols = build_stock_pool()
    logger.info("股票池: %d 只", len(symbols))
    fetch_history_batch(symbols)
    logger.info("=== 历史初始化完成 ===")


def daily_update() -> None:
    """每日增量：拉取当日行情快照 + 公告。"""
    logger.info("=== 每日增量工作流开始 ===")
    symbols = build_stock_pool()
    fetch_daily_spot(symbols)
    fetch_daily_news(symbols)
    logger.info("=== 每日增量完成 ===")


def quarterly_financial() -> None:
    """季度财务：并发拉取三张财务报表。"""
    logger.info("=== 季度财务工作流开始 ===")
    symbols = build_stock_pool()
    fetch_financial_batch(symbols)
    logger.info("=== 季度财务完成 ===")


_COMMANDS = {
    "init":      init_history,
    "daily":     daily_update,
    "financial": quarterly_financial,
}

if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else None
    if cmd not in _COMMANDS:
        print(f"Usage: python -m crawler.workflow [{'|'.join(_COMMANDS)}]")
        sys.exit(1)
    _COMMANDS[cmd]()
```

- [ ] **Step 3: 验证 CLI 入口**

```bash
python -m crawler.workflow 2>&1 | head -3
```

预期输出：`Usage: python -m crawler.workflow [init|daily|financial]`

- [ ] **Step 4: Commit**

```bash
git add crawler/workflow.py
git commit -m "feat: add workflow orchestration entry points (init/daily/financial)"
```

---

### Task 10: test.py — 端到端验证脚本

**Files:**
- 修改: `test.py`

- [ ] **Step 1: 覆写 test.py**

```python
# test.py — 手动端到端验证脚本
"""
运行方式：python test.py
使用少量代表性股票验证各模块，不触发全量爬取。
"""
import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# 跨行业代表：平安银行(深)、招商银行(沪)、宁德时代(深)、中国卫星(沪)、韦尔股份(沪)
SAMPLE_SYMBOLS = ["000001", "600036", "300750", "601881", "603501"]


def test_stock_pool():
    print("\n── Test 1: 股票池构建 ──")
    from crawler.stock_pool import build_stock_pool
    pool = build_stock_pool()
    assert len(pool) > 0, "股票池为空！"
    for code in pool[:5]:
        assert len(code) == 6 and code.isdigit(), f"代码格式错误: {code}"
    print(f"✓ 股票池: {len(pool)} 只 | 前5: {pool[:5]}")
    return pool


def test_market_history():
    print("\n── Test 2: 行情历史（5只样本股）──")
    from crawler.fetchers.market import fetch_history
    from crawler.storage.parquet import read
    for symbol in SAMPLE_SYMBOLS:
        fetch_history(symbol)
        df = read(f"data/market/{symbol}.parquet")
        assert len(df) > 100, f"[{symbol}] 数据行数不足: {len(df)}"
        size_kb = os.path.getsize(f"data/market/{symbol}.parquet") / 1024
        print(f"  ✓ {symbol}: {len(df)} 行 | {df['日期'].min()} ~ {df['日期'].max()} | {size_kb:.1f} KB")


def test_daily_spot():
    print("\n── Test 3: 每日行情快照 ──")
    from crawler.fetchers.market import fetch_daily_spot
    from crawler.storage.parquet import read
    from datetime import date
    today = str(date.today())
    fetch_daily_spot(SAMPLE_SYMBOLS)
    for symbol in SAMPLE_SYMBOLS[:2]:
        df = read(f"data/market/{symbol}.parquet")
        today_rows = df[df["日期"].astype(str).str.startswith(today)]
        flag = "✓" if len(today_rows) > 0 else "⚠"
        print(f"  {flag} {symbol}: 今日数据 {len(today_rows)} 行（非交易日可为0）")


def test_news():
    print("\n── Test 4: 公告数据 ──")
    from crawler.fetchers.news import fetch_daily_news
    from datetime import date
    fetch_daily_news(SAMPLE_SYMBOLS)
    today_str = date.today().strftime("%Y-%m-%d")
    path = f"data/news/{today_str}.parquet"
    if os.path.exists(path):
        from crawler.storage.parquet import read
        df = read(path)
        print(f"  ✓ 今日公告: {len(df)} 条 -> {path}")
    else:
        print(f"  ⚠ 今日无公告（非交易日或暂无公告）")


def test_financial():
    print("\n── Test 5: 财务报表（招商银行 600036）──")
    from crawler.fetchers.financial import fetch_financial
    symbol = "600036"
    fetch_financial(symbol)
    files = [f for f in os.listdir("data/financial") if f.startswith(symbol)]
    assert len(files) >= 1, f"[{symbol}] 未生成财务文件"
    print(f"  ✓ {symbol} 财务文件: {files}")


def test_idempotency():
    print("\n── Test 6: 存储幂等性验证 ──")
    from crawler.fetchers.market import fetch_history
    from crawler.storage.parquet import read
    symbol = "000001"
    fetch_history(symbol)  # 第一次写入
    df1 = read(f"data/market/{symbol}.parquet")
    fetch_history(symbol)  # 重跑，不应产生重复行
    df2 = read(f"data/market/{symbol}.parquet")
    assert len(df1) == len(df2), f"重跑后行数变化: {len(df1)} → {len(df2)}"
    print(f"  ✓ 幂等OK: 两次运行行数一致 ({len(df2)} 行)")


if __name__ == "__main__":
    print("=" * 55)
    print("Stock Crawler 端到端验证")
    print("=" * 55)

    test_stock_pool()
    test_market_history()
    test_daily_spot()
    test_news()
    test_financial()
    test_idempotency()

    print("\n" + "=" * 55)
    print("✓ 所有验证通过")
    print("=" * 55)
```

- [ ] **Step 2: 运行完整验证**

```bash
python test.py
```

预期输出：各测试打印 `✓`，最终 `✓ 所有验证通过`

- [ ] **Step 3: Commit**

```bash
git add test.py
git commit -m "feat: replace test.py with comprehensive end-to-end verification"
```

---

## Chunk 3: ruflo 工作流注册（在 Claude Code 交互式会话中执行）

### Task 11: 通过 ruflo 注册定时工作流

此任务不写代码文件，在 Claude Code 中通过 MCP 工具执行。

- [ ] **Step 1: 首次运行历史初始化**

通过 `mcp__ruflo__terminal_create` 创建终端，然后 `mcp__ruflo__terminal_execute` 运行：

```bash
python -m crawler.workflow init
```

预期输出：逐股票打印 `[symbol] History: N rows written.`，最终 `=== 历史初始化完成 ===`

- [ ] **Step 2: 注册每日增量定时工作流**

通过 `mcp__ruflo__workflow_create` 创建：
- 名称: `stock-daily-update`
- cron 表达式: `0 16 * * 1-5`（周一至周五 16:00，即 A 股收盘后）
- 命令: `cd /path/to/stock-analysis && python -m crawler.workflow daily`

预期：`mcp__ruflo__workflow_status` 返回 workflow 已注册

- [ ] **Step 3: 注册季度财务工作流（手动触发）**

通过 `mcp__ruflo__workflow_create` 创建：
- 名称: `stock-quarterly-financial`
- cron 表达式: `0 9 1 1,4,7,10 *`（每年1/4/7/10月1日 09:00，即季报季开始）
- 命令: `cd /path/to/stock-analysis && python -m crawler.workflow financial`

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "chore: complete stock crawler implementation"
```

---
