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
