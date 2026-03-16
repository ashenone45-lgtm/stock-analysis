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

    try:
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
    except Exception as e:
        logger.error("[%s] History fetch failed: %s", symbol, e)
        return

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
