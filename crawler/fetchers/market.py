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
    """拉取当日行情，追加到各股票 Parquet。

    使用 stock_zh_a_hist（push2his 服务器）逐只并发获取，
    避免依赖 push2.eastmoney.com 的实时快照接口。
    """
    today = date.today().strftime("%Y%m%d")

    def _fetch_one(symbol: str) -> None:
        try:
            df = retry_with_backoff(
                lambda s=symbol: ak.stock_zh_a_hist(
                    symbol=s, period="daily",
                    start_date=today, end_date=today, adjust="qfq",
                ),
                stock_code=symbol,
            )
        except Exception as e:
            logger.error("[%s] Daily spot failed: %s", symbol, e)
            return
        if df is None or df.empty:
            return
        write(df, MARKET_DIR / f"{symbol}.parquet", dedup_key="日期")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(_fetch_one, s): s for s in symbols}
        done = 0
        for future in as_completed(futures):
            try:
                future.result()
                done += 1
            except Exception as e:
                logger.error("[%s] Unexpected error: %s", futures[future], e)

    logger.info("Daily spot: updated %d / %d stocks.", done, len(symbols))
