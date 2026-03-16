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
