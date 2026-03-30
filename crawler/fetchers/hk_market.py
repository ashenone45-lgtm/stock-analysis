# crawler/fetchers/hk_market.py
"""港股行情爬取，使用 akshare stock_hk_hist() 接口"""
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta

import akshare as ak
import pandas as pd
from tqdm import tqdm

from crawler.storage.parquet import write
from crawler.utils import retry_with_backoff

logger = logging.getLogger(__name__)

HK_MARKET_DIR_NAME = "hk_market"
MAX_WORKERS = 3  # 港交所 API 同样有限流，保守取 3
HISTORY_YEARS = 3


def _get_hk_market_dir():
    from pathlib import Path
    return Path(__file__).parent.parent.parent / "data" / HK_MARKET_DIR_NAME


def fetch_history(symbol: str) -> None:
    """拉取单只港股近 HISTORY_YEARS 年日K历史，写入 Parquet。"""
    end_date = date.today()
    start_date = end_date - timedelta(days=365 * HISTORY_YEARS)
    market_dir = _get_hk_market_dir()

    try:
        df = retry_with_backoff(
            lambda: ak.stock_hk_hist(
                symbol=symbol,
                period="daily",
                start_date=start_date.strftime("%Y%m%d"),
                end_date=end_date.strftime("%Y%m%d"),
                adjust="qfq",
            ),
            stock_code=symbol,
        )
    except Exception as e:
        logger.error("[%s] HK history fetch failed: %s", symbol, e)
        return

    if df is None or df.empty:
        logger.warning("[%s] No HK history data returned, skipping.", symbol)
        return

    df = _normalize(df)
    write(df, market_dir / f"{symbol}.parquet", dedup_key="日期")
    logger.info("[%s] HK history: %d rows written.", symbol, len(df))


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    """stock_hk_hist() 返回列名已是中文，统一格式化日期和数值。"""
    # 列名映射（兼容可能的英文列名）
    rename_map = {
        "date": "日期",
        "open": "开盘",
        "close": "收盘",
        "high": "最高",
        "low": "最低",
        "volume": "成交量",
        "amount": "成交额",
        "amplitude": "振幅",
        "pct_chg": "涨跌幅",
        "change": "涨跌额",
        "turnover_rate": "换手率",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    if "日期" in df.columns:
        df["日期"] = pd.to_datetime(df["日期"]).dt.strftime("%Y-%m-%d")

    return df


def fetch_history_batch(symbols: list[str]) -> None:
    """并发拉取港股票池所有股票的历史 K 线。"""
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_history, s): s for s in symbols}
        with tqdm(total=len(symbols), desc="拉取港股历史K线", unit="只") as pbar:
            for future in as_completed(futures):
                symbol = futures[future]
                try:
                    future.result()
                except Exception as e:
                    logger.error("[%s] HK history fetch ultimately failed: %s", symbol, e)
                pbar.update(1)


def fetch_daily_spot(symbols: list[str]) -> None:
    """拉取港股最近行情，追加到各股票 Parquet。"""
    today = date.today()
    start = (today - timedelta(days=7)).strftime("%Y%m%d")
    end = today.strftime("%Y%m%d")
    market_dir = _get_hk_market_dir()

    updated = 0

    def _fetch_one(symbol: str) -> bool:
        try:
            df = retry_with_backoff(
                lambda s=symbol: ak.stock_hk_hist(
                    symbol=s,
                    period="daily",
                    start_date=start,
                    end_date=end,
                    adjust="qfq",
                ),
                stock_code=symbol,
            )
        except Exception as e:
            logger.error("[%s] HK daily spot failed: %s", symbol, e)
            return False
        if df is None or df.empty:
            return False
        df = _normalize(df)
        if df.empty:
            return False
        write(df, market_dir / f"{symbol}.parquet", dedup_key="日期")
        return True

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(_fetch_one, s): s for s in symbols}
        with tqdm(total=len(symbols), desc="拉取港股每日行情", unit="只") as pbar:
            for future in as_completed(futures):
                try:
                    if future.result():
                        updated += 1
                except Exception as e:
                    logger.error("[%s] HK unexpected error: %s", futures[future], e)
                pbar.update(1)

    today_str = today.strftime("%Y-%m-%d")
    sample = list(market_dir.glob("*.parquet"))[:10]
    latest_dates = set()
    for f in sample:
        try:
            tmp = pd.read_parquet(f, columns=["日期"])
            latest_dates.add(tmp["日期"].max())
        except Exception:
            pass
    latest = max(latest_dates) if latest_dates else "unknown"
    if latest < today_str:
        logger.warning(
            "HK daily spot: updated %d / %d stocks. Latest date in files: %s "
            "(today=%s — market may not have closed yet or data is delayed).",
            updated, len(symbols), latest, today_str,
        )
    else:
        logger.info("HK daily spot: updated %d / %d stocks. Latest date: %s.",
                    updated, len(symbols), latest)
