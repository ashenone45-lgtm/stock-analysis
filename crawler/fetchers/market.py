import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta

import akshare as ak
import pandas as pd
from tqdm import tqdm

from crawler.config import HISTORY_YEARS, MARKET_DIR, MAX_WORKERS
from crawler.storage.parquet import write
from crawler.utils import retry_with_backoff

logger = logging.getLogger(__name__)


def _exchange_prefix(symbol: str) -> str:
    """为6位代码加交易所前缀：6开头→sh，其余→sz"""
    return f"sh{symbol}" if symbol.startswith("6") else f"sz{symbol}"


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    """将 stock_zh_a_daily 英文列名转为中文，补充涨跌幅/涨跌额/振幅列"""
    df = df.rename(columns={
        "date":   "日期",
        "open":   "开盘",
        "high":   "最高",
        "low":    "最低",
        "close":  "收盘",
        "volume": "成交量",
        "amount": "成交额",
        "turnover": "换手率",
    }).drop(columns=["outstanding_share"], errors="ignore")

    df["日期"] = pd.to_datetime(df["日期"]).dt.strftime("%Y-%m-%d")
    df["换手率"] = (df["换手率"] * 100).round(4)          # 小数→百分比
    df["涨跌额"] = df["收盘"].diff().round(4)
    df["涨跌幅"] = (df["收盘"].pct_change() * 100).round(4)
    prev_close = df["收盘"].shift(1)
    df["振幅"] = ((df["最高"] - df["最低"]) / prev_close * 100).round(4)
    return df


def fetch_history(symbol: str) -> None:
    """拉取单只股票近 HISTORY_YEARS 年日K历史，写入 Parquet。"""
    end_date = date.today()
    start_date = end_date - timedelta(days=365 * HISTORY_YEARS)

    try:
        df = retry_with_backoff(
            lambda: ak.stock_zh_a_daily(
                symbol=_exchange_prefix(symbol),
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

    df = _normalize(df)
    write(df, MARKET_DIR / f"{symbol}.parquet", dedup_key="日期")
    logger.info("[%s] History: %d rows written.", symbol, len(df))


def fetch_history_batch(symbols: list[str]) -> None:
    """并发拉取股票池所有股票的历史 K 线。"""
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_history, s): s for s in symbols}
        with tqdm(total=len(symbols), desc="拉取历史K线", unit="只") as pbar:
            for future in as_completed(futures):
                symbol = futures[future]
                try:
                    future.result()
                except Exception as e:
                    logger.error("[%s] History fetch ultimately failed: %s", symbol, e)
                pbar.update(1)


def fetch_daily_spot(symbols: list[str]) -> None:
    """拉取最近行情，追加到各股票 Parquet。使用新浪财经接口。

    写入最近 5 日内的所有可用 K 线（dedup_key 防重），不强制要求"必须是今天"。
    新浪接口在收盘后有延迟，严格过滤今天会导致什么都写不进去。
    """
    today = date.today()
    # 多取几天保证 diff() 有参照，也兼容收盘延迟
    start = (today - timedelta(days=7)).strftime("%Y%m%d")
    end = today.strftime("%Y%m%d")

    updated = 0

    def _fetch_one(symbol: str) -> bool:
        try:
            df = retry_with_backoff(
                lambda s=symbol: ak.stock_zh_a_daily(
                    symbol=_exchange_prefix(s),
                    start_date=start, end_date=end, adjust="qfq",
                ),
                stock_code=symbol,
            )
        except Exception as e:
            logger.error("[%s] Daily spot failed: %s", symbol, e)
            return False
        if df is None or df.empty:
            return False
        df = _normalize(df)
        if df.empty:
            return False
        # 写入所有拿到的行；dedup_key="日期" 自动去重，旧数据不会重复
        write(df, MARKET_DIR / f"{symbol}.parquet", dedup_key="日期")
        return True

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(_fetch_one, s): s for s in symbols}
        with tqdm(total=len(symbols), desc="拉取每日行情", unit="只") as pbar:
            for future in as_completed(futures):
                try:
                    if future.result():
                        updated += 1
                except Exception as e:
                    logger.error("[%s] Unexpected error: %s", futures[future], e)
                pbar.update(1)

    today_str = today.strftime("%Y-%m-%d")
    # 抽查几个文件，确认最新日期
    sample = list(MARKET_DIR.glob("*.parquet"))[:10]  # 抽查前10只，快速验证日期
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
            "Daily spot: updated %d / %d stocks. Latest date in files: %s "
            "(today=%s — market may not have closed yet or data is delayed).",
            updated, len(symbols), latest, today_str,
        )
    else:
        logger.info("Daily spot: updated %d / %d stocks. Latest date: %s.",
                    updated, len(symbols), latest)
