# crawler/fetchers/us_market.py
"""美股行情爬取，使用 akshare stock_us_hist() 接口"""
import csv
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path

import akshare as ak
import pandas as pd
from tqdm import tqdm

from crawler.storage.parquet import write
from crawler.utils import retry_with_backoff

logger = logging.getLogger(__name__)

US_MARKET_DIR_NAME = "us_market"
MAX_WORKERS = 3  # 保守设置，避免频繁限流
HISTORY_YEARS = 3

_CODE_MAP_PATH = Path(__file__).parent.parent.parent / "data" / "us_code_map.csv"
_code_map_lock = threading.Lock()


def _get_us_market_dir() -> Path:
    return Path(__file__).parent.parent.parent / "data" / US_MARKET_DIR_NAME


def _load_code_map() -> dict[str, str]:
    """加载 data/us_code_map.csv，返回 {ticker: full_code}。

    akshare stock_us_hist() 要求 symbol='105.AAPL'，不能用裸 ticker。
    映射表由 us_workflow._write_code_map() 生成。
    """
    if not _CODE_MAP_PATH.exists():
        logger.warning("us_code_map.csv 不存在，将直接使用裸 ticker（可能导致爬取失败）。"
                       "请先运行: python -m crawler.us_workflow init")
        return {}
    result: dict[str, str] = {}
    with _CODE_MAP_PATH.open(encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            result[row["ticker"]] = row["code"]
    return result


def _ak_us_hist(code: str, start_date: str, end_date: str) -> "pd.DataFrame | None":
    """调用 akshare stock_us_hist，捕获 TypeError（API 返回 None）直接返回 None。

    akshare 内部在解析空响应时会抛 'NoneType' object is not subscriptable，
    这是确定性失败，不应触发重试逻辑，故在此处提前截获。
    code 格式必须为 '105.AAPL'（含市场前缀）。
    """
    try:
        return ak.stock_us_hist(
            symbol=code,
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust="qfq",
        )
    except TypeError:
        return None


def _probe_full_code(symbol: str) -> str | None:
    """当 code_map 未收录时，依次尝试 105/106/107 前缀，返回能拿到数据的完整代码。"""
    today = date.today().strftime("%Y%m%d")
    yesterday = (date.today() - timedelta(days=7)).strftime("%Y%m%d")
    for prefix in ("105", "106", "107"):
        candidate = f"{prefix}.{symbol}"
        try:
            df = ak.stock_us_hist(
                symbol=candidate,
                period="daily",
                start_date=yesterday,
                end_date=today,
                adjust="qfq",
            )
            if df is not None and not df.empty:
                logger.info("[%s] 探测到代码: %s", symbol, candidate)
                return candidate
        except Exception:
            pass
    return None


def fetch_history(symbol: str, code_map: dict[str, str]) -> None:
    """拉取单只美股近 HISTORY_YEARS 年日K历史，写入 Parquet。"""
    end_date = date.today()
    start_date = end_date - timedelta(days=365 * HISTORY_YEARS)
    market_dir = _get_us_market_dir()

    full_code = code_map.get(symbol)
    if not full_code:
        full_code = _probe_full_code(symbol)
        if not full_code:
            logger.warning("[%s] 未在代码映射表中找到，跳过。", symbol)
            return
        with _code_map_lock:
            code_map[symbol] = full_code

    try:
        df = retry_with_backoff(
            lambda: _ak_us_hist(
                full_code,
                start_date.strftime("%Y%m%d"),
                end_date.strftime("%Y%m%d"),
            ),
            stock_code=symbol,
        )
    except Exception as e:
        logger.error("[%s] US history fetch failed: %s", symbol, e)
        return

    if df is None or df.empty:
        logger.warning("[%s] No US history data returned, skipping.", symbol)
        return

    df = _normalize(df)
    write(df, market_dir / f"{symbol}.parquet", dedup_key="日期")
    logger.info("[%s] US history: %d rows written.", symbol, len(df))


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    """stock_us_hist() 返回列名已是中文，统一格式化日期和数值。"""
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

    # 换手率对部分 ETF 可能不存在，补 0 避免下游报错
    if "换手率" not in df.columns:
        df["换手率"] = 0.0

    return df


def fetch_history_batch(symbols: list[str]) -> None:
    """并发拉取美股票池所有股票的历史 K 线。"""
    code_map = _load_code_map()
    market_dir = _get_us_market_dir()
    market_dir.mkdir(parents=True, exist_ok=True)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_history, s, code_map): s for s in symbols}
        with tqdm(total=len(symbols), desc="拉取美股历史K线", unit="只") as pbar:
            for future in as_completed(futures):
                symbol = futures[future]
                try:
                    future.result()
                except Exception as e:
                    logger.error("[%s] US history fetch ultimately failed: %s", symbol, e)
                pbar.update(1)


def fetch_daily_spot(symbols: list[str]) -> None:
    """拉取美股最近行情，追加到各股票 Parquet。"""
    today = date.today()
    start = (today - timedelta(days=7)).strftime("%Y%m%d")
    end = today.strftime("%Y%m%d")
    market_dir = _get_us_market_dir()
    market_dir.mkdir(parents=True, exist_ok=True)

    code_map = _load_code_map()
    updated = 0

    def _fetch_one(symbol: str) -> bool:
        full_code = code_map.get(symbol)
        if not full_code:
            full_code = _probe_full_code(symbol)
            if not full_code:
                logger.warning("[%s] 未在代码映射表中找到，跳过。", symbol)
                return False
            with _code_map_lock:
                code_map[symbol] = full_code
        try:
            df = retry_with_backoff(
                lambda c=full_code: _ak_us_hist(c, start, end),
                stock_code=symbol,
            )
        except Exception as e:
            logger.error("[%s] US daily spot failed: %s", symbol, e)
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
        with tqdm(total=len(symbols), desc="拉取美股每日行情", unit="只") as pbar:
            for future in as_completed(futures):
                try:
                    if future.result():
                        updated += 1
                except Exception as e:
                    logger.error("[%s] US unexpected error: %s", futures[future], e)
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
            "US daily spot: updated %d / %d stocks. Latest date in files: %s "
            "(today=%s — market may not have closed yet or data is delayed).",
            updated, len(symbols), latest, today_str,
        )
    else:
        logger.info("US daily spot: updated %d / %d stocks. Latest date: %s.",
                    updated, len(symbols), latest)
