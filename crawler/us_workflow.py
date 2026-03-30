"""
美股工作流编排入口。

使用方式：
  python -m crawler.us_workflow init    # 历史初始化（一次性）
  python -m crawler.us_workflow daily   # 每日增量（收盘后）
"""
import csv
import logging
import os
import sys
from pathlib import Path

import pandas as pd

os.environ.setdefault("ARROW_DEFAULT_MEMORY_POOL", "system")

from crawler.fetchers.us_market import fetch_daily_spot, fetch_history_batch
from crawler.us_config import US_STOCK_POOL, get_all_codes

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
CODE_MAP_PATH = DATA_DIR / "us_code_map.csv"


def _write_csv_files() -> None:
    """生成 data/us_names.csv 和 data/us_industries.csv（供 gen_report 加载）"""
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    names_path = DATA_DIR / "us_names.csv"
    with names_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["code", "name"])
        for stocks in US_STOCK_POOL.values():
            for code, name in stocks:
                writer.writerow([code, name])
    logger.info("已写入 %s", names_path)

    industries_path = DATA_DIR / "us_industries.csv"
    with industries_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["代码", "行业"])
        for industry, stocks in US_STOCK_POOL.items():
            for code, _ in stocks:
                writer.writerow([code, industry])
    logger.info("已写入 %s", industries_path)


def _write_code_map() -> None:
    """从 akshare stock_us_spot_em() 拉取全量美股代码，保存 ticker→full_code 映射。

    akshare stock_us_hist() 要求 symbol 格式为 '105.AAPL' 而非裸 ticker，
    此函数将映射关系持久化到 data/us_code_map.csv，供爬取时使用。
    """
    import akshare as ak
    logger.info("正在从 akshare 拉取美股代码列表（需 2~3 分钟）...")
    spot_df = ak.stock_us_spot_em()
    # 代码字段形如 "105.AAPL"，ticker 是点号后半部分
    spot_df["ticker"] = spot_df["代码"].str.split(".").str[-1]
    # 只保留我们关心的 ticker，去重（同 ticker 取第一条）
    our_tickers = set(get_all_codes())
    filtered = (
        spot_df[spot_df["ticker"].isin(our_tickers)]
        [["ticker", "代码"]]
        .drop_duplicates(subset=["ticker"])
    )
    missing = our_tickers - set(filtered["ticker"])
    if missing:
        logger.warning("以下 ticker 在 spot_em 列表中未找到，尝试前缀探测: %s", sorted(missing))
        import akshare as _ak_probe
        import datetime as _dt
        probed_rows = []
        today = _dt.date.today()
        start_s = (today - _dt.timedelta(days=7)).strftime("%Y%m%d")
        end_s = today.strftime("%Y%m%d")
        for ticker in sorted(missing):
            found = False
            for prefix in ("105", "106", "107"):
                candidate = f"{prefix}.{ticker}"
                try:
                    df = _ak_probe.stock_us_hist(symbol=candidate, period="daily",
                                                  start_date=start_s, end_date=end_s, adjust="qfq")
                    if df is not None and not df.empty:
                        probed_rows.append({"ticker": ticker, "代码": candidate})
                        logger.info("[%s] 探测成功: %s", ticker, candidate)
                        found = True
                        break
                except Exception:
                    pass
            if not found:
                logger.warning("[%s] 前缀探测也未找到，将跳过该 ticker", ticker)
        if probed_rows:
            probed_df = pd.DataFrame(probed_rows)
            filtered = pd.concat([filtered, probed_df], ignore_index=True)

    with CODE_MAP_PATH.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["ticker", "code"])
        for _, row in filtered.iterrows():
            writer.writerow([row["ticker"], row["代码"]])
    logger.info("代码映射已写入 %s（%d 条）", CODE_MAP_PATH, len(filtered))


def _ensure_code_map() -> None:
    """如果 code map 不存在则自动拉取。"""
    if not CODE_MAP_PATH.exists():
        logger.info("us_code_map.csv 不存在，自动生成...")
        _write_code_map()


def init_history() -> None:
    """历史初始化：生成 CSV → 拉取代码映射 → 并发拉取近3年日K。"""
    logger.info("=== 美股历史初始化工作流开始 ===")
    _write_csv_files()
    _write_code_map()
    symbols = get_all_codes()
    logger.info("美股票池: %d 只", len(symbols))
    fetch_history_batch(symbols)
    logger.info("=== 美股历史初始化完成 ===")


def daily_update() -> None:
    """每日增量：确保 CSV 存在 → 拉取当日行情。"""
    logger.info("=== 美股每日增量工作流开始 ===")
    names_path = DATA_DIR / "us_names.csv"
    if not names_path.exists():
        logger.info("us_names.csv 不存在，自动生成...")
        _write_csv_files()
    _ensure_code_map()
    symbols = get_all_codes()
    if not symbols:
        raise RuntimeError("US stock pool is empty — aborting daily_update.")
    fetch_daily_spot(symbols)
    logger.info("=== 美股每日增量完成 ===")


_COMMANDS = {
    "init":  init_history,
    "daily": daily_update,
}

if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else None
    if cmd not in _COMMANDS:
        print(f"Usage: python -m crawler.us_workflow [{'|'.join(_COMMANDS)}]")
        sys.exit(1)
    try:
        _COMMANDS[cmd]()
    except RuntimeError as e:
        logger.error("%s", e)
        sys.exit(1)
