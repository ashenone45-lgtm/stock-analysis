"""
港股工作流编排入口。

使用方式：
  python -m crawler.hk_workflow init    # 历史初始化（一次性）
  python -m crawler.hk_workflow daily   # 每日增量（收盘后）
"""
import csv
import logging
import os
import sys
from pathlib import Path

os.environ.setdefault("ARROW_DEFAULT_MEMORY_POOL", "system")

from crawler.fetchers.hk_market import fetch_daily_spot, fetch_history_batch
from crawler.hk_config import HK_STOCK_POOL, get_all_codes

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"


def _write_csv_files() -> None:
    """生成 data/hk_names.csv 和 data/hk_industries.csv（供 gen_report 加载）"""
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    names_path = DATA_DIR / "hk_names.csv"
    with names_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["code", "name"])
        for stocks in HK_STOCK_POOL.values():
            for code, name in stocks:
                writer.writerow([code, name])
    logger.info("已写入 %s", names_path)

    industries_path = DATA_DIR / "hk_industries.csv"
    with industries_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["代码", "行业"])
        for industry, stocks in HK_STOCK_POOL.items():
            for code, _ in stocks:
                writer.writerow([code, industry])
    logger.info("已写入 %s", industries_path)


def init_history() -> None:
    """历史初始化：生成 CSV → 并发拉取近3年日K。"""
    logger.info("=== 港股历史初始化工作流开始 ===")
    _write_csv_files()
    symbols = get_all_codes()
    logger.info("港股票池: %d 只", len(symbols))
    fetch_history_batch(symbols)
    logger.info("=== 港股历史初始化完成 ===")


def daily_update() -> None:
    """每日增量：确保 CSV 存在 → 拉取当日行情。"""
    logger.info("=== 港股每日增量工作流开始 ===")
    # 确保 CSV 文件存在（首次 daily 时可能未 init）
    names_path = DATA_DIR / "hk_names.csv"
    if not names_path.exists():
        logger.info("hk_names.csv 不存在，自动生成...")
        _write_csv_files()
    symbols = get_all_codes()
    if not symbols:
        raise RuntimeError("HK stock pool is empty — aborting daily_update.")
    fetch_daily_spot(symbols)
    logger.info("=== 港股每日增量完成 ===")


_COMMANDS = {
    "init":  init_history,
    "daily": daily_update,
}

if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else None
    if cmd not in _COMMANDS:
        print(f"Usage: python -m crawler.hk_workflow [{'|'.join(_COMMANDS)}]")
        sys.exit(1)
    try:
        _COMMANDS[cmd]()
    except RuntimeError as e:
        logger.error("%s", e)
        sys.exit(1)
