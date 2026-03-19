"""
数据健康检查模块

扫描 data/market/ 各文件最新日期，检测是否有文件落后当前交易日超1天。
由 workflow.py daily_update() 末尾调用。
"""
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

MARKET_DIR = Path(__file__).parent.parent / "data" / "market"


def run_health_check(warn_threshold: int = 10) -> dict:
    """检查数据健康状态。

    单次遍历所有 parquet 文件：先找到全局最新日期，再标记落后文件。

    Args:
        warn_threshold: 落后文件数量超过此值时打印警告（默认 10）。

    Returns:
        {
            "latest_date": str,      # 最新交易日
            "stale_count": int,      # 落后文件数量
            "stale_files": list[str] # 落后文件名（最多20个）
        }
    """
    if not MARKET_DIR.exists():
        logger.warning("数据目录不存在: %s", MARKET_DIR)
        return {"latest_date": None, "stale_count": 0, "stale_files": []}

    # 单次遍历：读每个文件的最新日期，同时记录全局最大值
    file_dates: dict[str, str] = {}
    latest_date = None
    for f in MARKET_DIR.glob("*.parquet"):
        try:
            tmp = pd.read_parquet(f, columns=["日期"])
            tmp["日期"] = pd.to_datetime(tmp["日期"]).dt.strftime("%Y-%m-%d")
            d = tmp["日期"].max()
            file_dates[f.stem] = d
            if latest_date is None or d > latest_date:
                latest_date = d
        except Exception as e:
            logger.debug("无法读取 %s: %s", f.name, e)

    if latest_date is None:
        latest_date = datetime.today().strftime("%Y-%m-%d")

    # 第二步在内存中完成，无额外磁盘 I/O
    stale_files = [stem for stem, d in file_dates.items() if d < latest_date]
    total_checked = len(file_dates)

    stale_count = len(stale_files)
    result = {
        "latest_date": latest_date,
        "stale_count": stale_count,
        "stale_files": stale_files[:20],
    }

    if stale_count > warn_threshold:
        logger.warning(
            "⚠️ 数据健康检查：%d / %d 只股票数据落后于最新交易日 %s（前20: %s）",
            stale_count, total_checked, latest_date,
            ", ".join(stale_files[:20]),
        )
    else:
        logger.info(
            "数据健康检查通过：最新交易日 %s，落后文件 %d 个",
            latest_date, stale_count,
        )

    return result
