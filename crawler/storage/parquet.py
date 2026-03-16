import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def write(df: pd.DataFrame, path: str | Path, dedup_key: str = "日期") -> None:
    """幂等写入 Parquet。若文件已存在则合并去重后写回。

    Args:
        df: 待写入的 DataFrame
        path: 目标文件路径（.parquet）
        dedup_key: 去重列名；行情数据用 "日期"，财务数据用 "报告期"
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    if df is None or df.empty:
        logger.warning("write() called with empty DataFrame, skipping: %s", path)
        return

    if path.exists():
        existing = pd.read_parquet(path)
        combined = pd.concat([existing, df], ignore_index=True)
    else:
        combined = df.copy()

    if dedup_key in combined.columns:
        # 统一转为字符串后去重，避免类型不一致导致的重复保留
        combined[dedup_key] = combined[dedup_key].astype(str)
        combined = combined.drop_duplicates(subset=[dedup_key], keep="last")
        combined = combined.sort_values(dedup_key).reset_index(drop=True)

    combined.to_parquet(path, index=False)
    logger.debug("Wrote %d rows to %s", len(combined), path)


def read(
    path: str | Path,
    start: str | None = None,
    end: str | None = None,
    date_col: str = "日期",
) -> pd.DataFrame:
    """读取 Parquet，可按日期范围过滤。

    Args:
        path: 文件路径
        start: 起始日期字符串，如 "2024-01-01"（含）
        end: 结束日期字符串，如 "2024-12-31"（含）
        date_col: 日期列名
    Returns:
        DataFrame；文件不存在时返回空 DataFrame
    """
    path = Path(path)
    if not path.exists():
        return pd.DataFrame()

    df = pd.read_parquet(path)

    if (start or end) and date_col in df.columns:
        df[date_col] = pd.to_datetime(df[date_col])
        if start:
            df = df[df[date_col] >= pd.to_datetime(start)]
        if end:
            df = df[df[date_col] <= pd.to_datetime(end)]

    return df.reset_index(drop=True)
