"""共享工具：带指数退避的重试 + 符合规范的错误文件日志。"""
import logging
import time
from datetime import datetime
from pathlib import Path

from crawler.config import LOG_FILE


def _ensure_log_file() -> None:
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)


def log_error(stock_code: str, error_message: str, retry_count: int) -> None:
    """按规范格式写入错误日志：[timestamp] [stock_code] [error_message] [retry_count]"""
    _ensure_log_file()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] [{stock_code}] [{error_message}] [retry={retry_count}]\n"
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line)
    logging.getLogger(__name__).error(line.strip())


def retry_with_backoff(func, stock_code: str = "N/A", retries: int = 3):
    """执行 func()，失败时指数退避重试，最终失败写入错误日志并重新抛出异常。

    Args:
        func: 无参可调用对象
        stock_code: 用于错误日志的股票代码标识
        retries: 最大重试次数（含首次尝试）
    Returns:
        func() 的返回值
    Raises:
        最后一次异常
    """
    last_exc = None
    for attempt in range(retries):
        try:
            return func()
        except Exception as e:
            last_exc = e
            wait = 2 ** attempt
            logging.getLogger(__name__).warning(
                "[%s] Attempt %d/%d failed: %s. Retrying in %ds...",
                stock_code, attempt + 1, retries, e, wait,
            )
            if attempt < retries - 1:
                time.sleep(wait)

    log_error(stock_code, str(last_exc), retries)
    raise last_exc
