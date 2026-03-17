"""
日K线查看工具

用法：
  python view_kline.py 600036          # 近90天
  python view_kline.py 600036 180      # 近180天
  python view_kline.py 600036 2024-01-01 2024-06-30  # 指定日期范围
"""
import io
import sys

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

import matplotlib
matplotlib.use("Agg")  # 非交互后端，避免 tkinter 字体问题
import matplotlib.pyplot as plt
matplotlib.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "Noto Sans SC"]
matplotlib.rcParams["font.family"] = "sans-serif"
matplotlib.rcParams["axes.unicode_minus"] = False

import mplfinance as mpf
import pandas as pd
from pathlib import Path

DATA_DIR = Path(__file__).parent / "data" / "market"
NAMES_CSV = Path(__file__).parent / "data" / "stock_names.csv"


def get_name(code: str) -> str:
    if not NAMES_CSV.exists():
        return ""
    df = pd.read_csv(NAMES_CSV, dtype=str)
    row = df[df["code"] == code]
    return row["name"].iloc[0] if not row.empty else ""


def load(code: str, start: str = None, end: str = None, days: int = 90) -> pd.DataFrame:
    path = DATA_DIR / f"{code}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"找不到 {path}，请先运行 init 或 daily 工作流")

    df = pd.read_parquet(path)
    df["日期"] = pd.to_datetime(df["日期"])
    df = df.set_index("日期").sort_index()
    df = df.rename(columns={"开盘": "Open", "最高": "High", "最低": "Low",
                             "收盘": "Close", "成交量": "Volume"})

    if start and end:
        df = df.loc[start:end]
    else:
        df = df.tail(days)

    return df


def plot(code: str, df: pd.DataFrame) -> None:
    name = get_name(code)
    name_str = f" {name}" if name else ""
    title = f"{code}{name_str}  日K线  {df.index[0].date()} ~ {df.index[-1].date()}  ({len(df)}天)"

    # 均线
    mav = (5, 20, 60) if len(df) >= 60 else (5, 20) if len(df) >= 20 else (5,)

    out = Path(__file__).parent / f"kline_{code}.png"
    style = mpf.make_mpf_style(
        base_mpf_style="charles",
        rc={
            "font.sans-serif": ["Microsoft YaHei", "SimHei"],
            "font.family": "sans-serif",
            "axes.unicode_minus": False,
        },
    )
    mpf.plot(
        df,
        type="candle",
        style=style,
        title=title,
        ylabel="价格 (元)",
        ylabel_lower="成交量",
        mav=mav,
        volume=True,
        figsize=(16, 8),
        show_nontrading=False,
        savefig=str(out),
    )
    print(f"已保存: {out}")
    import subprocess
    subprocess.Popen(["start", "", str(out)], shell=True)


if __name__ == "__main__":
    args = sys.argv[1:]
    if not args:
        print("用法: python view_kline.py <股票代码> [天数 | 开始日期 结束日期]")
        print("示例: python view_kline.py 600036")
        print("      python view_kline.py 600036 180")
        print("      python view_kline.py 600036 2024-01-01 2024-06-30")
        sys.exit(1)

    code = args[0]

    if len(args) == 3:
        df = load(code, start=args[1], end=args[2])
    elif len(args) == 2:
        df = load(code, days=int(args[1]))
    else:
        df = load(code, days=90)

    print(f"加载 {code}: {len(df)} 条记录，{df.index[0].date()} ~ {df.index[-1].date()}")
    plot(code, df)
