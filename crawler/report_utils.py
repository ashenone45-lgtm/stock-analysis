"""
报告生成共用工具函数

gen_report.py / report_html.py / push_report.py 均可从此处导入，
避免在多个文件中重复定义相同逻辑。
"""
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

FINANCIAL_DIR = Path(__file__).parent.parent / "data" / "financial"


def sector_table(df: pd.DataFrame) -> pd.DataFrame:
    """按行业聚合市场数据，返回按平均涨跌降序排列的 DataFrame。"""
    if df.empty or "行业" not in df.columns:
        return pd.DataFrame()
    grp = df.groupby("行业")
    result = pd.DataFrame({
        "行业":     list(grp.groups.keys()),
        "平均涨跌": grp["涨跌幅"].mean().values,
        "成交额亿": grp["成交额亿"].sum().values,
        "上涨数":   grp["涨跌幅"].apply(lambda x: (x > 0).sum()).values,
        "下跌数":   grp["涨跌幅"].apply(lambda x: (x < 0).sum()).values,
        "强势数":   grp["score"].apply(lambda x: (x >= 3).sum()).values,
        "平均量比": grp["vol_ratio"].mean().values,
    })
    return result.sort_values("平均涨跌", ascending=False).reset_index(drop=True)


def load_financial_snapshot(symbols: list) -> dict:
    """读取财务快照，返回 {code: {"rev_yoy": float, "debt_ratio": float}}。

    rev_yoy:    最新季报营收 vs 约一年前同期的同比增速（%）
    debt_ratio: 最新期资产负债率（%）
    """
    result = {}
    for code in symbols:
        entry = {}
        income_path = FINANCIAL_DIR / f"{code}_income.parquet"
        if income_path.exists():
            try:
                df = pd.read_parquet(income_path).sort_values("报告日")
                rev_col = next((c for c in ["营业总收入", "营业收入"] if c in df.columns), None)
                if rev_col:
                    rev = df[rev_col].dropna().astype(float)
                    if len(rev) >= 5 and rev.iloc[-5] != 0:
                        entry["rev_yoy"] = (rev.iloc[-1] - rev.iloc[-5]) / abs(rev.iloc[-5]) * 100
            except Exception:
                pass
        balance_path = FINANCIAL_DIR / f"{code}_balance.parquet"
        if balance_path.exists():
            try:
                df = pd.read_parquet(balance_path).sort_values("报告日")
                last = df.iloc[-1]
                if "资产总计" in df.columns and "负债合计" in df.columns:
                    a, l = float(last["资产总计"]), float(last["负债合计"])
                    if a != 0 and not np.isnan(a) and not np.isnan(l):
                        entry["debt_ratio"] = l / a * 100
            except Exception:
                pass
        if entry:
            result[code] = entry
    return result


def prepare_report_context(df: pd.DataFrame) -> dict[str, Any]:
    """聚合报告所需的全部统计量，供 HTML / PDF 渲染器共用，避免重复计算。

    Returns:
        dict，包含以下 key：
        total, n_up, n_down, n_flat, n_limit_up, n_limit_down,
        avg_chg, med_chg, total_vol, avg_vol_ratio,
        rsi_avg, n_oversold, n_overbought,
        strong, weak, strong_top,
        top10_gain, top10_loss, top5_vol, oversold_c, risk_stocks,
        sector_df, fin_strong, fin_oversold,
        sentiment, suggestion, se_emoji, strong_weak
    """
    total         = len(df)
    n_up          = int((df["涨跌幅"] > 0).sum())
    n_down        = int((df["涨跌幅"] < 0).sum())
    n_flat        = total - n_up - n_down
    n_limit_up    = int((df["涨跌幅"] >= 9.9).sum())
    n_limit_down  = int((df["涨跌幅"] <= -9.9).sum())
    avg_chg       = float(df["涨跌幅"].mean())
    med_chg       = float(df["涨跌幅"].median())
    total_vol     = float(df["成交额亿"].sum())
    avg_vol_ratio = float(df["vol_ratio"].dropna().mean())
    rsi_avg       = float(df["rsi"].dropna().mean())
    n_oversold    = int((df["rsi"] < 30).sum())
    n_overbought  = int((df["rsi"] > 70).sum())

    strong      = df[df["score"] >= 3]
    weak        = df[df["score"] <= -3]
    strong_top  = strong.nlargest(10, "涨跌幅")
    top10_gain  = df.nlargest(10, "涨跌幅")
    top10_loss  = df.nsmallest(10, "涨跌幅")
    top5_vol    = df.nlargest(5, "成交额亿")
    oversold_c  = df[(df["rsi"] < 30) & (df["涨跌幅"] < 0)].nsmallest(8, "rsi")
    risk_stocks = df[df["涨跌幅"] <= -9.9].sort_values("涨跌幅")
    sector_df   = sector_table(df)

    fin_strong   = load_financial_snapshot(list(strong_top["代码"])) if not strong_top.empty else {}
    fin_oversold = load_financial_snapshot(list(oversold_c["代码"])) if not oversold_c.empty else {}

    if avg_chg >= 1.0:
        sentiment, suggestion = "乐观偏多", "可适度跟进强势股，注意仓位控制"
    elif avg_chg >= 0:
        sentiment, suggestion = "温和偏多", "关注成交量放大的强势品种，轻仓试多"
    elif avg_chg >= -1.5:
        sentiment, suggestion = "谨慎偏空", "以观望为主，持仓控制在五成以下"
    elif avg_chg >= -3.0:
        sentiment, suggestion = "明显偏空", "观望为主，不追跌；关注超卖龙头企稳信号"
    else:
        sentiment, suggestion = "极度悲观", "空仓观望；超卖个股等放量止跌后再考虑布局"

    se_emoji    = {"乐观偏多": "🚀", "温和偏多": "📈", "谨慎偏空": "😐",
                   "明显偏空": "📉", "极度悲观": "🆘"}.get(sentiment, "📊")
    strong_weak = ("空头占优" if len(weak) > len(strong)
                   else "多头占优" if len(strong) > len(weak) else "多空均衡")

    return {
        "total": total,
        "n_up": n_up, "n_down": n_down, "n_flat": n_flat,
        "n_limit_up": n_limit_up, "n_limit_down": n_limit_down,
        "avg_chg": avg_chg, "med_chg": med_chg,
        "total_vol": total_vol, "avg_vol_ratio": avg_vol_ratio,
        "rsi_avg": rsi_avg, "n_oversold": n_oversold, "n_overbought": n_overbought,
        "strong": strong, "weak": weak, "strong_top": strong_top,
        "top10_gain": top10_gain, "top10_loss": top10_loss,
        "top5_vol": top5_vol, "oversold_c": oversold_c, "risk_stocks": risk_stocks,
        "sector_df": sector_df,
        "fin_strong": fin_strong, "fin_oversold": fin_oversold,
        "sentiment": sentiment, "suggestion": suggestion,
        "se_emoji": se_emoji, "strong_weak": strong_weak,
    }
