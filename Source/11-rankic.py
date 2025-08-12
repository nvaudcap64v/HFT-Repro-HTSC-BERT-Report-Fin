# 11-rankic.py
# Jeff He @ Apr. 8

import os
import pandas as pd
import numpy as np
from scipy.stats import spearmanr
from tqdm import tqdm
from sklearn.preprocessing import StandardScaler

FACTOR_FILE = r"pivoted.xlsx"
RETURN_DIR = r"D:\Projects\history-month"
OUTPUT_FILE = "RankIC.csv"

def load_factor_data():
    df_factor = pd.read_excel(FACTOR_FILE, index_col=0, parse_dates=True)
    df_factor = df_factor.stack().reset_index()
    df_factor.columns = ['日期', '股票代码', '因子值']
    df_factor['股票代码'] = df_factor['股票代码'].astype(str).str.zfill(6)
    df_factor = df_factor[df_factor['因子值'] != 0]
    df_factor = df_factor.groupby(['股票代码', pd.Grouper(key='日期', freq='M')]).last().reset_index()
    scaler = StandardScaler()
    df_factor['因子值'] = df_factor.groupby('股票代码')['因子值'].transform(
        lambda x: scaler.fit_transform(x.values.reshape(-1, 1)).flatten()
    )
    return df_factor

def load_return_data():
    all_returns = []
    for file in tqdm(os.listdir(RETURN_DIR), desc="加载行情数据"):
        if file.endswith(".csv"):
            stock_code = file.split("_")[0]
            file_path = os.path.join(RETURN_DIR, file)
            try:
                df = pd.read_csv(file_path, parse_dates=['日期'])
                if df['日期'].dt.to_period('M').min() != pd.Period('2008-01', freq='M'):
                    continue
                df['因子月份'] = df['日期'] - pd.offsets.MonthEnd(1)
                df['股票代码'] = stock_code
                all_returns.append(df[['因子月份', '股票代码', '涨跌幅']])
            except Exception as e:
                print(f"加载 {file} 失败: {str(e)}")
    return pd.concat(all_returns) if all_returns else pd.DataFrame()

def calculate_rankic(factor_df, return_df):
    merged = pd.merge(
        factor_df,
        return_df,
        left_on=['股票代码', '日期'],
        right_on=['股票代码', '因子月份'],
        how='inner'
    ).dropna(subset=['因子值', '涨跌幅'])
    results = []
    for date, group in tqdm(merged.groupby('日期'), desc="计算RankIC"):
        if len(group) < 2:
            continue
        ic, p_value = spearmanr(group['因子值'], group['涨跌幅'])
        results.append({
            '日期': date,
            'RankIC': ic,
            'p值': p_value,
            '股票数量': len(group)
        })
    return pd.DataFrame(results)

def main():
    print("Factor Data Loading")
    factor_df = load_factor_data()
    print("\nMarket Data Loading")
    return_df = load_return_data()
    if return_df.empty:
        print("Abort")
        return
    rankic_df = calculate_rankic(factor_df, return_df)
    rankic_df.to_csv(OUTPUT_FILE, index=False)
    print(f"\n{OUTPUT_FILE}")
    print("\nData：")
    print(f"Average: {rankic_df['RankIC'].mean():.4f}")
    print(f"p: {(rankic_df['p'] < 0.05).mean():.2%}")
    print(f"Std: {rankic_df['RankIC'].std():.4f}")
    print(f"max: {rankic_df['RankIC'].max():.4f}")
    print(f"min: {rankic_df['RankIC'].min():.4f}")

if __name__ == "__main__":
    main()