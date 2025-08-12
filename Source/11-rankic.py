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
    df_factor.columns = ['����', '��Ʊ����', '����ֵ']
    df_factor['��Ʊ����'] = df_factor['��Ʊ����'].astype(str).str.zfill(6)
    df_factor = df_factor[df_factor['����ֵ'] != 0]
    df_factor = df_factor.groupby(['��Ʊ����', pd.Grouper(key='����', freq='M')]).last().reset_index()
    scaler = StandardScaler()
    df_factor['����ֵ'] = df_factor.groupby('��Ʊ����')['����ֵ'].transform(
        lambda x: scaler.fit_transform(x.values.reshape(-1, 1)).flatten()
    )
    return df_factor

def load_return_data():
    all_returns = []
    for file in tqdm(os.listdir(RETURN_DIR), desc="������������"):
        if file.endswith(".csv"):
            stock_code = file.split("_")[0]
            file_path = os.path.join(RETURN_DIR, file)
            try:
                df = pd.read_csv(file_path, parse_dates=['����'])
                if df['����'].dt.to_period('M').min() != pd.Period('2008-01', freq='M'):
                    continue
                df['�����·�'] = df['����'] - pd.offsets.MonthEnd(1)
                df['��Ʊ����'] = stock_code
                all_returns.append(df[['�����·�', '��Ʊ����', '�ǵ���']])
            except Exception as e:
                print(f"���� {file} ʧ��: {str(e)}")
    return pd.concat(all_returns) if all_returns else pd.DataFrame()

def calculate_rankic(factor_df, return_df):
    merged = pd.merge(
        factor_df,
        return_df,
        left_on=['��Ʊ����', '����'],
        right_on=['��Ʊ����', '�����·�'],
        how='inner'
    ).dropna(subset=['����ֵ', '�ǵ���'])
    results = []
    for date, group in tqdm(merged.groupby('����'), desc="����RankIC"):
        if len(group) < 2:
            continue
        ic, p_value = spearmanr(group['����ֵ'], group['�ǵ���'])
        results.append({
            '����': date,
            'RankIC': ic,
            'pֵ': p_value,
            '��Ʊ����': len(group)
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
    print("\nData��")
    print(f"Average: {rankic_df['RankIC'].mean():.4f}")
    print(f"p: {(rankic_df['p'] < 0.05).mean():.2%}")
    print(f"Std: {rankic_df['RankIC'].std():.4f}")
    print(f"max: {rankic_df['RankIC'].max():.4f}")
    print(f"min: {rankic_df['RankIC'].min():.4f}")

if __name__ == "__main__":
    main()