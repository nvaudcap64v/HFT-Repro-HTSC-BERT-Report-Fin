# 14-aggregate-return.py
# Jeff He @ Apr. 8

import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt
from scipy import stats
import akshare as ak

def load_data(factor_path, return_dir):
    factor_df = pd.read_excel(factor_path, index_col=0)
    factor_df.index = pd.to_datetime(factor_df.index)
    factor_df = factor_df.stack().reset_index()
    factor_df.columns = ['date', 'stock_id', 'factor']
    factor_df['stock_id'] = factor_df['stock_id'].astype(str).str.zfill(6)
    factor_df['month'] = factor_df['date'].dt.to_period('M')
    monthly_factor = factor_df.sort_values('date').groupby(['stock_id', 'month']).last().reset_index()
    monthly_factor['next_month'] = monthly_factor['month'] + 1
    monthly_factor['factor_abs'] = monthly_factor['factor'].abs()
    monthly_sorted = monthly_factor.sort_values(['month', 'factor_abs'], ascending=[True, False])
    def assign_layer(group):
        n = len(group)
        group_num = 5
        size = n // group_num
        layers = []
        for i in range(n):
            if i < (group_num-1)*size:
                layer = i // size
            else:
                layer = group_num-1
            layers.append(layer)
        return pd.Series(layers, index=group.index)
    monthly_sorted['layer'] = monthly_sorted.groupby('month', group_keys=False).apply(assign_layer)
    monthly_factor = monthly_sorted[['stock_id', 'next_month', 'layer']]
    return_path = Path(return_dir)
    all_returns = []
    for f in return_path.glob('*.csv'):
        stock_id = f.stem.split('_')[0].zfill(6)
        df_return = pd.read_csv(f, parse_dates=['日期'], usecols=['日期', '涨跌幅'])
        df_return.columns = ['date', 'return']
        df_return['stock_id'] = stock_id
        df_return['month'] = df_return['date'].dt.to_period('M')
        all_returns.append(df_return)
    return_df = pd.concat(all_returns)
    merged_df = pd.merge(
        monthly_factor,
        return_df,
        left_on=['stock_id', 'next_month'],
        right_on=['stock_id', 'month'],
        how='inner'
    )
    return merged_df

def get_shanghai_index_monthly_returns():
    index_daily = ak.stock_zh_index_daily(symbol="sh000001")
    index_daily['date'] = pd.to_datetime(index_daily['date'])
    index_daily.set_index('date', inplace=True)
    monthly_last = index_daily.resample('M').last()
    monthly_last['monthly_return'] = monthly_last['close'].pct_change()
    monthly_last['month'] = monthly_last.index.to_period('M')
    monthly_last = monthly_last[~monthly_last.index.duplicated(keep='last')]
    monthly_last = monthly_last.dropna(subset=['monthly_return'])
    return monthly_last.set_index('month')['monthly_return']

def calculate_excess_returns(df):
    stock_monthly_returns = (
        df.groupby(['stock_id', 'month'])['return']
        .apply(lambda x: (1 + x/100).prod() - 1)
        .reset_index()
        .rename(columns={'return': 'monthly_return'})
    )
    layer_info = df[['stock_id', 'month', 'layer']].drop_duplicates()
    merged_returns = pd.merge(stock_monthly_returns, layer_info, on=['stock_id', 'month'], how='left')
    layer_monthly = merged_returns.groupby(['month', 'layer'])['monthly_return'].mean().unstack()
    index_returns = get_shanghai_index_monthly_returns()
    common_months = layer_monthly.index.intersection(index_returns.index)
    layer_monthly = layer_monthly.loc[common_months]
    index_returns = index_returns.loc[common_months]
    excess_returns = layer_monthly.sub(index_returns, axis=0)
    cumulative_returns = (1 + excess_returns).cumprod() - 1
    return excess_returns, cumulative_returns

if __name__ == "__main__":
    factor_path = r"pivoted.xlsx"
    return_dir = r"D:\Projects\new-history"
    df = load_data(factor_path, return_dir)
    if df.empty:
        print("Data is empty")
        exit()
    all_months = pd.Series(df['month'].unique()).sort_values()
    if len(all_months) > 0:
        filtered_df = df[df['month'].isin(all_months[:-1])]
    else:
        print("Data is not available")
        exit()
    excess_ret, cum_ret = calculate_excess_returns(filtered_df)
    plt.figure(figsize=(12, 6))
    cum_ret.plot(title='Cumulative Monthly Excess Returns (vs SH.000001)')
    plt.ylabel('Cumulative Excess Return')
    plt.xlabel('Month')
    plt.grid(True)
    plt.show()

    print("Avg. Return for every layer:")
    print(excess_ret.mean())
    print("\nT-Value:")
    for layer in range(5):
        t_stat, p_value = stats.ttest_1samp(excess_ret[layer].dropna(), 0)
        print(f"Layer {layer}: t={t_stat:.3f} (p={p_value:.3f})")