# 10-get-data-from-akshare.py
# Jeff He @ Apr. 8

import akshare as ak
from pymongo import MongoClient

def get_stock_codes_from_mongodb():
    mongo_cli = MongoClient('mongodb://localhost:27017/')
    db = mongo_cli['processed_stock_reports_db']
    collection = db['processed_reports']
    query = {"发布日期": {"$gte": "2008-01-01", "$lte": "2008-09-23"}}

    raw = collection.distinct("股票代码", query)
    processed = []
    for code in raw:
        try:
            formatted_code = str(int(code)).zfill(6)
            processed.append(formatted_code)
        except:
            print(f"error in code : {code}")
    unique = list(set(processed))
    
    mongo_cli.close()
    print(f"{len(unique)} found")
    return unique

def get_stock_history_data(stock_code):
    try:
        df = ak.stock_zh_a_hist(symbol=stock_code, period="monthly", start_date="20080101", end_date="20191231", adjust="") # Change argument "period" for daily data
        df = df[['日期', '收盘', '涨跌幅']]
        df.insert(0, '股票代码', stock_code)
        return df
    except Exception as e:
        print(f"failed in {stock_code} : {str(e)}")
        return None

def save_to_csv(dataframe, stock_code):
    if dataframe is not None and not dataframe.empty:
        filename = f"{stock_code}_2008_history.csv"
        dataframe.to_csv(filename, index=False)
        print(f"saved {filename} ({len(dataframe)})")

def main():
    processed_code = get_stock_codes_from_mongodb()
    for code in processed_code:
        history_data = get_stock_history_data(code)
        if history_data is not None:
            save_to_csv(history_data, code)

if __name__ == "__main__":
    main()