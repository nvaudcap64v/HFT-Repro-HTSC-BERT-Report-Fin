# 8-factor-visualize.py
# Jeff He @ Apr. 8

import pymongo
import pandas as pd
from datetime import datetime
import warnings
warnings.filterwarnings("ignore",category=Warning)

mongo_cli = pymongo.MongoClient("mongodb://localhost:27017/")
db = mongo_cli["processed_stock_reports_db"]
collection = db["processed_reports"]
start_date = str(datetime(2008, 1, 1))
end_date = str(datetime(2008, 9, 27))

query = {"��������": {"$gte": start_date, "$lte": end_date}}
reports = collection.find(query)
stock_data = {}

for report in reports:
    code = report['��Ʊ����']
    report_date = report['��������']
    score = report['�ۺϵ÷�']

    if code not in stock_data:
        stock_data[code] = {}
    date_str = report_date
    stock_data[code][date_str] = score

all_time = sorted(set(date for data in stock_data.values() for date in data.keys()))
df = pd.DataFrame(columns=['��Ʊ����'] + all_time)
rows = []

for code, data in stock_data.items():
    row = {'��Ʊ����': code}
    for date in all_time:
        row[date] = data.get(date, 0)
    rows.append(row)
df = pd.concat([df, pd.DataFrame(rows)], ignore_index=True)
df.to_excel("stock_scores.xlsx", index=False)
print("done")