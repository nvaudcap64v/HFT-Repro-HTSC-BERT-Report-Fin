# 9-pivot-out.py
# Jeff He @ Apr. 8

import pymongo
import pandas as pd
from datetime import datetime

mongo_cli = pymongo.MongoClient("mongodb://localhost:27017/")
db = mongo_cli["processed_stock_reports_db"]
collection = db["processed_reports"]

start_date = str(datetime(2008, 1, 1))
end_date = str(datetime(2008, 9, 27))

query = {"发布日期": {"$gte": start_date, "$lte": end_date}}
reports = collection.find(query)
data = {}

for report in reports:
    code = report['股票代码']
    date = report['发布日期']
    score = report['综合得分']
    if code not in data:
        data[code] = {}

    date_str = date
    data[code][date_str] = score

all_time = sorted(set(date for data in data.values() for date in data.keys()))
df = pd.DataFrame(columns=['股票代码'] + all_time)
rows = []

for stock_code, data in data.items():
    row = {'股票代码': stock_code}
    for date in all_time:
        row[date] = data.get(date, 0)
    rows.append(row)
df = pd.concat([df, pd.DataFrame(rows)], ignore_index=True)
weights = [1/(90-i) for i in range(90)]

def calculate_weighted_score(row, start_index, weights):
    weighted = 0
    total_weight = 0
    for i in range(90):
        index = start_index - i
        if index < 1:
            score = 0
        else:
            date = row.index[index]
            score = row[date]
        
        weight = weights[i]
        weighted += score * weight
        total_weight += weight
    
    return weighted / total_weight if total_weight != 0 else 0

results = []

for index, row in df.iterrows():
    if pd.isna(row['股票代码']):
        continue

    for start_index in range(1, len(row)):
        score = calculate_weighted_score(row, start_index, weights)
        results.append({'股票代码': row['股票代码'], 'Date': row.index[start_index], 'Score': score})

result_df = pd.DataFrame(results)
pivot_df = result_df.pivot(index='Date', columns='股票代码', values='Score')

pivot_df.to_excel("pivoted.xlsx")
print("Done")