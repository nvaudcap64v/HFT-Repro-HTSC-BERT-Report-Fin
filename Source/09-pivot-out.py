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

query = {"��������": {"$gte": start_date, "$lte": end_date}}
reports = collection.find(query)
data = {}

for report in reports:
    code = report['��Ʊ����']
    date = report['��������']
    score = report['�ۺϵ÷�']
    if code not in data:
        data[code] = {}

    date_str = date
    data[code][date_str] = score

all_time = sorted(set(date for data in data.values() for date in data.keys()))
df = pd.DataFrame(columns=['��Ʊ����'] + all_time)
rows = []

for stock_code, data in data.items():
    row = {'��Ʊ����': stock_code}
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
    if pd.isna(row['��Ʊ����']):
        continue

    for start_index in range(1, len(row)):
        score = calculate_weighted_score(row, start_index, weights)
        results.append({'��Ʊ����': row['��Ʊ����'], 'Date': row.index[start_index], 'Score': score})

result_df = pd.DataFrame(results)
pivot_df = result_df.pivot(index='Date', columns='��Ʊ����', values='Score')

pivot_df.to_excel("pivoted.xlsx")
print("Done")