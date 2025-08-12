# 3-write-to-mongo.py
# Jeff He @ Apr. 8

import pandas as pd
import pymongo
import os

mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
db = mongo_client["stock_reports_db"]
collection = db["reports"]
path = r"D:\Projects\sina-report"

for file_name in os.listdir(path):
    if file_name.endswith(".csv"):
        file_path = os.path.join(path, file_name)
        df = pd.read_csv(file_path)

        for _, row in df.iterrows():
            record = {
                "股票代码": row["股票代码"],
                "券商简称": row["券商简称"],
                "发布日期": row["发布日期"],
                "研报标题": row["研报标题"],
                "报告链接": row["报告链接"],
                "研报文本": row["研报文本"],
                "研究员": row["研究员"],
                "年份": file_name.split('-')[0],
                "月份": file_name.split('-')[1].split('.')[0]
            }
            collection.insert_one(record)
    print(f"{file_name} is done")

print("All done")