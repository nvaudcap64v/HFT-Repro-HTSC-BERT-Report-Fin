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
                "��Ʊ����": row["��Ʊ����"],
                "ȯ�̼��": row["ȯ�̼��"],
                "��������": row["��������"],
                "�б�����": row["�б�����"],
                "��������": row["��������"],
                "�б��ı�": row["�б��ı�"],
                "�о�Ա": row["�о�Ա"],
                "���": file_name.split('-')[0],
                "�·�": file_name.split('-')[1].split('.')[0]
            }
            collection.insert_one(record)
    print(f"{file_name} is done")

print("All done")