# 13-delete-wrong-field.py
# Jeff He @ Apr. 8

from pymongo import MongoClient

mongo_cli = MongoClient('mongodb://localhost:27017/')
db = client['processed_stock_reports_db']
collection = db['processed_reports']

collection.update_many({}, {'$unset': {'×ÛºÏµÃ·Ö': ""}})

print("Success")