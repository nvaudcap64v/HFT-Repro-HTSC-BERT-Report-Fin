# 4-mongo-data-processing-1.py
# Jeff He @ Apr. 8

import pymongo
import re

mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
db = mongo_client['stock_reports_db']
collection = db['reports']
counter = 0
op_buffer = []

def clean_escaped_characters(text):
    if isinstance(text, str):
        processed = re.sub(r'[\n\r\t]', '', text)
        processed = re.sub(r'[^a-zA-Z0-9\u4e00-\u9fa5\s,.。，!？！:;()\-+*/&^%$#@=_<>]', '', processed)
        return processed
    return text

def remove_risk_warning(text):
    if isinstance(text, str):
        processed = re.sub(r'风险提示：[^\n]*', '', text)
        return processed
    return text

for doc in collection.find():
    report = doc.get('研报文本', '')

    if report:
        processed_text = clean_escaped_characters(report)
        cleaned_text = remove_risk_warning(processed_text)
        op_buffer.append(pymongo.UpdateOne(
                {'_id': doc['_id']},
                {'$set': {'研报文本': cleaned_text}}))
    counter += 1
    if counter % 1000 == 0:
        collection.bulk_write(op_buffer)
        op_buffer = []

if op_buffer:
    collection.bulk_write(op_buffer)

print("data processing I done")