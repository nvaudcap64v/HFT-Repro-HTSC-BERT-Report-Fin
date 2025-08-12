# 5-mongo-data-processing-2.py
# Jeff He @ Apr. 8

import pymongo
import re

mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
db = mongo_client['stock_reports_db']
collection = db['reports']
new = mongo_client['processed_stock_reports_db']
new_collection = new['processed_reports']
op_buffer = []

def split_by_sentence(text):
    if isinstance(text, str):
        sentences = re.split(r'(?<=。)', text)
        sentences = [sentence.strip() for sentence in sentences if sentence.strip()]
        return sentences
    return []

def is_meaningless(text):
    phrases = ["数据来源", "相关资料", "本报告不构成投资建议", "免责声明", "资料来源", "数据来自"]
    for phrase in phrases:
        if text.startswith(phrase):
            return True
    return False

for doc in collection.find():
    report = doc.get('研报文本', '')
    if report:
        split_sentences = split_by_sentence(report)
        filtered = [sentence for sentence in split_sentences if not is_meaningless(sentence)]
        if filtered:
            new_doc = doc.copy()
            new_doc['研报文本'] = filtered
            op_buffer.append(pymongo.InsertOne(new_doc))
        else:
            new_doc = doc.copy()
            del new_doc['研报文本']
            op_buffer.append(pymongo.InsertOne(new_doc))
    if len(op_buffer) >= 1000:
        new_collection.bulk_write(op_buffer)
        op_buffer = []

if op_buffer:
    new_collection.bulk_write(op_buffer)

print("data processing II is done")