# 7-BERT-on-cpu.py
# Jeff He @ Apr. 8

import tensorflow as tf
from transformers import BertTokenizer
from pymongo import MongoClient
import numpy as np

mongo_cli = MongoClient('mongodb://localhost:27017/')
source_db = mongo_cli['processed_stock_reports_db']
source_collection = source_db['processed_reports']
sentiment_db = mongo_cli['sentiment_analysis_v2_db']
sentiment_collection = sentiment_db['sentence_predictions']

tokenizer = BertTokenizer.from_pretrained(r'D:\Projects\bert')
model = tf.saved_model.load(r'D:\Projects\report-analysis\trained_model')
infer = model.signatures['serving_default']

def process_report(report) -> dict:
    if '研报文本' not in report or not report['研报文本']:
        return None

    encodings = tokenizer(report['研报文本'], padding=True, truncation=True, max_length=500, return_tensors="tf")
    encodings['token_type_ids'] = tf.zeros_like(encodings['input_ids'])

    logits = infer(input_ids=encodings['input_ids'], attention_mask=encodings['attention_mask'], token_type_ids=encodings['token_type_ids'])['logits']
    probs = tf.nn.softmax(logits, axis=-1).numpy()
    predictions = np.argmax(probs, axis=1)
    sentiment_docs = []
    adjusted_total = 0.0

    for i, text in enumerate(report['研报文本']):
        raw_prob = float(probs[i][1])
        adjusted_score = raw_prob - 0.5 
        adjusted_total += adjusted_score
        sentiment_doc = {
            '股票代码': report['股票代码'],
            '发布日期': report['发布日期'],
            '研报标题': report['研报标题'],
            '原始文本': text,
            '预测结果': '正面' if predictions[i] == 1 else '负面',
            '原始正面概率': raw_prob,
            '调整后得分': adjusted_score,
            '关联文档ID': report['_id']
        }
        sentiment_docs.append(sentiment_doc)

    avg_score = adjusted_total / len(report['研报文本']) if report['研报文本'] else 0
    return {'sentiment_docs': sentiment_docs, 'avg_score': avg_score}

def get_user_date_input(prompt):
    while True:
        date_str = input(prompt)
        try:
            return date_str
        except ValueError:
            print("wrong format")

def main():
    start_date = get_user_date_input("start:")
    end_date = get_user_date_input("end:")

    if end_date < start_date:
        print("wrong input")
        return

    query = {"发布日期": {"$gte": start_date, "$lte": end_date}}
    cursor = source_collection.find(query).sort("发布日期", 1)

    processed_dates = set()
    total_processed = 0
    current_date = None
    counter = 0

    for report in cursor:
        try:
            if current_date is not None and report['发布日期'] != current_date:
                print(f"day counter :  {current_date}, paper counter : {counter}, total counter : {len(processed_dates)}")
                counter = 0
            current_date = report['发布日期']
            result = process_report(report)
            
            if not result:
                continue
            if result['sentiment_docs']:
                sentiment_collection.insert_many(result['sentiment_docs'])
            source_collection.update_one({'_id': report['_id']}, {'$set': {'综合得分': result['avg_score']}})
            processed_dates.add(current_date)
            counter += 1
            total_processed += 1
            
        except Exception as e:
            print(f'error when processing {report.get("_id")}:{str(e)}')
            continue

    if current_date is not None:
        print(f"day counter :  {current_date}, paper counter : {counter}, total counter : {len(processed_dates)}")

    print(f"day counter : {len(processed_dates)} , paper counter : {total_processed}")

if __name__ == "__main__":
    main()