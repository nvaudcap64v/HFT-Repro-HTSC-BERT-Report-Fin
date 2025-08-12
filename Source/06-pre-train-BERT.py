# 6-pre-train-BERT.py
# Jeff He @ Apr. 8

import pandas as pd
import tensorflow as tf
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report
from transformers import BertTokenizer, TFBertForSequenceClassification

tf.config.threading.set_intra_op_parallelism_threads(6)
tf.config.threading.set_inter_op_parallelism_threads(6)

data_path = 'test_data.csv'
df = pd.read_csv(data_path)
texts = df['标题']
labels = df['正负面'].values

train_texts, val_texts, train_labels, val_labels = train_test_split(texts, labels, test_size=0.2)

model_path = r'D:\Projects\bert'

tokenizer = BertTokenizer.from_pretrained(model_path)

def encode_texts(texts, tokenizer, max_length=500):
    return tokenizer(texts.tolist(), padding=True, truncation=True, max_length=max_length, return_tensors="tf")

train_encodings = encode_texts(train_texts, tokenizer)
val_encodings = encode_texts(val_texts, tokenizer)

train_dataset = tf.data.Dataset.from_tensor_slices((dict(train_encodings), train_labels))
val_dataset = tf.data.Dataset.from_tensor_slices((dict(val_encodings), val_labels))

model = TFBertForSequenceClassification.from_pretrained(model_path, num_labels=2)

optimizer = tf.keras.optimizers.Adam(learning_rate=0.00001)
loss = tf.keras.losses.SparseCategoricalCrossentropy(from_logits=True)
metrics = ['accuracy']

model.compile(optimizer=optimizer, loss=loss, metrics=metrics)
history = model.fit(train_dataset.batch(8), epochs=5, validation_data=val_dataset.batch(8))
model.save('trained_model')
print("model saved")

val_loss, val_accuracy = model.evaluate(val_dataset.batch(8))
print(f"validation loss: {val_loss}")
print(f"validation accuracy: {val_accuracy}")
val_preds = model.predict(val_dataset.batch(8)).logits
predicted_labels = tf.argmax(val_preds, axis=1)
print("accuracy:", accuracy_score(val_labels, predicted_labels))
print(classification_report(val_labels, predicted_labels))