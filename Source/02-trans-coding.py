# 2-trans-coding.py
# Jeff He @ Apr. 8

import chardet
import pandas as pd
import os

file_path = r"D:\Projects\sina-report\2015-01.csv"

with open(file_path, "rb") as file:
    print(chardet.detect(file.read()))

df = pd.read_csv(file_path, encoding='utf-8-sig')
os.remove(file_path)
new_path = file_path
new_df = df.to_csv(new_path, encoding="utf-8", index=False)