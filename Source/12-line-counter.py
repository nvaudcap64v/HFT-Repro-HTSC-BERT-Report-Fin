# 12-line-counter.py
# Jeff He @ Apr. 8

import os
import pandas as pd

new_dict = dict()

def count_lines_in_csv(folder_path, output_file):
    total_lines_ = 0
    with open(output_file, 'w', encoding='utf-8') as f:
        for filename in os.listdir(folder_path):
            if filename.endswith(".csv"):
                file_path = os.path.join(folder_path, filename)
                try:
                    df = pd.read_csv(file_path)
                    lines_in_file = len(df) - 1
                    total_lines_ += lines_in_file
                    f.write(f"{filename}-{lines_in_file}\n")
                except Exception as e:
                    f.write(f"unable to read {filename}: {e}\n")
            new_dict[filename] = total_lines_
        f.write(f"total line count: {total_lines_}\n")

    return total_lines_

folder_path = r'D:\Projects\sina-report'
output_file = 'output.txt'
total_lines_count = count_lines_in_csv(folder_path, output_file)

print(f"total line count: {total_lines_count}")