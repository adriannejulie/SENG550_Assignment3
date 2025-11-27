import pandas as pd
import os

df = pd.read_csv('orders.csv')

nested_folder_path = os.path.join("part1", "data", "raw")
os.makedirs(nested_folder_path, exist_ok=True)
print(f"Created nested folder: {nested_folder_path}")

for column_value, group in df.groupby('order_dow'):
    folder_name = os.path.join(nested_folder_path, str(column_value))  # folder inside raw
    os.makedirs(folder_name, exist_ok=True)  # create the subfolder if it doesn't exist
    file_path = os.path.join(folder_name, f'orders_{column_value}.csv')  # file path inside subfolder
    group.to_csv(file_path, index=False)
    print(f"Saved {file_path}")
