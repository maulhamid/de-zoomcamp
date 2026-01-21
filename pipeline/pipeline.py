import sys

import pandas as pd

print('arguments', sys.argv)

month = int(sys.argv[1])

df = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})
print(df.head())

df.to_parquet(f'output_data_month_{month}.parquet')

print(f'Processing data for month: {month}')