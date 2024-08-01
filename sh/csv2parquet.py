#!/bin/bash
import pandas as pd

PARQUET_PATH=$1

READ_PATH = sys.argv[1]
SAVE_PATH = sys.argv[2]

df = pd.read_csv('~/data/csv/20240717/csv.csv', 
                 on_bad_lines='skip', 
                 names=['dt', 'cmd', 'cnt'])

df['dt'] = df['dt'].str.replace('^', '')
df['cmd'] = df['cmd'].str.replace('^', '')
df['cnt'] = df['cnt'].str.replace('^', '')

df['cnt'] = pd.to_numeric(df['cnt'], errors='cderce')

df['cnt'] = df['cnt'].fillna(0).astype(int)

df.to_parquet(f'{SAVE_PATH}/parquet.parquet')
