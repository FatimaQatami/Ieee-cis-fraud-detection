import pandas as pd

df = pd.read_parquet('merged.parquet')
print(df.info())


def reduce_memory(df):
    for col in df.select_dtypes(include='int64').columns:
        df[col] = df[col].astype('int32')

    for col in df.select_dtypes(include='float64').columns:
        df[col] = df[col].astype('float32')

    return df

reduce_memory(df)

print(df.info())

df.to_parquet('memo_reduced.parquet')