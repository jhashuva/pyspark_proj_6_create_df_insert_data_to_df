import create_df
from pyspark.sql.functions import isnan, when, count, col
df = None
if create_df.df:
    df=create_df.df

def no_of_records():
    if df:
        return df.distinct().count()
    else:
        return "no records available"

def col_names():
    if df:
        return df.columns
    else:
        return "No records available"

def count_missing_values():
    if df:
        return df.select([count(when(isnan(c), c)).alias(c) for c in df.columns]).show()
    else:
        return "no records available"

def delete_cols(col):
    if df:
        df1 = df.drop(col)
        return df1.columns
    else:
        return "No columns available to delete."
