from pyspark.sql import Row
from pyspark.sql.functions import lit
from pyspark.sql.types import StructField, StructType, StringType, LongType, IntegerType
import init_context

sc,sp = init_context.init_context()
df = None
df1 = None
def create_schema():
    myManualSchema = StructType([
        StructField("sno", IntegerType(), True),
    ])
    df = sp.createDataFrame([],myManualSchema)
    return df
def add_col(df,col_name,data_type):
    if df:
        df.withColumn(col_name,lit(None))
    else:
        df = create_schema()
        df.withColumn(col_name,lit())

def drop_col(col_name):
    if df:
        if col_name in df.columns:

            df1 = df.drop(col_name)
            return df1
        else:
            return f"{col_name} is not in Data Frame"
    else:
        return f"data frame has no column to delete."

    # return df1

