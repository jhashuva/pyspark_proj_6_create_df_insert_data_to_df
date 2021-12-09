from pyspark.sql import Row
from pyspark.sql.functions import lit
from pyspark.sql.types import StructField, StructType, StringType, LongType, IntegerType
import init_context

sc,sp = init_context.init_context()
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
    pass
    # return df1

