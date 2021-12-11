import pandas as pd

import create_df

def write_data():
    if create_df.df1:
        pdf = create_df.df1.toPandas()
        pdf.to_csv("Data/df1.csv")
        return True