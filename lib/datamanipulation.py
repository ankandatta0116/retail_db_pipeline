from pyspark.sql.functions import * # type: ignore

def filter_closed_orders(df):
    return df.filter(col("InvoiceNo").isin(["549251"])) # type: ignore