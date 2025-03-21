from pyspark.sql.functions import * # type: ignore

def lost_orders(df):
    returns_df = df.filter(col("InvoiceNo").startswith("C")) \
               .withColumn("lost_revenue", col("Quantity") * col("UnitPrice"))
    return returns_df

def grouped_df(df):
    grouped_df = df.groupBy("StockCode", "InvoiceDate").agg(sum("lost_revenue").alias("lost_revenue"))
    return grouped_df