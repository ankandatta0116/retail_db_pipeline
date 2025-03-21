import sys
from lib import datamanipulation, dataReader, utils, datetime
from pyspark.sql.functions import *  # type: ignore

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Please specify the environment")
        sys.exit(-1)

    job_run_env = sys.argv[1]
    
    print("Creating Spark Session")
    spark = utils.get_spark_session(job_run_env)
    print(spark)
    print("Created Spark Session")
    dt = datetime.get_current_date()
    ts = datetime.get_current_time()
    # Read orders and filter closed orders
    df = dataReader.read_data(spark, job_run_env)
    df_filtered = datamanipulation.lost_orders(df)    
    df_filtered.show(truncate = False)
    df_filtered.write.mode("overwrite").parquet(f"hdfs://localhost:9000/user/vboxuser/retail_db/lost_revenue/{dt}/lost_revenue_{ts}.parquet")
    print("Finshed writing lost to HDFS")
    df_filtered = df_filtered.cache()
    df_filtered.count()
    grouping = datamanipulation.grouped_df(df_filtered)

    grouping = grouping.cache()
    grouping.count()
    print("Writing to HDFS")
    grouping.write.mode("overwrite").partitionBy("StockCode").parquet(f"hdfs://localhost:9000/user/vboxuser/retail_db/lost_revenue/{dt}/lost_revenue_{ts}.parquet")
    print("Finshed writing aggregations to HDFS")
    print("Stopping Spark Session")
    spark.stop()
    print("End of main")
