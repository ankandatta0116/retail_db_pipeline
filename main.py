import sys
from lib import datamanipulation, dataReader, utils
from pyspark.sql.functions import *  # type: ignore

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Please specify the environment")
        sys.exit(-1)

    job_run_env = sys.argv[1]
    
    print("Creating Spark Session")
    spark = utils.get_spark_session(job_run_env)
    print("Created Spark Session")

    # Read orders and filter closed orders
    df = dataReader.read_data(spark, job_run_env)
    df_filtered = datamanipulation.filter_closed_orders(df)

    
    df_filtered.show(truncate = False)
    spark.stop()
    print("End of main")
