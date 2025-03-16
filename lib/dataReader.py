from lib import configReader
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType
from pyspark.sql import SparkSession
from py4j.java_gateway import JavaGateway
import re
from py4j.java_gateway import java_import

def read_data(spark, env):
    schema = StructType([
        StructField("InvoiceNo", StringType(), True),
        StructField("StockCode", StringType(), True),
        StructField("Description", StringType(), True),
        StructField("Quantity", IntegerType(), True),
        StructField("InvoiceDate", StringType(), True),
        StructField("UnitPrice", DoubleType(), True),
        StructField("CustomerID", IntegerType(), True),
        StructField("Country", StringType(), True)
    ])

    conf = configReader.get_app_config(env)
    base_path = conf["file.path"]

    if not base_path.startswith("hdfs://"):
        base_path = f"hdfs://localhost:9000{base_path}"

    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.defaultFS", "hdfs://localhost:9000")  
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    path = spark._jvm.org.apache.hadoop.fs.Path(base_path)

    status = fs.listStatus(path)
    directories = [file.getPath().getName() for file in status if file.isDirectory()]
    date_dirs = sorted([d for d in directories if re.match(r"\d{4}-\d{2}-\d{2}", d)], reverse=True)

    if not date_dirs:
        raise FileNotFoundError(f"No valid date partitions found in {base_path}")

    file_path = f"{base_path}/{date_dirs[0]}/raw.parquet"

    print(f"Reading data from: {file_path}")

    df = spark.read.schema(schema).parquet(file_path)

    return df


