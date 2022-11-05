from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

TOPIC = "myTest"
BOOTSTRAP_SERVERS = 'localhost:9092'

spark = SparkSession.builder.appName("MY_TEST").master("local[*]").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

stream_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
        .option("subscribe", TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

stream_df.printSchema()

stream_df_new = stream_df.select(\
                from_json(stream_df.value.cast("string"),\
                StructType().add("Date", StringType())\
                            .add("Price", DoubleType())).alias("INFO")\
                ,"key", "timestamp"
                ).select("INFO.*")

stream_df_new.printSchema()

orders_agg_write_stream = stream_df_new \
        .writeStream \
        .format('console') \
        .trigger(processingTime='5 seconds') \
        .start()

orders_agg_write_stream.awaitTermination()  
