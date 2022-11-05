from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from keras.models import load_model
from sklearn.preprocessing import MinMaxScaler
from matplotlib.animation import FuncAnimation
import numpy as np

model = load_model('stock_LSTM')
date = np.array([])
real = np.array([])
predict = np.array([])

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
                StructType().add('Date', StringType())\
                            .add('Price', DoubleType())))

stream_df_new.printSchema()

def process_row(row):
    date = np.concatenate([date, [row.Date]])
    real = np.concatenate([real, [row.Price]])
    if real.shape[0] < 60:
        predict = np.concatenate([predict, [row.Price]])
        return
    elif real.shape[0] == 60:
        predict = np.concatenate([predict, [row.Price]])
    predictCur = model.predict(np.reshape(real[-60:],(60,1)))
    predict = np.concatenate([predict, predictCur])
    print(predictCur)



orders_agg_write_stream = stream_df_new \
        .writeStream \
        .foreach(process_row) \
        .trigger(processingTime='5 seconds') \
        .start()

orders_agg_write_stream.awaitTermination()
