import pyspark, os, json, cv2, configparser
import numpy as np
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import TimestampType
from pyspark.sql.types import IntegerType
from pyspark.sql import functions

from faceDetector import detect

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.2 pyspark-shell'
#spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.2 streamProcessor.py

def run(argv=None):
    #Read config file
    config = configparser.ConfigParser()
    config.read('stream-processor-prop.cfg')

    #Initiate spark
    conf = SparkConf().setMaster("local[*]").setAppName("stream-processor")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, int(config.get("Kafka","interval")))

    #Initiate kafka properties
    brokers = config.get("Kafka","bootstrap.servers")
    topic= config.get("Kafka","topic")
    processedImageDir = config.get("OutputDir", "processed.output.dir")

    #Create schema for json message
    schema = StructType([
			StructField("cameraId", StringType(), True),
			StructField("timestamp", TimestampType(), True),
			StructField("rows", IntegerType(), True),
			StructField("cols", IntegerType(), True),
			StructField("data", StringType(), True)])

    #Create DataSet from stream messages from kafka

    kvs = KafkaUtils.createStream(ssc, brokers, "spark-streaming-consumer", {topic:1})
    #json.loads(x[1]['cameraId'])
    img = kvs.map(lambda x: ( json.loads(x[1])['cameraId']  , detect(json.loads(x[1])))) \
        .groupByKey()
    img.pprint()

    ssc.start()
    ssc.awaitTermination()


if __name__ == '__main__':
    run()
