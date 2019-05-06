
from pyspark.sql import SparkSession
import configparser
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import TimestampType
from pyspark.sql.types import IntegerType
from pyspark.sql import functions


def run(argv=None):
    #Read config file
    config = configparser.ConfigParser()
    config.read('stream-processor-prop.cfg')

    spark = SparkSession.builder.master("local").appName("stream-processor")\
        .config("spark.jars", "spark-streaming-kafka-0-8-assembly_2.11-2.4.2.jar")\
        .getOrCreate()

    processedImageDir = config.get("OutputDir", "processed.output.dir")
    #spark-submit streamProcessor.py --packages https://org.apache.spark:spark-sql-kafka-0-8_2.1
    #Create schema for json message
    schema = StructType([
			StructField("cameraId", StringType(), True),
			StructField("timestamp", TimestampType(), True),
			StructField("rows", IntegerType(), True),
			StructField("cols", IntegerType(), True),
			StructField("data", StringType(), True)])

    #Create DataSet from stream messages from kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", config.get("Kafka","bootstrap.servers")) \
        .option("subscribe", "test") \
        .load()

    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    print(df)

    """ds = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", config.get("Kafka","bootstrap.servers")) \
        .option("subscribe", config.get("Kafka","topic")) \
        .option("kafka.max.partition.fetch.bytes", config.get("Kafka","max.partition.fetch.bytes")) \
        .option("kafka.max.poll.records", config.get("Kafka","max.poll.records")) \
        .load() \
        .selectExpr("CAST(value AS STRING) as message") \
        .saveAsTextFile("test")"""

        #.select(functions.from_json(functions.col("message"),schema) \
        #.alias("json")) \
        #.select("json.*")
        #.as(Encoders.bean(VideoEventData))

    #key-value pair of cameraId-dataBlock
    #kvDataset = ds.map(lambda data : (data[0], data)).groupByKey()


    #process for openCV <TODO> write some stuff here
    #processedDataset = kvDataset.saveAsTextFile("test")

    #faceDetector.run();;;;;;

    #start
    """query = processedDataset.writeStream() \
        .outputMode("update") \
        .format("console") \
        .start() \
        .awaitTermination()"""


if __name__ == '__main__':
    run()
