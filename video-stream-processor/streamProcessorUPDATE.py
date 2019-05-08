#!/usr/bin/env python
# coding: utf-8
import os

'''
Working version however when running local a warning will appear:
    ' WARN  RandomBlockReplicationPolicy:66 - Expecting 1 replicas with only 0 peer/s. '
According to Stachoverflow this is because the number of workers is to low resulting in a non-replica behaviour.

Sause: https://stackoverflow.com/questions/32583273/spark-streaming-get-warn-replicated-to-only-0-peers-instead-of-1-peers

'''
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.2 pyspark-shell'

#spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.2 streamProcessorUPDATE.py
import pyspark
from pyspark import SparkContext, SparkConf

from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession

conf = SparkConf().setMaster("local[*]").setAppName("Streamer")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")


ssc = StreamingContext(sc, 2)

brokers = "34.90.40.186:2181"
topic= "test"

#directKafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {"bootstrap_servers": brokers})

kvs = KafkaUtils.createStream(ssc, brokers, "spark-streaming-consumer", {topic:1})

kvs.pprint()

ssc.start()
ssc.awaitTermination()
