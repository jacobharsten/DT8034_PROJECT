import pyspark, os, json, cv2, configparser
import numpy as np
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import KafkaProducer
import base64
import time
from google.cloud import storage

from faceDetector import detect

#add environment varible, since kafka does not come with pyspark
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.2 pyspark-shell'

#constants
KAFKA_URI = '34.90.40.186:9092'
KAFKA_TOPIC = 'viewer'
CONFIG_FILE_NAME = 'stream-processor-prop.cfg'


#create kafka producer
producer = KafkaProducer(bootstrap_servers=KAFKA_URI,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))


#spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.2 streamProcessor.py
#JAVA VERSION 8.1

def handler(message):
    """ Sends messages to kafka server"""

    records = message.collect()
    for record in records:
        
        #get data obj 
        obj = record[1]

        _, buf = cv2.imencode('.jpg', obj['data'])
        byte_array = base64.b64encode(buf)

        data = {
            "cameraId": obj['cameraId'],
            "timestamp": obj['timestamp'],
            "rows": obj['rows'],
            "cols": obj['cols'],
            "data": byte_array.decode('utf-8'),
            "face": obj['face']
        }

        # send message
        producer.send(KAFKA_TOPIC, data)
        producer.flush()

def run(argv=None):
    """ Spark pipeline"""

    #Read config file
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE_NAME)

    #Initiate spark
    conf = SparkConf().setMaster("local[*]").setAppName("stream-processor")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, int(config.get("Kafka","interval")))
    processedImageDir = config.get("OutputDir", "processed.output.dir")

    #Initiate kafka properties
    brokers = config.get("Kafka","bootstrap.servers")
    topic= config.get("Kafka","topic")
    
    # create kafka stream obj
    kvs = KafkaUtils.createStream(ssc, brokers, "spark-streaming-consumer", {topic:2})
    package = kvs.map(lambda x: (json.loads(x[1])['cameraId'], json.loads(x[1])))
    processedImages = package.map(lambda x: (x[0],detect(x[1])))
    blurredImages = processedImages.map(lambda x: (x[0],convolute(x[1])))

    '''
    For outputting to stream-viewer
        blurredImages.foreachRDD(handler)
    '''

    filteredImages = blurredImages.filter(lambda data: data[1]['face'] == 1) \
        .groupByKey() \
        .foreachRDD(saveToText)

    ssc.start()
    ssc.awaitTermination()

def saveToText(rdd):
    """ Save each rdd to gc bucket"""

    rdd.foreach(lambda data: writeObj(data))

def writeObj(data):
    """Uploads a file to the bucket."""
    
    #SET TO CONFIG
    BUCKET_NAME = 'streambucket'
    STORAGE_NAME = 'results/'

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(BUCKET_NAME)

    for i in data[1]:
        
        # TODO: FIX TIME.TIME() TO DATESTAMP FROM DATA PLS.
        DEST_NAME = STORAGE_NAME + 'output-'+ str(data[0]) +'-'+str(time.time())+'.txt'
        blob = bucket.blob(DEST_NAME)

        blob.upload_from_string(str(i))

def saveFile(data):
    """ Save file in .jpg format. """ 

    img_name = 'images/'+ str(data['cameraId']) +'--'+str(data['timestamp'])+'.jpg'
    cv2.imwrite(img_name, data['data'])
    return data

def gaussianKernel(sigma):
    """ Create gaussian filter with kernel length 3 """ 

    #creates gaussian kernel with side length and a sigma
    gx = np.arange(-sigma*3, sigma*3)
    x, y = np.meshgrid(gx, gx)
    kernel = np.exp(-(np.square(x) + np.square(y)) / (2*np.square(sigma)))

    return kernel / np.sum(np.sum(kernel))

def convolute(package):
    """ Convolute the data in package with a gaussian filter """ 

    img = package['data']
    kernel = gaussianKernel(2)
    
    # Padded fourier transform, with the same shape as the image
    kernel_ft = np.fft.fft2(kernel, s=img.shape[:2])
    
    # convolve
    img_ft = np.fft.fft2(img, axes=(0, 1))
    
    # the 'newaxis' is to match to color dimension
    imgKer_ft = img_ft * kernel_ft[:, :, np.newaxis]
    img = np.fft.ifft2(imgKer_ft,axes=(0, 1)).real
    img = np.array(img, dtype = np.uint8 )
    package['data'] = img
    return package

if __name__ == '__main__':
    run()
