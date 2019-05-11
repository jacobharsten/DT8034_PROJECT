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
from kafka import KafkaProducer
import base64

from faceDetector import detect

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.2 pyspark-shell'
producer = KafkaProducer(bootstrap_servers='34.90.40.186:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
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
    processedImageDir = config.get("OutputDir", "processed.output.dir")

    #Initiate kafka properties
    brokers = config.get("Kafka","bootstrap.servers")
    topic= config.get("Kafka","topic")

    kvs = KafkaUtils.createStream(ssc, brokers, "spark-streaming-consumer", {topic:1})
    package = kvs.map(lambda x: json.loads(x[1]))
    img = package.map(lambda x: detect(x))
    blurred_img = img.map(lambda x: convolute(x))

    blurred_img.pprint()


    ssc.start()
    ssc.awaitTermination()

def saveFile(data):
    img_name = 'images/face-'+str(data['timestamp'])+'.jpg'
    cv2.imwrite(img_name, data['data'])
    return data

def gaussianKernel(sigma):
    #creates gaussian kernel with side length and a sigma
    gx = np.arange(-sigma*3, sigma*3)
    x, y = np.meshgrid(gx, gx)

    kernel = np.exp(-(np.square(x) + np.square(y)) / (2*np.square(sigma)))

    return kernel / np.sum(np.sum(kernel))

def sendToViewer(data):
    img = data['data']
    img_as_text = base64.b64encode(img)
    data = {
    "cameraId":1,
    "data": img_as_text.decode('utf-8')
    }
    producer.send('viewer',data)


def convolute(package):
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
    #Send to viewer topic
    #Save image
    saveFile(package)
    return package

if __name__ == '__main__':
    run()
