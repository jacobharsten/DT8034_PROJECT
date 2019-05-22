import pyspark, os, json, cv2, configparser
import numpy as np
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import KafkaProducer
import base64

from faceDetector import detect

#add environment varible, since kafka does not come with pyspark
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.2 pyspark-shell'

#constants
CONFIG_FILE_NAME = 'stream-processor-prop.cfg'
config = configparser.ConfigParser()
config.read(CONFIG_FILE_NAME)
OUTPUT_DIR = config.get("Output","processed.dir")
KAFKA_URI = config.get('Kafka','bootstrap.server.produce')
KAFKA_TOPIC = config.get('Kafka','topic.produce')

# create producer used to send procces data back into kafka
producer = KafkaProducer(bootstrap_servers=KAFKA_URI)

def makeMsg(data):

    # get json object
    obj = data[1]

    # encode image
    _, buf = cv2.imencode('.jpg', obj['data'])
    byte_array = base64.b64encode(buf)

    data = {
        "cameraId": obj['cameraId'],
        "frameId": obj['frameId'],
        "timestamp": obj['timestamp'],
        "rows": obj['rows'],
        "cols": obj['cols'],
        "data": byte_array.decode('utf-8'),
        "face": obj['face']
    }

    return json.dumps(data).encode('utf-8')

def sendMsg(messages):
    """ Sends messages to kafka server"""

    # collect
    records = messages.collect()
    for record in records:

        # send message
        producer.send(KAFKA_TOPIC, record)

def gaussianKernel(sigma):
    """ Create gaussian filter with kernel of given length """

    gx = np.arange(-sigma*3, sigma*3)
    x, y = np.meshgrid(gx, gx)
    kernel = np.exp(-(np.square(x) + np.square(y)) / (2*np.square(sigma)))
    return kernel / np.sum(np.sum(kernel))

def convolute(package):
    """ Convolute the data in package with a gaussian filter """

    img = package[1]['data']
    # Padded fourier transform, with the same shape as the image
    kernel_ft = np.fft.fft2(kernel, s=img.shape[:2])
    # convolve
    img_ft = np.fft.fft2(img, axes=(0, 1))

    # the 'newaxis' is to match to color dimension
    imgKer_ft = img_ft * kernel_ft[:, :, np.newaxis]
    img = np.fft.ifft2(imgKer_ft,axes=(0, 1)).real
    img = np.array(img, dtype = np.uint8 )
    package[1]['data'] = img
    return package

def saveOutput(rdd):
    """ Save rdd contents to file"""

    if not rdd.isEmpty():
        fileDir = OUTPUT_DIR+str(rdd.id())+'.txt'
        rdd.saveAsTextFile(fileDir)

def run(argv=None):
    """ Spark pipeline"""

    #Initiate spark
    conf = SparkConf().setMaster("local[*]").setAppName("stream-processor")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, int(config.get("Kafka","interval")))

    #Initiate kafka properties
    brokers = config.get("Kafka","bootstrap.server.consume")
    topic = config.get("Kafka","topic.consume")

    # create kafka stream
    kvs = KafkaUtils.createStream(ssc, brokers, "spark-streaming-consumer", {topic:16})
    package = kvs.map(lambda x: (json.loads(x[1])['cameraId'], json.loads(x[1])))
    processedImages = package.map(detect)
    blurredImages = processedImages.map(convolute)

    # send all frames to stream viewer
    live_stream = blurredImages.map(makeMsg).foreachRDD(sendMsg)

    # filter faces only and save to text
    filteredImages = blurredImages.filter(lambda data: data[1]['face'] == 1) \
                    .groupByKey()
    output = filteredImages.map(lambda x: (x[0], len(x[1]))) \
                        .foreachRDD(saveOutput)

    ssc.start()
    ssc.awaitTermination()


if __name__ == '__main__':

    # create kernel object for convolute
    kernel = gaussianKernel(2)

    # run spark
    run()
