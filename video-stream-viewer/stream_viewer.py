import kafka
import base64
import cv2
import asyncio
import threading
import time
import json
from kafka import KafkaConsumer
import numpy as np
import configparser


# constants
conf = configparser.ConfigParser()
conf.read('../config.cfg')
KAFKA_URI = conf.get('Kafka', 'server')
KAFKA_MSG_BUFFER = asyncio.Queue()
FRAME_BUFFER = asyncio.Queue()
TOPIC = conf.get('Kafka', 'topic.out')

def consume():
    """ Consume messages from kafka. """

    consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers = KAFKA_URI,
            auto_offset_reset = 'latest',
            max_partition_fetch_bytes=2097152
            )
    # this loop will continue even if there are no msg at a given time
    buf = []
    for msg in consumer:

        value = msg.value.decode('utf8')
        jsonMsg = json.loads(value)
        frameRaw = jsonMsg['data']
        frameId = jsonMsg['frameId']
        jpg_as_np = np.frombuffer(base64.b64decode(frameRaw), dtype=np.uint8);
        image_buffer = cv2.imdecode(jpg_as_np, flags=1)

        if len(buf) > 100:
            buf.sort(key=lambda x:x[0])
            for fr in buf:
                FRAME_BUFFER.put_nowait(fr[1])
            buf = []
        buf.append((frameId, image_buffer))

    consumer.close()

def startLiveStream():
    """ Livestream loop that display frames retrieved from kafka. """

    # load buffer images
    buffer_images = []
    for i in range(7):

        img = cv2.imread('bufferimages/buffer{}.png'.format(i+1))
        img = cv2.resize(img, (600, 600))
        buffer_images.append(img)

    # keep track of next buffer image
    buffer_disp_count = 0

    # flag for frame/buffer display
    display = False

    while True:

        print('Frame buffer size:{0}    '.format(FRAME_BUFFER.qsize()), end='\r')
        if display:

            # get next frame in queue
            frame = FRAME_BUFFER.get_nowait()
            cv2.imshow('livestream', frame)

            # added delay to keep steady framerate
            cv2.waitKey(35)
        else:

            # display next buffer images
            cv2.imshow('livestream', buffer_images[buffer_disp_count])
            buffer_disp_count = (buffer_disp_count+1) % 7

            # added delay to keep steady framerate
            cv2.waitKey(150)


        # to keep a steady stream buffer when low on frames
        if FRAME_BUFFER.qsize() < 20:
            display = False

        elif FRAME_BUFFER.qsize() > 100:
            display = True


if __name__ == '__main__':

    # run kafka consumer and stream on seperate threads
    consumeThread = threading.Thread(target=consume)
    consumeThread.start()
    startLiveStream()
