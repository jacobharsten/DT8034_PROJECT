import kafka
import base64
import cv2
import asyncio
import threading
import time
import json
from kafka import KafkaConsumer
import numpy as np

"""
En tr8d f0r consuming kafka och en f0r att k0ra video streamen
"""

KAFKA_URI = '34.90.40.186:9092'
KAFKA_MSG_BUFFER = asyncio.Queue()
FRAME_BUFFER = asyncio.Queue()
TOPIC = 'viewer'

def consume():

    consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers = KAFKA_URI,
            auto_offset_reset = 'latest',
            max_partition_fetch_bytes=2097152
            )
    for msg in consumer:

        value = msg.value.decode('utf8')
        jsonMsg = json.loads(value)
        frameRaw = jsonMsg['data']
        jpg_as_np = np.frombuffer(base64.b64decode(frameRaw), dtype=np.uint8);
        image_buffer = cv2.imdecode(jpg_as_np, flags=1)

        FRAME_BUFFER.put_nowait(image_buffer)

    consumer.close()

def startLiveStream():

    # load buffer images
    buffer_images = []
    for i in range(7):

        img = cv2.imread('bufferimages/buffer{}.png'.format(i+1))
        img = cv2.resize(img, (600, 600))
        buffer_images.append(img)

    buffer_disp_count = 0
    display = False

    while True:

        print('Buffer health:{0}'.format(FRAME_BUFFER.qsize()))
        if display:

            # detta funkar f0r att testa, kanske kan anv7nda oss ab pyqt??
            frame = FRAME_BUFFER.get_nowait()
            cv2.imshow('livestream', frame)

            cv2.waitKey(85)
        else:
            cv2.imshow('livestream', buffer_images[buffer_disp_count])
            buffer_disp_count = (buffer_disp_count+1) % 7

            #adjust for 20/30 fps or whatever
            cv2.waitKey(120)


        # fill buffer conditions
        if FRAME_BUFFER.qsize() < 20:
            display = False

        elif FRAME_BUFFER.qsize() > 100:
            display = True


if __name__ == '__main__':

    consumeThread = threading.Thread(target=consume)
    consumeThread.start()
    startLiveStream()
    # add clean up for when livestream ends
