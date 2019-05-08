import kafka
import cv2
import asyncio
import threading
import time
from kafka import KafkaConsumer

"""
En tr8d f0r consuming kafka och en f0r att k0ra video streamen
"""

KAFKA_URI = '0.0.0.0:0000'
KAFKA_MSG_BUFFER = asyncio.Queue()
FRAME_BUFFER = asyncio.Queue()


""" UTIL? """

def parseKafkaMessages(messages):
    """ Parsa JSON till opencv frame typ """
    for msg in KAFKA_MSG_BUFFER:
        
        # kommer frames i ordning?
        frame = msg.frame
        FRAME_BUFFER.put_nowait(frame)

def imagesToVideo(images):
    """ Kanske inte beh0vs"""
    pass



""" Consume functions """


def consumeKafkaMessages():
    """ tr8d func """
        
    # consume loop, hackig af
    while True:

        consume()
        parseKafkaMesasges()
        
        # 0.5 ar helt random 
        time.sleep(0.5)


def consume():
    consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers = KAFKA_URI,
            auto_offset_reset = 'latest'
            )

    for msg in consumer:
        KAFKA_MSG_BUFFER.put(msg)

def startLiveStream():
    
    # load buffer images
    buffer_images = []
    for i in range(7):
        buffer_images[i] = cv2.imread('bufferimages/buffer{}.png'.format(i+1))
    
    buffer_disp_count = 0
    while True:

        if len(FRAME_BUFFER) > 100:

            # detta funkar f0r att testa, kanske kan anv7nda oss ab pyqt??
            frame = FRAME_BUFFER.get()
            cv2.imshow(frame)
        else:
            cv2.imshow(buffer_images[buffer_disp_count])
            buffer_disp_count = (buffer_disp_count+1) % 7
        
        
        #adjust for 20/30 fps or whatever
        time.sleep(0.2)


if __name__ == '__main__':
    

    consumeThread = Thread(target=consumeKafka)
    consumeThread.start()
    startLiveStream()
    # add clean up for when livestream ends

















