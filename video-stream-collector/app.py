import cv2
import numpy as np
import base64
import json
import time
from kafka import KafkaProducer
import configparser
import sys


def rescale_frame(frame, percent=75):
    # Used to rescale the images

    width = int(frame.shape[1] * percent/ 100)
    height = int(frame.shape[0] * percent/ 100)
    dim = (width, height)
    return cv2.resize(frame, dim, interpolation =cv2.INTER_AREA)


if __name__ == '__main__':

    # read config
    conf = configparser.ConfigParser()
    conf.read('../config.cfg')
    kafka_topic = conf.get('Kafka', 'topic.in')
    kafka_srv = conf.get('Kafka', 'server')

    # make value serializer
    val_serial = lambda v: json.dumps(v).encode('utf-8')

    print('streaming video, press q to exit')
    # Create a VideoCapture object and read from input file
    # If the input is the camera, pass 0 instead of the video file name
    cap = cv2.VideoCapture(0)
    #cap = cv2.VideoCapture('output.avi')

    # Create kafka producer
    producer = KafkaProducer(bootstrap_servers=kafka_srv,
                             value_serializer=val_serial,
                             acks='all')

    # Check if camera opened successfully
    if cap.isOpened() == False:
        print("Error opening video stream or file")
        sys.exit()

    # for logging
    logfile = open('log_test.txt', 'w')
    frameid = 0

    # Read until video is completed
    while cap.isOpened():

        # Capture frame-by-frame
        ret, frame = cap.read()
        if ret == True:

            # current image size is too big, make smaller
            frame50 = rescale_frame(frame, percent=25)

            # Fetch rows and cols of the frame
            rows, cols = frame50.shape[:-1]

            #encode jpg format and pass as base64
            ret, buffer = cv2.imencode('.jpg', frame50)
            jpg_as_text = base64.b64encode(buffer)

            # create a timestamp
            ts = time.time()

            # create json structure
            data = {
                "cameraId": 1,
                "frameId": frameid,
                "timestamp": ts,
                "rows": rows,
                "cols": cols,
                "data": jpg_as_text.decode('utf-8'),
                "face": 0
            }

            #send data to kafka server
            producer.send(kafka_topic, data)

            # Press Q on keyboard to exit
            if cv2.waitKey(25) & 0xFF == ord('q'):
                break
            logfile.write(' '.join([str(ts), str(frameid)+'\n']))
            frameid += 1

        # no frame, exit loop
        else:
            break

    # Cleanup when done
    logfile.close()
    cap.release()
    cv2.destroyAllWindows()
