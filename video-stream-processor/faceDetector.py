import numpy as np
import cv2
import base64
import json


def getMat(data):
    mat = base64.b64decode(data)
    mat = np.frombuffer(mat, dtype=np.uint8)
    mat = cv2.imdecode(mat, flags=1)
    return mat

def findFace(image):
    faceCascade = cv2.CascadeClassifier('haarcascade_frontalface_default.xml')
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

    # Detect faces in the image
    faces = faceCascade.detectMultiScale(
        gray,
        scaleFactor=1.3,
        minNeighbors=5,
        minSize=(30, 30),
        flags = cv2.CASCADE_SCALE_IMAGE
    )
    if faces == ():
        return False
    # Draw a rectangle around the faces
    for (x, y, w, h) in faces:
        cv2.rectangle(image, (x, y), (x+w, y+h), (0, 255, 0), 2)
    return True

def detect(data):
    #get image matrix
    image_buffer = getMat(data['data'])
    #find faces
    if findFace(image_buffer):
        print(type(image_buffer))
        #save img to cloud
    data['data'] = image_buffer
    return data
    #display image for the lulz
