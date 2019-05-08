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
    # Draw a rectangle around the faces
    for (x, y, w, h) in faces:
        cv2.rectangle(image, (x, y), (x+w, y+h), (0, 255, 0), 2)

def detect(data):
    #get image matrix
    image_buffer = getMat(data)
    #find faces
    findFace(image_buffer)
    return image_buffer
    #display image for the lulz

if __name__ == '__main__':
    run()

    """
    while(True):
        cv2.imshow("image", image_buffer)
        if cv2.waitKey(25) & 0xFF == ord('q'):
            break
    """
