import numpy as np
import cv2
import base64
import json


def getMat(data):
    """ Convert base64 data to a cv2 image """ 

    mat = base64.b64decode(data)
    mat = np.frombuffer(mat, dtype=np.uint8)
    mat = cv2.imdecode(mat, flags=1)
    return mat

def findFace(image):
    """ Find faces in image, if there are faces draw
        rectangles around them and return true else return 
        false. """ 

    # create face classifer
    faceCascade = cv2.CascadeClassifier('haarcascade_frontalface_default.xml')
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

    # Detect faces in the image, the parameters are the default values set by opencv
    faces = faceCascade.detectMultiScale(
        gray,
        scaleFactor=1.3,
        minNeighbors=5,  
        minSize=(30, 30),
        flags = cv2.CASCADE_SCALE_IMAGE
    )
    # no faces were found 
    if faces == ():
        return False
    
    # Draw a rectangle around the faces
    for (x, y, w, h) in faces:
        cv2.rectangle(image, (x, y), (x+w, y+h), (0, 255, 0), 2)
    
    return True

def saveFile(data):
    """ Write image to .jpg file """ 

    img_name = 'images/face-'+str(data['timestamp'])+'.jpg'
    cv2.imwrite(img_name, data['data'])
    return data

def detect(data):
    """ Run face detection on image and set flags in json object """ 
    
    #get image matrix
    image_buffer = getMat(data['data'])
    
    #find faces
    if findFace(image_buffer):
        data['face'] = 1
    
    data['data'] = image_buffer
    return data
