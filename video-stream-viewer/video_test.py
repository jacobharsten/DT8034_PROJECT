import cv2
import numpy as np
import sys

if __name__ == '__main__':

    cap = cv2.VideoCapture('output.avi')

    if not cap.isOpened():
        print('Error while opening videofile')
        sys.exit()

    while cap.isOpened():

        ret, frame = cap.read()
        if ret:

            cv2.imshow('Frame', frame)
        else:
            break

        if cv2.waitKey(25) & 0xFF == ord('q'):
            break
    cap.release()
    cv2.destorAllWindows()