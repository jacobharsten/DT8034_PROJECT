import cv2
import numpy as np
import base64
import json
import time
from kafka import KafkaProducer


# Used to rescale the images
def rescale_frame(frame, percent=75):
    width = int(frame.shape[1] * percent/ 100)
    height = int(frame.shape[0] * percent/ 100)
    dim = (width, height)
    return cv2.resize(frame, dim, interpolation =cv2.INTER_AREA)

# Create a VideoCapture object and read from input file
# If the input is the camera, pass 0 instead of the video file name
cap = cv2.VideoCapture("output.avi")

producer = KafkaProducer(bootstrap_servers='34.90.40.186:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Check if camera opened successfully
if (cap.isOpened()== False):
  print("Error opening video stream or file")

cnt = 0
# Read until video is completed
while(cap.isOpened()):
  # Capture frame-by-frame
  ret, frame = cap.read()
  if ret == True:

    frame50 = rescale_frame(frame, percent=50)

    # Fetch rows and cols of the frame
    rows, cols = frame50.shape[:-1]

    ret, buffer = cv2.imencode('.jpg', frame50)
    jpg_as_text = base64.b64encode(buffer)

    ts = time.time()

    data = {
    "cameraId": 1,
    "timestamp": ts,
    "rows": rows,
    "cols": cols,
    "data": jpg_as_text.decode('utf-8')
    }

    #TESTING PURPOSE FOR SPARK
    if(cnt == 0):
        producer.send('test', data)
        cnt = cnt + 1

    # Press Q on keyboard to  exit
    if cv2.waitKey(25) & 0xFF == ord('q'):
      break

  # Break the loop
  else:
    break

#print(data[data])
#for i in range(len(arr)):
#    jpg_original = base64.b64decode(arr[i])
#    jpg_as_np = np.frombuffer(jpg_original, dtype=np.uint8);
#    image_buffer = cv2.imdecode(jpg_as_np, flags=1)
#    decodedArr.append(image_buffer)


#jpg_original = base64.b64decode(pung['data'])
#jpg_as_np = np.frombuffer(jpg_original, dtype=np.uint8);
#image_buffer = cv2.imdecode(jpg_as_np, flags=1)

cap.release()

# Closes all the frames
cv2.destroyAllWindows()
