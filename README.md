# DT8034 Project 2019
Image analysis using Apache Spark & Apache Kafka.

**SPARK VERSION:** [2.3.2]

## Kafka Config
> Currently running on a single node.

**Kafka version:** [2.1.1] 

**IP:** 34.90.222.198 

**PORT**: 9092 (ZooKeeper port 2181)

## Installation
> Dependencies can be found at the end of this README file.

### Running locally:
```bash
spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.2
```

### Running in cloud:

**Create cluster**

```bash
gcloud dataproc clusters create my-cluster \
    --image-version 1.3 \
    --metadata 'MINICONDA_VARIANT=3' \
    --metadata 'MINICONDA_VERSION=latest' \
    --metadata 'CONDA_PACKAGES=opencv=3.4.2' \
    --metadata 'PIP_PACKAGES=pyspark==2.3.2 kafka-python==1.4.6 google-cloud-storage==1.15.0' \
    --initialization-actions \
    gs://dataproc-initialization-actions/conda/bootstrap-conda.sh,gs://dataproc-initialization-actions/conda/install-conda-env.sh
```

**Upload files**

The following files need to be uploaded to Google Cloud: 
* faceDetector.py 
* stream-processor-prop.cfg 
* haarcascade_frontalface_default.xml 
* streamProcessor.py 

**Run in cloud**
```bash
gcloud dataproc jobs submit pyspark --py-files faceDetector.py,stream-processor-prop.cfg,haarcascade_frontalface_default.xml streamProcessor.py --cluster=my-cluster --properties spark.jars.packages=org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.2
```


## video-stream-collector

This component will handle data collection from file or camera and push the data to our Kafka endpoint. This serves as our **producer**. 

## video-stream-processor

This component will handle the processing of the data and serves as our **consumer**. It will be running as a Spark application and **subscribe** to our kafka topic, process the data in smaller batches and output it to our bucket. 

## video-stream-viewer

This component will display the proccesed frames, either from spark directly or via kafka. OpenCV is used to display the frames but this should be changed to some option where the user can select which stream to watch etc.

## utils

This component only contains some simple scripts to consume/produce message in our Kafka Broker. 

## Dependencies
**On machine:**  
Java 8.1  
Python 3.7  
**In Python environment:**  
opencv-python=4.1.0  
kafka-python=1.4.6  
pyspark=2.3.2  
numpy=1.16.3  
