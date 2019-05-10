# DT8034 Project 2019
Our cool git for the project in the course DT8034 for applying face-recognition from multiple inputs using Apache Spark.  

**TODO:**

- [x] Set up working Kafka Broker that can produce and consume messages.
- [x] Set up a video-collector that can encode our image and push to Kafka.
- [x] Set up a Spark Application that can consume messages from our broker.
- [x] Decode the data within Spark, apply face recognition on small batches and sort by camera-id and timestamp. 
- [x] Output the data in a Google Cloud Bucket. 
- [ ] Fix kernal in convolution to support 'same' approach.
- [ ] Fix some sort of output for execution purposes.
- [ ] Post information to a topic for video-stream-visualisation.
- [ ] Only push images that face a face detected. 
- [ ] Test run with 1 producer.
- [ ] Test with several producers. 


**SPARK VERSION:** [2.3.2]


## Kafka
Currently running on a single node (might need to change to cluster)

**Kafka version:** [2.1.1] 

**IP:** 34.90.40.186 

**PORT**: 9092 (ZooKeeper port 2181) 
	


## video-stream-collector

This component will handle data collection from file or camera and push the data to our Kafka endpoint. This serves as our **producer**. 

## video-stream-processor

This component will handle the processing of the data and serves as our **consumer**. It will be running as a Spark application and **subscribe** to our kafka topic, process the data in smaller batches and output it to our bucket. 

## video-stream-viewer

This component will display the proccesed frames, either from spark directly or via kafka. OpenCV is used to display the frames but this should be changed to some option where the user can select which stream to watch etc.

## utils

This component only contains some simple scripts to consume/produce message in our Kafka Broker. 


## License

ⓒOB CORP
