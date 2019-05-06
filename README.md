# DT8034 Project 2019
Our cool git for the project in the course DT8034 for applying face-recognition from multiple inputs using Apache Spark.  

**TODO:**

- [x] Set up working Kafka Broker that can produce and consume messages.
- [x] Set up a video-collector that can encode our image and push to Kafka.
- [ ] Set up a Spark Application that can consume messages from our broker.
- [ ] Decode the data within Spark, apply face recognition on small batches and sort by camera-id and timestamp. 
- [ ] Output the data in a Google Cloud Bucket. 




## Kafka
Currently running on a single node (might need to change to cluster)

**Kafka version:** [2.1.1] 
**IP:** 34.90.40.186 
**PORT**: 9092 (ZooKeeper port 2181) 
	


## video-stream-collector

This component will handle data collection from file or camera and push the data to our Kafka endpoint. This serves as our **producer**. 

## video-stream-processor

This component will handle the processing of the data and serves as our **consumer**. It will be running as a Spark application and **subscribe** to our kafka topic, process the data in smaller batches and output it to our bucket. 

## utils

This component only contains some simple scripts to consume/produce message in our Kafka Broker. 






## UML diagrams

A very good looking diagram I created in case it is not clear.

```mermaid
sequenceDiagram
Producer ->> Kafka Broker: push data
Consumer->>Kafka Broker: fetch data

```

## License

ⓒOB CORP
