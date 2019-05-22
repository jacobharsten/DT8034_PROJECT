from kafka import KafkaConsumer
import configparser


if __name__ == '__main__':

    # read config
    conf = configparser.ConfigParser()
    conf.read('../config.cfg')
    kafka_topic = conf.get('Kafka', 'topic.in')
    kafka_srv = conf.get('Kafka', 'bootstrap_servers')

    # create and run simple consumer
    consumer = KafkaConsumer(kafka_topic, bootstrap_servers=kafka_srv)
    count = 0
    for msg in consumer:
         count += 1
         print(count)
