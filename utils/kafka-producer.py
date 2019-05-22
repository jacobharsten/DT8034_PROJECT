from kafka import KafkaProducer
import configparser

if __name__ = '__main__':

    # read config
    conf = configparser.ConfigParser()
    conf.read('../config.cfg')
    kafka_topic = conf.get('Kafka', 'topic.in')
    kafka_srv = conf.get('Kafka', 'bootstrap_servers')
    val_serial = lambda v: json.dumps(v).encode('utf-8')

    # create producer
    producer = KafkaProducer(bootstrap_servers=kafka_srv,
                             value_serializer=val_serial)

    # message to send
    data = {
    "message": 'Here is a message',
    }

    # send
    producer.send(kafka_topic, data)
