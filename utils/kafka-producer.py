from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='34.90.40.186:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

message = "Here is a message"

data = {
"message": message,
}

producer.send('test', data)
