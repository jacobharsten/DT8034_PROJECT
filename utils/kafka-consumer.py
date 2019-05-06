from kafka import KafkaConsumer

consumer = KafkaConsumer('test', bootstrap_servers='34.90.40.186:9092')
for msg in consumer:
     print (msg)
