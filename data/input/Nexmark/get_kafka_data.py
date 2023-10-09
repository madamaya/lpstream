from kafka import KafkaConsumer

# To consume latest messages and auto-commit offsets
consumer = KafkaConsumer('nexmark',
                        auto_offset_reset='earliest',
                        group_id='my-group',
                        bootstrap_servers=['localhost:9092'])

count = 0
with open("nexmark.json", "w") as w:
  for message in consumer:
      # print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition, message.offset, message.key, message.value.decode('utf-8')))
      count = count + 1
      w.write(message.value.decode('utf-8') + "\n")

      if count % 1000000 == 0:
        print(count)

