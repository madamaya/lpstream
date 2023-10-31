import sys
import datetime
import json
from kafka import KafkaConsumer

if len(sys.argv) == 2:
    tupleNum = int(sys.argv[1])
else:
    assert 1 == 0

# mapping作成
mp = {}
with open("pairs.csv") as f:
    while True:
        line = f.readline()
        if line == "":
            break

        elements = line.split(",")

        assert len(elements) == 2

        mp[elements[1].replace("\n", "")] = elements[0].replace("\n", "")

# To consume latest messages and auto-commit offsets
consumer = KafkaConsumer('ad-events',
                         auto_offset_reset='earliest',
                         group_id='my-group',
                         bootstrap_servers=['localhost:9092'])

count = 0
with open("../data/ysb.json", "w") as w:
    for message in consumer:
        # print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition, message.offset, message.key, message.value.decode('utf-8')))
        count = count + 1
        line = message.value.decode('utf-8')
        j_data = json.loads(line)
        j_data["campaign_id"] = mp[j_data["ad_id"]]
        w.write("{}\n".format(json.dumps(j_data)))

        if count % 100000 == 0:
            print("\r{} tuples has been generated.".format(count), end="")

        if count >= tupleNum:
            print(" [end]")
            break
