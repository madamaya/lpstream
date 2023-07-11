import sys
from kafka import KafkaConsumer

arguments = sys.argv
assert len(arguments) == 4
output_topic_name = arguments[1]
output_file_dir = arguments[2]
test_code = arguments[3]

#output_topic_name = "linearroadA-o"
#output_file_dir = "/Users/yamada-aist/workspace/l3stream/data/output/linearroadA"
#test_code = "2"

topic_name = "linearroadA-o"

consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'])
consumer.subscribe([topic_name])

read_count = 0
try:
    with open("{}/{}_{}.txt".format(output_file_dir, topic_name, test_code), "w") as f:
        print("Waiting")
        for record in consumer:
            line = record.value.decode("utf-8") + "\n"
            if ("\0" in line):
                pass
            else:
                f.write(line)
                f.flush()
                read_count = read_count + 1
            if read_count % 10 == 0:
                print("{} records have been received.".format(read_count))
finally:
    print("END::: {} records have been received.".format(read_count))