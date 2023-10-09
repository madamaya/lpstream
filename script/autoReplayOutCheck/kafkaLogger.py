import sys
import time
from kafka import KafkaConsumer

if __name__ == "__main__":
    assert len(sys.argv) == 3

    topicName = sys.argv[1] + "-o"
    filePath = sys.argv[2]

    consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'])
    consumer.subscribe([topicName])

    read_count = 0
    try:
        with open("{}".format(filePath), "w") as f:
            # print("Waiting")
            for record in consumer:
                line = record.value.decode("utf-8") + "\n"
                # print(line)
                if ("\0" in line):
                    pass
                else:
                    f.write(line)
                    f.flush()
                    read_count = read_count + 1
                if read_count % 1000 == 0:
                    print("{} records have been received.".format(read_count))
    finally:
        print("END::: {} records have been received.".format(read_count))

