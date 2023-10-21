import sys
import json
from kafka import KafkaConsumer

def findMaxTsFromOut(filePath):
    maxTs = -1
    with open(filePath) as f:
        while True:
            line = f.readline()
            if line == "":
                break
            jdata = json.loads(line.replace("\\", ""))
            ts = int(jdata["TS"])
            maxTs = max(maxTs, ts)
    return maxTs

def getOutPerPart(topicName):
    consumer = KafkaConsumer(
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset="earliest",
        consumer_timeout_ms=5000)
    consumer.subscribe([topicName])

    read_count = 0
    partitionOut = {}
    for record in consumer:
        line = record.value.decode("utf-8")
        partition = record.partition

        if partition not in partitionOut:
            partitionOut[partition] = line
        else:
            partitionOut[partition] = line

        read_count = read_count + 1
        if read_count % 1000 == 0:
            print("\r{} records have been received.".format(read_count), end="")
    print("\rEND::: {} records have been received.".format(read_count))
    return partitionOut

if __name__ == "__main__":

    assert len(sys.argv) == 4

    fileDir = sys.argv[1]
    fileFile = sys.argv[2]
    topicName = sys.argv[3]

    outputMaxTs = findMaxTsFromOut(fileDir.split("-")[0] + "/" + fileFile)
    maxWM = getOutPerPart(topicName)

    print("######################################################")
    print("########### outputMaxTs = {} ###########".format(outputMaxTs))
    print("########### maxWM = {} ###########".format(maxWM))
    print("######################################################")
