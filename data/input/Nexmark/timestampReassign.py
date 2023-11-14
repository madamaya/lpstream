import sys
import json
from datetime import datetime
from datetime import timedelta

if __name__ == "__main__":
    assert len(sys.argv) == 3, str(len(sys.argv)) + " != 2"

    filePath = sys.argv[1]
    throughput = int(sys.argv[2])

    incrementSize = 1000000000 // throughput
    assert incrementSize > 0, str(incrementSize) + " <= 0"

    count = 0
    currentTime = datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
    with open(filePath) as f:
        with open(filePath + ".reassign", "w") as w:
            while True:
                line = f.readline()
                if line == "":
                    break
                count += 1
                jdata = json.loads(line)

                type = ""
                if jdata["event_type"] == 1:
                    type = "auction"
                elif jdata["event_type"] == 2:
                    type = "bid"
                else:
                    type = "person"

                jdata[type]["dateTime"] = currentTime.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                currentTime += timedelta(microseconds=incrementSize//1000)
                elements = line.split(",")
                w.write(json.dumps(jdata) + "\n")
                if count % 10000 == 0:
                    print(count)

    print(count)