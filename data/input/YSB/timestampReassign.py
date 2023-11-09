import sys
import json
from datetime import datetime
from datetime import timedelta

if __name__ == "__main__":
    assert len(sys.argv) == 3, str(len(sys.argv)) + " != 2"

    filePath = sys.argv[1]
    throughput = int(sys.argv[2])

    incrementSize = 1000000 // throughput
    assert incrementSize > 0, str(incrementSize) + " <= 0"

    count = 0
    currentTime = 1672498800000000
    with open(filePath) as f:
        with open(filePath + ".reassign", "w") as w:
            while True:
                line = f.readline()
                if line == "":
                    break
                count += 1
                #print(",".join(line.split(",")[:2] + [currentTime.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]] + line.split(",")[3:]))
                jdata = json.loads(line)
                jdata["event_time"] = currentTime // 1000
                currentTime += incrementSize
                w.write(json.dumps(jdata) + "\n")
                if count % 10000 == 0:
                    print(count)

    print(count)