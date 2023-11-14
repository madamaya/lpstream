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
    currentTime = 0
    with open(filePath) as f:
        with open(filePath + ".reassign", "w") as w:
            while True:
                line = f.readline()
                if line == "":
                    break
                count += 1
                currentTime += incrementSize
                elements = line.split(",")
                lst = elements[:1] + [str(currentTime // 1000000000)] + elements[2:]
                w.write(",".join(lst))
                if count % 10000 == 0:
                    print(count)

    print(count)