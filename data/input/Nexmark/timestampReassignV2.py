import sys
import json
import time
from datetime import datetime
from datetime import timedelta

if __name__ == "__main__":
    assert len(sys.argv) == 4, str(len(sys.argv)) + " != 4"

    filePath = sys.argv[1]
    throughput = int(sys.argv[2])
    loopNum = int(sys.argv[3])

    incrementSize = 1000000000 // throughput
    assert incrementSize > 0, str(incrementSize) + " <= 0"

    stime = time.time()
    ptime = time.time()

    allCount = 0
    oneCount = 0
    currentTime = datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
    with open(filePath + ".reassignv2", "w") as w:
        for loop in range(loopNum):
            count = 0
            with open(filePath) as f:
                while True:
                    line = f.readline()
                    if line == "":
                        print("\rloop={}, count={} [end]".format(loop, count))
                        allCount += count
                        oneCount = count
                        break
                    count += 1

                    head = line.split("\"dateTime\"")
                    tail = head[1].split(",")
                    dateTime = currentTime.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                    resultLine = head[0] + "\"dateTime\":\"{}\",{}".format(dateTime, ",".join(tail[1:]))
                    currentTime += timedelta(microseconds=incrementSize//1000)
                    w.write(resultLine)
                    if count % 10000 == 0:
                        if allCount == 0:
                            print("\rloop={}, count={}".format(loop, count), end="")
                        else:
                            print("\rloop={}, count={} (this loop:{}%, all:{}%)".format(loop, count, count * 100 // (allCount//loop), (loop * oneCount + count) * 100 // (oneCount*loopNum)), end="")
            print("this loop ({}): {} [s]".format(loop + 1, time.time() - ptime))
            print("all: {} [s]".format(time.time() - stime))
            ptime = time.time()

    print("{} data generated.".format(allCount))
    print("all: {} [s]".format(time.time() - stime))