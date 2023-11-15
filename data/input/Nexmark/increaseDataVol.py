import sys
import time
import json
from datetime import datetime
from datetime import timedelta

loop = 1

if len(sys.argv) == 5:
    filePath = sys.argv[1]
    loop = int(sys.argv[2])
    print(sys.argv[3])
    startDate = datetime.strptime(sys.argv[3], "%Y-%m-%d %H:%M:%S.%f")
    endDate = datetime.strptime(sys.argv[4], "%Y-%m-%d %H:%M:%S.%f")
    duration = endDate - startDate + timedelta(milliseconds=1)
else:
    assert 1 == 0, len(sys.argv)

print("*** Start dataGen ****")
with open(filePath + ".increased", "w") as w:
    stime = time.time()
    ptime = time.time()

    for i in range(0, loop):
        count = 0
        with open(filePath) as f:
            while True:
                line = f.readline()
                if line == "":
                    print("\rloop={}, count={}".format(loop, count))
                    print("this loop: {} [s], all: {} [s]".format(time.time()-ptime, time.time()-stime))
                    ptime = time.time()
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

                jdata[type]["dateTime"] = (datetime.strptime(jdata[type]["dateTime"], "%Y-%m-%d %H:%M:%S.%f") + duration * i).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                w.write(json.dumps(jdata) + "\n")

                if count % 10000 == 0:
                    print("\rloop={}, count={}".format(loop, count), end="")

print("*** END: {}[s] ***".format(time.time() - stime))
