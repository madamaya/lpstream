import sys
import json

assert len(sys.argv) == 2

filePath = sys.argv[1]

maxTs = -1
with open(filePath) as f:
    while True:
        line = f.readline()
        if line == "":
            break
        jdata = json.loads(line)
        ts = int(jdata["TS"])
        maxTs = max(maxTs, ts)

print("maxTs = {}".format(maxTs))