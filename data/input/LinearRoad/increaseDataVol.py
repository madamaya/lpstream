import sys
import time

loop = 1
tsShipt = 10800

if len(sys.argv) == 2:
    loop = int(sys.argv[1])

print("*** Read h1.csv ***")
with open("h1.csv") as f:
    _baseLines = f.readlines()

print("*** Create original data list ***")
baseLines = [line.rstrip("\n") for line in _baseLines]

print("*** Start dataGen ****")
with open("../data/lr.csv", "w") as w:
    stime = time.time()
    ptime = time.time()
    ctime = time.time()

    for i in range(0, loop):
        for line in baseLines:
            tmpLine = line.split(",")
            tmpLine[1] = str(int(tmpLine[1]) + tsShipt * i)
            line = ",".join(tmpLine)
            w.write("\"" + line + "\"\n")

        ctime = time.time()
        print("- Loop {} is finished.".format(i))
        print("- Duration from starting: {}[s]".format(ctime - stime))
        print("- Duration for this loop: {} [s]".format(ctime - ptime))
        ptime = ctime

print("*** END: {}[s] ***".format(time.time() - stime))
