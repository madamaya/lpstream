import sys
import time
import glob
import numpy as np
import matplotlib.pyplot as plt

filterRate = 0.1
plotLatency = True
queries = ["LR", "Nexmark", "NYC", "YSB"]
startTime = time.time()
windowSize = -1

def readMonitor(filePath):
    with open(filePath) as f:
        elements = f.readline().replace("\n", "").split(",")
    return int(elements[0]), int(elements[1])

def readTrigger(filePath):
    with open(filePath) as f:
        elements = f.readline().replace("\n", "").split(",")
    return int(elements[0]), int(elements[1]), int(elements[2])

def calcResults():
    results = {}
    for query in queries:
        metrics3List = []
        metrics4List = []
        metrics3StdList = []
        metrics4StdList = []
        files = glob.glob("{}/*-{}-*.log".format(query, windowSize))
        for i in range(1, len(files)//2 + 1):
            stTr, edTr, ed2Tr = readTrigger("{}/{}-{}-trigger.log".format(query, i, windowSize))
            stMo, edMo = readMonitor("{}/{}-{}-monitor.log".format(query, i, windowSize))

            met3 = edTr - stMo
            met4 = edMo - edTr

            metrics3List.append(met3)
            metrics4List.append(met4)

        metrics3NpList = np.array(metrics3List)
        metrics4NpList = np.array(metrics4List)

        results[query] = [metrics3NpList.mean(), metrics3NpList.std(), metrics4NpList.mean(), metrics4NpList.std()]

    return results


def writeResults(results):
    with open("metrics3.result.{}.{}.txt".format(startTime, windowSize), "w") as w:
        # Write mean
        w.write("DURATION,l3stream\n")
        for query in queries:
            w.write("{},{}\n".format(query, results[query][0]))

        w.write("\n")

        # Write std
        w.write("STD,l3stream\n")
        for query in queries:
            w.write("{},{}\n".format(query, results[query][1]))

    with open("metrics4.result.{}.{}.txt".format(startTime, windowSize), "w") as w:
        # Write mean
        w.write("DURATION,l3stream\n")
        for query in queries:
            w.write("{},{}\n".format(query, results[query][2]))

        w.write("\n")

        # Write std
        w.write("STD,l3stream\n")
        for query in queries:
            w.write("{},{}\n".format(query, results[query][3]))


if __name__ == "__main__":
    assert len(sys.argv) == 2
    windowSize = int(sys.argv[1])

    print("* calcResults() *")
    results = calcResults()

    print("* writeResults(results) *")
    writeResults(results)

    print(startTime)
    """
    try:
        nparray = np.loadtxt("hoge.log", dtype="int64")
    except OSError:
        print("hoge.log not found.")
    """
