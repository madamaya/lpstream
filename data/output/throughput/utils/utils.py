import os
import glob
import numpy as np
import matplotlib.pyplot as plt

def getLine(filePath):
    with open(filePath) as f:
        return f.readline().replace("\n", "")

def extractTsAndTupleNum(filePath):
    elements = getLine(filePath).split(",")
    return int(elements[0]), int(elements[1]), int(elements[3])

def getFileNames(dirPath):
    files = glob.glob("{}/*.log".format(dirPath))
    fileSet = set()
    for file in files:
        fileSet.add(os.path.basename(file.split("_")[0]))
    return fileSet

def calcResults(queries, approaches):
    results = {}
    for query in queries:
        for approach in approaches:
            thList = []
            for file in getFileNames("{}/{}".format(query, approach)):
                startTsMin = -1
                endTsMax = -1
                allTupleNum = 0
                fileList = []
                for p in glob.glob("{}/{}/*{}*.log".format(query, approach, file)):
                    fileList.append(p)
                    # read log data
                    print("*** Read log data ({}) ***".format(p))
                    startTs, endTs, tupleNum = extractTsAndTupleNum("{}".format(p))
                    startTsMin = startTs if (startTsMin < 0) else min(startTsMin, startTs)
                    endTsMax = endTs if (endTsMax < 0) else max(endTsMax, endTs)
                    allTupleNum = allTupleNum + tupleNum
                thList.append(tupleNum / ((endTs - startTs) // 1e9))

                print("p = {}".format(fileList))

            thNpList = np.array(thList)
            thMean = thNpList.mean()
            thSed = thNpList.std()

            if query not in results:
                results[query] = {}
            results[query][approach] = [thMean, thSed, thNpList.size]

    return results

def resultFigsGen(results, queries, approaches, flag):
    for query in queries:
        resultsList = [results[query][approach][0] for approach in approaches]
        colorList = []
        for approach in approaches:
            if approach == "baseline":
                colorList.append("b")
            elif approach == "genealog":
                colorList.append("g")
            elif approach == "l3stream":
                colorList.append("r")

        plt.bar(range(len(resultsList)), resultsList, tick_label=approaches, color=colorList)
        plt.title("*{}* result (Throughput, {})".format(query, flag))
        plt.ylabel("Latency")
        plt.savefig("./{}.pdf".format(query))
        plt.close()

def writeResults(results, queries, approaches, startTime, flag):
    with open("throughput.{}.result.{}.txt".format(flag, startTime), "w") as w:
        # Write mean
        w.write("MEAN,{}\n".format(",".join(approaches)))
        for query in queries:
            w.write("{},".format(query))

            means = []
            for approach in approaches:
                means.append(str(results[query][approach][0]))
            w.write("{}\n".format(",".join(means)))

        w.write("\n")

        # Write std
        w.write("STD,{}\n".format(",".join(approaches)))
        for query in queries:
            w.write("{},".format(query))

            stds = []
            for approach in approaches:
                stds.append(str(results[query][approach][1]))
            w.write("{}\n".format(",".join(stds)))

        w.write("\n")

        # Write cnt
        w.write("CNT,{}\n".format(",".join(approaches)))
        for query in queries:
            w.write("{},".format(query))

            stds = []
            for approach in approaches:
                stds.append(str(results[query][approach][2]))
            w.write("{}\n".format(",".join(stds)))