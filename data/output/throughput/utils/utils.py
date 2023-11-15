import os
import glob
import numpy as np
import matplotlib.pyplot as plt
from scipy import stats
import itertools

def logDump(query, approach, thList, startTime):
    with open("./results/throughput.info.{}.log".format(round(startTime)), "a") as w:
        w.write("{}\n".format(query))
        w.write("{}\n".format(approach))
        for i in range(len(thList)):
            w.write("{},avg=,{}\n".format(i+1, thList[i]))
        npMeansArray = np.array(thList)
        w.write("{}\n".format(npMeansArray.mean()))
        w.write("{}\n".format(npMeansArray.std()))
        w.write("\n")

def logDumpWelch(query, approaches, startTime, allList):
    print(allList)
    bonferroniCoef = (len(approaches) * (len(approaches) - 1)) // 2
    with open("./results/throughput.info.{}.log".format(round(startTime)), "a") as w:
        w.write("===== Welch results ({}) =====\n".format(query))
        for pair in itertools.combinations(approaches, 2):
            #for i in range(len(allList[approaches[0]])):
            ret = stats.ttest_ind(allList[pair[0]], allList[pair[1]], equal_var=False)
            fixedPvalue = ret.pvalue * bonferroniCoef
            if fixedPvalue <= 0.01:
                resultFlag = "diff (0.01)"
            elif 0.01 < fixedPvalue and fixedPvalue <= 0.05:
                resultFlag = "diff (0.05)"
            else:
                resultFlag = "nodiff"
            w.write("{}-{}:{}: {} ({})\n".format(pair[0], pair[1], resultFlag, str(ret), bonferroniCoef))
        w.write("=================================\n")
        w.write("\n")

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

def calcResults(queries, approaches, startTime):
    results = {}
    for query in queries:
        allList = {}
        for approach in approaches:
            thList = []
            allDuration = 0
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
                thList.append(allTupleNum / ((endTs - startTs) // 1e9))
                allDuration = allDuration + (endTs - startTs)

                print("p = {}".format(fileList))

            # dump log
            print("*** Dump log ***")
            logDump(query, approach, thList, startTime)
            allList[approach] = thList

            thNpList = np.array(thList)
            thMean = thNpList.mean()
            thSed = thNpList.std()

            if query not in results:
                results[query] = {}
            results[query][approach] = [thMean, thSed, thNpList.size, allDuration]

        print("*** Welch test ***")
        logDumpWelch(query, approaches, startTime, allList)

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
        plt.ylabel("Throughput [tuples/s]")
        plt.savefig("./results/{}.pdf".format(query))
        plt.close()

def writeResults(results, queries, approaches, startTime, flag):
    with open("./results/throughput.{}.result.{}.txt".format(flag, startTime), "w") as w:
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

        w.write("\n")

        # Write duration
        w.write("DURATION,{}\n".format(",".join(approaches)))
        for query in queries:
            w.write("{},".format(query))

            stds = []
            for approach in approaches:
                stds.append(str(results[query][approach][3] // 1e9))
            w.write("{}\n".format(",".join(stds)))