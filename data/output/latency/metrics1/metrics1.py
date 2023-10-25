import time
import glob
import numpy as np
import matplotlib.pyplot as plt

filterRate = 0.1
plotLatency = True
queries = ["LR", "Nexmark", "NYC", "YSB"]
approaches = ["baseline", "genealog", "l3stream"]
#approaches = ["baseline", "genealog"]
startTime = time.time()

def logDump(query, approach, meanList, stdList):
    with open("metrics1.info.{}.log".format(round(startTime)), "a") as w:
        w.write("{}\n".format(query))
        w.write("{}\n".format(approach))
        for i in range(len(meanList)):
            w.write("{},avg=,{},std=,{}\n".format(i+1, meanList[i], stdList[i]))
        npMeansArray = np.array(meanList)
        w.write("{}\n".format(npMeansArray.mean()))
        w.write("{}\n".format(npMeansArray.std()))
        w.write("\n")

def calcResults():
    results = {}
    for query in queries:
        for approach in approaches:
            meanList = []
            stdList = []
            for l in glob.glob("{}/{}/*.log".format(query, approach)):
                # read log data
                print("*** Read log data ({}) ***".format(l))
                nparray = np.loadtxt("{}".format(l), dtype="int64")
                filteredTuple = round(nparray.size * filterRate)

                # extract valid data
                print("*** Extract valid data ***")
                validarray = nparray[filteredTuple:(nparray.size - filteredTuple)]

                # calc mean and std
                print("*** Calc mean ***")
                mean = validarray.mean()
                print("*** Calc std ***")
                std = validarray.std()
                meanList.append(mean)
                stdList.append(std)

                if plotLatency:
                    # plot all latencies
                    print("*** Plot latencies ***")
                    plt.figure()
                    plt.plot(range(validarray.size), validarray, linewidth=0.1)
                    print("*** Save fig ***")
                    plt.savefig("{}.pdf".format(l))
                    plt.close()

            # dump log
            print("*** Dump log ***")
            logDump(query, approach, meanList, stdList)

            print("*** Calc result ({},{}) ***".format(query, approach))
            meanNpList = np.array(meanList)
            allMean = meanNpList.mean()
            allStd = meanNpList.std()

            if query not in results:
                results[query] = {}
            results[query][approach] = [allMean, allStd, meanNpList.size]

    return results


def writeResults(results):
    with open("metrics1.result.{}.txt".format(startTime), "w") as w:
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


if __name__ == "__main__":
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
