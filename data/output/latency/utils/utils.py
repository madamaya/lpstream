import time
import glob
import numpy as np
import matplotlib.pyplot as plt

def logDump(query, approach, meanList, stdList, startTime, flag):
    with open("./results/latency.{}.info.{}.log".format(flag, round(startTime)), "a") as w:
        w.write("{}\n".format(query))
        w.write("{}\n".format(approach))
        for i in range(len(meanList)):
            w.write("{},avg=,{},std=,{}\n".format(i+1, meanList[i], stdList[i]))
        npMeansArray = np.array(meanList)
        w.write("{}\n".format(npMeansArray.mean()))
        w.write("{}\n".format(npMeansArray.std()))
        w.write("\n")

def calcResults(queries, approaches, filterRate, plotLatency, plotLatencyCmp, startTime, flag):
    results = {}
    for query in queries:
        if plotLatencyCmp:
            allValidList = {}
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
                print(nparray, type(nparray), l)
                if nparray.size != 1:
                    validarray = nparray[filteredTuple:(nparray.size - filteredTuple)]
                else:
                    validarray = nparray

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
                    plt.plot(range(validarray.size), validarray, linewidth=0.1)
                    if plotLatencyCmp:
                        if approach not in allValidList:
                            allValidList[approach] = []
                        allValidList[approach].append(validarray)

            if plotLatency:
                print("*** Save fig ***")
                plt.title("{}-{}".format(query, approach))
                plt.ylabel("Latency")
                plt.savefig("./results/figs/{}-{}.pdf".format(query, approach))
                plt.close()

            # dump log
            print("*** Dump log ***")
            logDump(query, approach, meanList, stdList, startTime, flag)

            print("*** Calc result ({},{}) ***".format(query, approach))
            meanNpList = np.array(meanList)
            allMean = meanNpList.mean()
            allStd = meanNpList.std()

            if query not in results:
                results[query] = {}
            results[query][approach] = [allMean, allStd, meanNpList.size]

        if plotLatencyCmp:
            print("*** Save fig for comparison ***")
            for i in range(len(allValidList[approaches[0]])):
                for approach in approaches:
                    plt.plot(range(allValidList[approach][i].size), allValidList[approach][i], linewidth=0.1)
                plt.title("{}-{}-comparison".format(query, i))
                plt.ylabel("Latency")
                plt.legend(approaches)
                plt.savefig("./results/figs/{}-{}-comparison.pdf".format(query, i))
                plt.close()

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
        plt.title("*{}* result (Latency, {})".format(query, flag))
        plt.ylabel("Latency")
        plt.savefig("./results/{}.pdf".format(query))
        plt.close()

def writeResults(results, queries, approaches, startTime, flag):
    with open("./results/latency.{}.result.{}.txt".format(flag, startTime), "w") as w:
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
