import time
import glob
import numpy as np
from scipy import stats
import matplotlib.pyplot as plt
import itertools

def markerSize(dataNum):
    if dataNum <= 100:
        return 10
    elif dataNum <= 1000:
        return 5
    elif dataNum <= 10000:
        return 1
    elif dataNum <= 100000:
        return 0.5
    else:
        return 0.1

def logDump(query, approach, meanList, stdList, startTime, flag, size):
    with open("./results/latency.{}.info.{}.{}.log".format(flag, round(startTime), size), "a") as w:
        w.write("{}\n".format(query))
        w.write("{}\n".format(approach))
        for i in range(len(meanList)):
            w.write("{},avg=,{},std=,{}\n".format(i+1, meanList[i], stdList[i]))
        npMeansArray = np.array(meanList)
        w.write("MEAN: {}\n".format(npMeansArray.mean()))
        w.write("STD: {}\n".format(npMeansArray.std()))
        w.write("\n")

def logDumpWelch(query, approaches, startTime, flag, allValidList, size):
    bonferroniCoef = (len(approaches) * (len(approaches) - 1)) // 2
    with open("./results/latency.{}.info.{}.{}.log".format(flag, round(startTime), size), "a") as w:
        w.write("===== Welch results ({}) =====\n".format(query))
        for pair in itertools.combinations(approaches, 2):
            for i in range(len(allValidList[approaches[0]])):
                ret = stats.ttest_ind(allValidList[pair[0]][i], allValidList[pair[1]][i], equal_var=False)
                fixedPvalue = ret.pvalue * bonferroniCoef
                if fixedPvalue <= 0.01:
                    resultFlag = "diff (0.01)"
                elif 0.01 < fixedPvalue and fixedPvalue <= 0.05:
                    resultFlag = "diff (0.05)"
                else:
                    resultFlag = "nodiff"

                w.write("{}-{}:{}:{}: {} ({})\n".format(pair[0], pair[1], i, resultFlag, str(ret), bonferroniCoef))
        w.write("=================================\n")
        w.write("\n")

def calcResults(queries, approaches, filterRate, plotLatency, plotLatencyCmp, violinPlot, startTime, flag, size):
    results = {}
    allValidList = {}
    for query in queries:
        if violinPlot == False:
            allValidList = {}
        allValidList[query] = {}
        for approach in approaches:
            meanList = []
            medianList = []
            stdList = []
            for idx in range(len(glob.glob("{}/{}/*_{}.log".format(query, approach, size)))):
                l = "{}/{}/{}_{}.log".format(query, approach, str(idx+1), size)
                # read log data
                print("*** Read log data ({}) ***".format(l))
                tmpLines = []
                with open(l) as f:
                    while True:
                        line = f.readline()
                        if line == "":
                            break
                        elements = line.split(",")
                        #tmpLines.append(int(elements[0]) - int(elements[2]))
                        tmpLines.append(int(elements[1]))
                #nparray = np.loadtxt("{}".format(l), dtype="int64")
                nparray = np.array(tmpLines)
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
                print("*** Calc median ***")
                median = np.median(validarray)

                meanList.append(mean)
                medianList.append(median)
                stdList.append(std)

                if approach not in allValidList[query]:
                    allValidList[query][approach] = []
                allValidList[query][approach].append(validarray)

            if plotLatency:
                for validList in allValidList[query][approach]:
                    plt.plot(range(validList.size), validList, linewidth=0.1)
                print("*** Save fig ***")
                plt.title("{}-{}-{}".format(query, approach, size))
                plt.ylim(bottom=0)
                plt.ylabel("Latency [ns]")
                plt.savefig("./results/figs/{}-{}-{}.pdf".format(query, approach, size))
                plt.close()

                for validList in allValidList[query][approach]:
                    plt.plot(range(validList.size), validList, linestyle="None", marker=".", markersize=markerSize(validList.size))
                print("*** Save fig ***")
                plt.title("{}-{}-{}".format(query, approach, size))
                plt.ylim(bottom=0)
                plt.ylabel("Latency [ns]")
                plt.savefig("./results/figs/{}-{}-{}.png".format(query, approach, size), dpi=900)
                plt.close()

            # dump log
            print("*** Dump log ***")
            logDump(query, approach, meanList, stdList, startTime, flag, size)

            print("*** Calc result ({},{}) ***".format(query, approach))
            meanNpList = np.array(meanList)
            allMean = meanNpList.mean()
            allStd = meanNpList.std()

            if query not in results:
                results[query] = {}
            results[query][approach] = [allMean, allStd, meanNpList.size, meanNpList, medianList]

        if plotLatencyCmp:
            print("*** Save fig for comparison ***")
            for i in range(len(allValidList[query][approaches[0]])):
                for approach in approaches:
                    plt.plot(range(allValidList[query][approach][i].size), allValidList[query][approach][i], linewidth=0.1)
                plt.title("{}-{}-{}-comparison".format(query, i, size))
                plt.ylim(bottom=0)
                plt.ylabel("Latency")
                plt.legend(approaches)
                plt.savefig("./results/figs/{}-{}-{}-comparison.pdf".format(query, i, size))
                plt.close()

                for approach in approaches:
                    plt.plot(range(allValidList[query][approach][i].size), allValidList[query][approach][i], linestyle="None", marker=".", markersize=markerSize(allValidList[query][approach][i].size))
                plt.title("{}-{}-{}-comparison".format(query, i, size))
                plt.ylim(bottom=0)
                plt.ylabel("Latency")
                plt.legend(approaches)
                plt.savefig("./results/figs/{}-{}-{}-comparison.png".format(query, i, size), dpi=900)
                plt.close()

        print("*** Welch test ***")
        logDumpWelch(query, approaches, startTime, flag, allValidList[query], size)

    if violinPlot == True:
        print("*** Violin plot ***")
        for query in queries:
            for idx in range(len(allValidList[query][approaches[0]])):
                validListsIdx = [allValidList[query][approach][idx] for approach in approaches]
                v_fig = plt.violinplot(validListsIdx, showmeans=True, showmedians=True)
                v_fig['cmedians'].set_color('C1')
                plt.xticks(range(1, len(approaches)+1), [approach for approach in approaches])
                plt.title("*{}* result (Latency, {}, {}, {}, violin)".format(query, flag, idx, size))
                plt.ylabel("Latency [ns]")
                plt.savefig("./results/{}.violin.{}.{}.pdf".format(query, idx, size))
                plt.close()
    else:
        print("*** No violin plot ***")

    return results

def resultFigsGen(results, queries, approaches, flag, size):
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
            else:
                colorList.append("m")

        plt.bar(range(len(resultsList)), resultsList, tick_label=approaches, color=colorList)
        plt.title("*{}* result (Latency, {}, {})".format(query, flag, size))
        plt.ylabel("Latency (mean) [ns]")
        plt.savefig("./results/{}-{}-mean.pdf".format(query, size))
        plt.close()

        for idx in range(results[query][approaches[0]][2]):
            resultsList = [results[query][approach][3][idx] for approach in approaches]
            plt.bar(range(len(resultsList)), resultsList, tick_label=approaches, color=colorList)
            plt.title("*{}* result (Latency, {}, {}, {})".format(query, flag, idx, size))
            plt.ylabel("Latency (mean) [ns]")
            plt.savefig("./results/{}-{}-{}-mean.pdf".format(query, idx, size))
            plt.close()

        for idx in range(results[query][approaches[0]][2]):
            resultsList = [results[query][approach][4][idx] for approach in approaches]
            plt.bar(range(len(resultsList)), resultsList, tick_label=approaches, color=colorList)
            plt.title("*{}* result (Latency, {}, {}, {})".format(query, flag, idx, size))
            plt.ylabel("Latency (median) [ns]")
            plt.savefig("./results/{}-{}-{}-median.pdf".format(query, idx, size))
            plt.close()

def writeResults(results, queries, approaches, startTime, flag, size):
    with open("./results/latency.{}.result.{}.{}.txt".format(flag, startTime, size), "w") as w:
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

        # Write median
        w.write("MEDIAN,{}\n".format(",".join(approaches)))
        for query in queries:
            w.write("{},".format(query))

            for idx in range(results[query][approaches[0]][2]):
                median = []
                w.write("idx={}\n".format(idx))
                for approach in approaches:
                    median.append(str(results[query][approach][4][idx]))
                w.write("{}\n".format(",".join(median)))

        w.write("\n")

        # Write cnt
        w.write("CNT,{}\n".format(",".join(approaches)))
        for query in queries:
            w.write("{},".format(query))

            stds = []
            for approach in approaches:
                stds.append(str(results[query][approach][2]))
            w.write("{}\n".format(",".join(stds)))
