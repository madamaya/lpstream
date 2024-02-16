import os, sys
import time
from distutils.util import strtobool
from utils import utils

filterRate = 0.1
#plotLatency = True
#plotLatencyCmp = True
#violinPlot = True
#queries = ["Syn1", "Syn2", "Syn3", "LR", "NYC", "Nexmark", "YSB"]
#approaches = ["baseline", "genealog", "l3stream", "l3streamlin"]
#dataSize = [-1, 10]
startTime = time.time()
flag = "metrics1"

def main(idx, size, queries, approaches, filterRate, plotLatency, plotLatencyCmp, violinPlot, startTime, flag):
    if size == -1:
        tmp_queries = [query for query in queries if "Syn" not in query]
    else:
        tmp_queries = [query for query in queries if "Syn" in query]

    if idx == 4:
        tmp_approaches = [approach for approach in approaches if approach in ["genealog", "l3streamlin"]]
    else:
        tmp_approaches = approaches

    print("* calcResults() *")
    results = utils.calcResults(tmp_queries, tmp_approaches, filterRate, plotLatency, plotLatencyCmp, violinPlot, startTime, flag, size, idx)

    print("* resultFigsGen *")
    utils.resultFigsGen(results, tmp_queries, tmp_approaches, flag, size, idx)

    print("* writeResults(results) *")
    utils.writeResults(results, tmp_queries, tmp_approaches, startTime, flag, size, idx)

def arg_parser(elements):
    result_type = elements[0]
    plotLatency = bool(strtobool(elements[1]))
    plotLatencyCmp = bool(strtobool(elements[2]))
    violinPlot = bool(strtobool(elements[3]))
    queries = elements[4].split()
    approaches = elements[5].split()
    dataSize = list(map(int, elements[6].split()))

    return result_type, plotLatency, plotLatencyCmp, violinPlot, queries, approaches, dataSize

if __name__ == "__main__":
    if len(sys.argv) != 8:
        print("IllegalArguments: len(sys.argv) = {}".format(len(sys.argv)))
        exit(1)

    # argv[1]: type (latency or throughput), argv[2]: plotLatency, argv[3]: plotLatencyCmp, argv[4]: violinPlot, argv[5]: queries, argv[6]: approaches, argv[7]: dataSize
    result_type, plotLatency, plotLatencyCmp, violinPlot, queries, approaches, dataSize = arg_parser(sys.argv[1:])

    print("result_type = {}, type = {}".format(result_type, type(result_type)))
    print("plotLatency = {}, type = {}".format(plotLatency, type(plotLatency)))
    print("plotLatencyCmp = {}, type = {}".format(plotLatencyCmp, type(plotLatencyCmp)))
    print("violinPlot = {}, type = {}".format(violinPlot, type(violinPlot)))
    print("queries = {}, type = {}".format(queries, type(queries)))
    print("approaches = {}, type = {}".format(approaches, type(approaches)))
    print("dataSize = {}, type = {}".format(dataSize, type(dataSize)))

    if not os.path.exists("./results/figs"):
        os.makedirs("./results/figs")

    if result_type == "latency":
        for idx in [1, 2, 3, 4]:
            for size in dataSize:
                main(idx, size, queries, approaches, filterRate, plotLatency, plotLatencyCmp, violinPlot, startTime, flag)
    elif result_type == "throughput":
        idx = 2 # K2K latency
        for size in dataSize:
            main(idx, size, queries, approaches, filterRate, plotLatency, plotLatencyCmp, violinPlot, startTime, flag)
    else:
        print("IllegalArguments: result_type = {}".format(result_type))
        exit(1)