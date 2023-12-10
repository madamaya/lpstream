import os
import time
from utils import utils

filterRate = 0.1
plotLatency = True
plotLatencyCmp = True
violinPlot = True
queries = ["LR2", "Nexmark", "NYC", "YSB", "Nexmark2", "NYC2", "YSB2"]
approaches = ["baseline", "genealog", "l3stream", "l3streamlin"]
startTime = time.time()
flag = "metrics1"

if __name__ == "__main__":
    if not os.path.exists("./results/figs"):
        os.makedirs("./results/figs")

    for i in range(2):
        print("* calcResults() *")
        results = utils.calcResults(queries, approaches, filterRate, plotLatency, plotLatencyCmp, violinPlot, startTime, flag, i)

        print("* resultFigsGen *")
        utils.resultFigsGen(results, queries, approaches, flag, i)

        print("* writeResults(results) *")
        utils.writeResults(results, queries, approaches, startTime, flag, i)

