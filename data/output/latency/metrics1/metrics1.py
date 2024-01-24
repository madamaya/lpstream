import os
import time
from utils import utils

filterRate = 0.1
plotLatency = True
plotLatencyCmp = True
violinPlot = True
#queries = ["LR2", "NYC", "Nexmark2", "YSB"]
queries = ["Syn1", "Syn2", "Syn3"]
approaches = ["baseline", "genealog", "l3stream", "l3streamlin"]
dataSize = [100]
startTime = time.time()
flag = "metrics1"

if __name__ == "__main__":
    if not os.path.exists("./results/figs"):
        os.makedirs("./results/figs")

    for idx in [1, 2]:
        for size in dataSize:
            if idx == 2:
                tmp_approaches = [approach for approach in approaches if approach in ["genealog", "l3streamlin"]]
            else:
                tmp_approaches = approaches

            print("* calcResults() *")
            results = utils.calcResults(queries, tmp_approaches, filterRate, plotLatency, plotLatencyCmp, violinPlot, startTime, flag, size, idx)

            print("* resultFigsGen *")
            utils.resultFigsGen(results, queries, tmp_approaches, flag, size, idx)

            print("* writeResults(results) *")
            utils.writeResults(results, queries, tmp_approaches, startTime, flag, size, idx)

