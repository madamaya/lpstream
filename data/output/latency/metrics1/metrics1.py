import os
import time
from utils import utils

filterRate = 0.1
plotLatency = True
plotLatencyCmp = True
violinPlot = True
#queries = ["LR2", "NYC", "Nexmark2", "YSB"]
queries = ["Syn1", "Syn5", "Syn3", "LR2", "NYC", "Nexmark3", "YSB"]
approaches = ["baseline", "genealog", "l3stream", "l3streamlin"]
dataSize = [-1, 10]
startTime = time.time()
flag = "metrics1"

def main(idx, size, queries, approaches, filterRate, plotLatency, plotLatencyCmp, violinPlot, startTime, flag):
    if size == -1:
        tmp_queries = [query for query in queries if "Syn" not in query]
    else:
        tmp_queries = [query for query in queries if "Syn" in query]

    if idx == 2:
        tmp_approaches = [approach for approach in approaches if approach in ["genealog", "l3streamlin"]]
    else:
        tmp_approaches = approaches

    print("* calcResults() *")
    results = utils.calcResults(tmp_queries, tmp_approaches, filterRate, plotLatency, plotLatencyCmp, violinPlot, startTime, flag, size, idx)

    print("* resultFigsGen *")
    utils.resultFigsGen(results, tmp_queries, tmp_approaches, flag, size, idx)

    print("* writeResults(results) *")
    utils.writeResults(results, tmp_queries, tmp_approaches, startTime, flag, size, idx)

if __name__ == "__main__":
    if not os.path.exists("./results/figs"):
        os.makedirs("./results/figs")

    for idx in [1, 2]:
        for size in dataSize:
            main(idx, size, queries, approaches, filterRate, plotLatency, plotLatencyCmp, violinPlot, startTime, flag)