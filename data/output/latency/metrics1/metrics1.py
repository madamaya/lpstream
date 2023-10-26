import os
import time
from utils import utils

filterRate = 0.1
plotLatency = True
plotLatencyCmp = True
queries = ["LR", "Nexmark", "NYC", "YSB"]
approaches = ["baseline", "genealog", "l3stream"]
startTime = time.time()
flag = "metrics1"

if __name__ == "__main__":
    if not os.path.exists("./figs"):
        os.makedirs("./figs")

    print("* calcResults() *")
    results = utils.calcResults(queries, approaches, filterRate, plotLatency, plotLatencyCmp, startTime, flag)

    print("* resultFigsGen *")
    utils.resultFigsGen(results, queries, approaches, flag)

    print("* writeResults(results) *")
    utils.writeResults(results, queries, approaches, startTime, flag)

