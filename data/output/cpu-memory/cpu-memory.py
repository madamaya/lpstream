import os
import time
from utils import utils

filterRate = 0.1
plotTrends = True
queries = ["LR2", "NYC", "YSB", "Nexmark2"]
approaches = ["baseline", "genealog", "l3stream", "l3streamlin"]
startTime = time.time()
flag = "metrics1"

if __name__ == "__main__":
    if not os.path.exists("./results/figs"):
        os.makedirs("./results/figs")

    print("* calcResults() *")
    a,b,c,d = utils.calcResults(queries, approaches, filterRate, plotTrends, startTime)
