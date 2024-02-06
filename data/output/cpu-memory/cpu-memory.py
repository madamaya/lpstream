import os
import time
from utils import utils

filterRate = 0.1
plotTrends = True
queries = ["Syn1", "Syn2", "Syn3", "LR", "NYC", "Nexmark", "YSB"]
approaches = ["baseline", "genealog", "l3stream", "l3streamlin"]
dataSize = [-1, 10]
startTime = time.time()
flag = "metrics1"

if __name__ == "__main__":
    if not os.path.exists("./results/figs"):
        os.makedirs("./results/figs")

    for size in dataSize:
        if size == -1:
            tmp_queries = [query for query in queries if "Syn" not in query]
        else:
            tmp_queries = [query for query in queries if "Syn" in query]

        print("* calcResults() *")
        a,b,c,d = utils.calcResults(tmp_queries, approaches, filterRate, plotTrends, startTime, size)
