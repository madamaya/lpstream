import os
import time
from utils import utils

filterRate = 0.1
plotTrends = True
#queries = ["LR2", "NYC", "YSB", "Nexmark2"]
queries = ["Syn1", "Syn5", "Syn3", "LR2", "NYC", "Nexmark3", "YSB"]
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
