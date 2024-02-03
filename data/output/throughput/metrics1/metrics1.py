import os
import time
from utils import utils

#queries = ["LR2", "NYC", "Nexmark2", "YSB"]
queries = ["Syn1", "Syn5", "Syn3", "LR2", "NYC", "Nexmark3", "YSB"]
approaches = ["baseline", "genealog", "l3stream", "l3streamlin"]
dataSize = [-1, 10]
startTime = time.time()
flag = "metrics1"

if __name__ == "__main__":
    if not os.path.exists("./results"):
        os.makedirs("./results")

    for size in dataSize:
        if size == -1:
            tmp_queries = [query for query in queries if "Syn" not in query]
        else:
            tmp_queries = [query for query in queries if "Syn" in query]

        print("* calcResults(queries, approaches, size) *")
        results = utils.calcResults(tmp_queries, approaches, startTime, size)

        print("* resultFigsGen(results, queries, approaches, flag, size) *")
        utils.resultFigsGen(results, tmp_queries, approaches, flag, size)

        print("* writeResults(results, queries, approaches, startTime, flag, size) *")
        utils.writeResults(results, tmp_queries, approaches, startTime, flag, size)
