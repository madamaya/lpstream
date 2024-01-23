import os
import time
from utils import utils

#queries = ["LR2", "NYC", "Nexmark2", "YSB"]
queries = ["Syn1", "Syn2", "Syn3"]
approaches = ["baseline", "genealog", "l3stream", "l3streamlin"]
dataSize = [100]
startTime = time.time()
flag = "metrics1"

if __name__ == "__main__":
    if not os.path.exists("./results"):
        os.makedirs("./results")

    for size in dataSize:
        print("* calcResults(queries, approaches, size) *")
        results = utils.calcResults(queries, approaches, startTime, size)

        print("* resultFigsGen(results, queries, approaches, flag, size) *")
        utils.resultFigsGen(results, queries, approaches, flag, size)

        print("* writeResults(results, queries, approaches, startTime, flag, size) *")
        utils.writeResults(results, queries, approaches, startTime, flag, size)
