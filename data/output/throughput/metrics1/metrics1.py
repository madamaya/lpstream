import os
import time
from utils import utils

queries = ["LR", "Nexmark", "NYC", "YSB"]
approaches = ["baseline", "genealog", "l3stream", "l3streamlin"]
startTime = time.time()
flag = "metrics1"

if __name__ == "__main__":
    if not os.path.exists("./results"):
        os.makedirs("./results")

    print("* calcResults(queries, approaches) *")
    results = utils.calcResults(queries, approaches, startTime)

    print("* resultFigsGen(results, queries, approaches, flag) *")
    utils.resultFigsGen(results, queries, approaches, flag)

    print("* writeResults(results, queries, approaches, startTime,, flag) *")
    utils.writeResults(results, queries, approaches, startTime, flag)
