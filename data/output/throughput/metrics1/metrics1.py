import os, sys
import time
from utils import utils

#queries = ["Syn1", "Syn2", "Syn3", "LR", "NYC", "Nexmark", "YSB"]
#approaches = ["baseline", "genealog", "l3stream", "l3streamlin"]
#dataSize = [-1, 10]
startTime = time.time()
flag = "metrics1"

def arg_parser(elements):
    queries = elements[0].split()
    approaches = elements[1].split()
    dataSize = list(map(int, elements[2].split()))

    return queries, approaches, dataSize

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("IllegalArguments: len(sys.argv) = {}".format(len(sys.argv)))
        exit(1)

    # argv[1]: queries, argv[2]: approaches, argv[3]: dataSize
    queries, approaches, dataSize = arg_parser(sys.argv[1:])

    print("queries = {}, type = {}".format(queries, type(queries)))
    print("approaches = {}, type = {}".format(approaches, type(approaches)))
    print("dataSize = {}, type = {}".format(dataSize, type(dataSize)))

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
