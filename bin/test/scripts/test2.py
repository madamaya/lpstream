import sys
import glob

def arg_parser(elements):
    queries = elements[0].split()
    dataSizes = list(map(int, elements[1].split()))
    outputDir = elements[2]
    return queries, dataSizes, outputDir

def make_chk_ts_map(query, size):
    return_map = {}
    with open("{}_{}.log".format(query, size)) as f:
        for line in f:
            elements = line.replace("\n", "").split(",")
            return_map[int(elements[0])] = int(elements[1])
    return return_map

def get_ws(query):
    if query == "LR":
        return 0
    elif query == "Nexmark":
        return 20
    elif query == "Nexmark2":
        return 1000
    elif query == "NYC":
        return 3000
    elif query == "NYC2":
        return 15000
    elif query == "YSB":
        return 1000
    elif query == "YSB2":
        return 10000
    elif query == "Syn1":
        return 0
    elif query == "Syn2":
        return 1000
    elif query == "Syn3":
        return 10000
    else:
        raise Exception

def make_results_set(path, size, chkts):
    results_set = set()
    files = glob.glob("{}/{}_*.csv".format(path, size))
    for file in files:
        with open(file) as f:
            for line in f:
                elements = line.split(":::::")
                timestamp = int(elements[2].replace("\n", ""))
                if timestamp > chkts:
                    results_set.add(elements[0].replace("\n", ""))
    return results_set

def cmp_base_and_current_result(base_approach_result_set, approach_path, size, chkts, ws):
    result_flag = True
    results_set = set()
    files = glob.glob("{}/{}_*.csv".format(approach_path, size))
    for file in files:
        with open(file) as f:
            for line in f:
                elements = line.replace("\n", "").split(":::::")
                output = elements[0]
                lineage = elements[1]
                ts = int(elements[2])
                reliable_flag = bool(elements[3])

                if ts > chkts + ws:
                    if output not in base_approach_result_set:
                        print("NOT_FOUND_ERROR: " + output)

                    if reliable_flag == True:
                        results_set.add(output)
                    else:
                        print("RELIABLE_FLAG_ERROR: " + output)
                        results_set.add(output)
                elif ts > chkts:
                    if output not in base_approach_result_set:
                        print("NOT_FOUND_ERROR: " + output)
                    results_set.add(output)

    result_flag = len(base_approach_result_set) == len(results_set) and len(base_approach_result_set) == len(base_approach_result_set & results_set)
    return result_flag

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("IllegalArguments: len(sys.argv) = {}".format(len(sys.argv)))
        exit(1)

    queries, dataSizes, outputDir = arg_parser(sys.argv[1:])

    print("queries = {}, type = {}".format(queries, type(queries)))
    print("dataSizes = {}, type = {}".format(dataSizes, type(dataSizes)))
    print("outputDir = {}, type = {}".format(outputDir, type(outputDir)))

    test_result = True
    with open("test2_result.log", "w") as w:
        for size in dataSizes:
            for query in queries:
                if ("Syn" in query and size == -1) or ("Syn" not in queries and size != -1):
                    continue

                query_result = True
                chktimestamps = make_chk_ts_map(query, size)
                ws = get_ws(query)
                for replay_idx in range(1, 5+1):
                    base_approach = "l3stream"
                    base_approach_result_set = make_results_set("{}/{}/{}".format(outputDir, query, base_approach), size, chktimestamps[replay_idx])
                    current_result = cmp_base_and_current_result(base_approach_result_set, "{}/{}/l3streamlin/{}".format(outputDir, query, replay_idx), size, chktimestamps[replay_idx], ws)

                    if current_result == True:
                        print("{},{}: ✅".format(query, replay_idx))
                    else:
                        print("{},{}: ❌".format(query, replay_idx))

                    query_result &= current_result

                if query_result == True:
                    print("{}: ✅".format(query))
                else:
                    print("{}: ❌".format(query))

                test_result &= query_result

        if test_result == True:
            print("test2: ✅")
        else:
            print("test2: ❌")
