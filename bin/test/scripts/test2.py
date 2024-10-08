import sys
import glob

def arg_parser(elements):
    queries = elements[0].split()
    dataSizes = list(map(int, elements[1].split()))
    outputDir = elements[2]
    return queries, dataSizes, outputDir

def remove_4th_attribute(line, syn10flag):
    if not syn10flag:
        return line
    else:
        tmp_line_elements = line.split(",")
        return ",".join(tmp_line_elements[:3] + tmp_line_elements[4:])

def make_chk_ts_map(query, size):
    return_map = {}
    with open("../redis_log/{}_{}.log".format(query, size)) as f:
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


def make_results_set(path, size, flag):
    results_set = set()
    files = glob.glob("{}/{}_*.csv".format(path, size))
    for file in files:
        with open(file) as f:
            for line in f:
                elements = line.replace("\n", "").split(":::::")
                if flag == True:
                    element = remove_4th_attribute(elements[0], "syn10" in path)
                else:
                    element = remove_4th_attribute(":::::".join(elements[:2]), "syn10" in path)
                results_set.add(element)
    return results_set

def make_results_set_ts(path, size, chkts, flag):
    results_set = set()
    files = glob.glob("{}/{}_*.csv".format(path, size))
    for file in files:
        with open(file) as f:
            for line in f:
                elements = line.replace("\n", "").split(":::::")
                if flag == True:
                    element = remove_4th_attribute(elements[0], "syn10" in path)
                else:
                    element = remove_4th_attribute(":::::".join(elements[:2]), "syn10" in path)
                timestamp = int(elements[2])
                if timestamp > chkts:
                    results_set.add(element)
    return results_set

def cmp_base_and_current_result(base_approach_result_set, gen_approach_result_set_out, gen_approach_result_set_out_lin, approach_path, size, chkts, ws):
    result_flag = True
    false_exist_flag = False
    results_set = set()
    files = glob.glob("{}/{}_*.csv".format(approach_path, size))
    for file in files:
        with open(file) as f:
            for line in f:
                elements = line.replace("\n", "").split(":::::")
                output = remove_4th_attribute(elements[0], "syn10" in approach_path)
                lineage = elements[1]
                ts = int(elements[2])
                reliable_flag = elements[3]

                if ts > chkts + ws:
                    if output not in base_approach_result_set:
                        result_flag = False
                        print("NOT_FOUND_ERROR: " + output)

                    if reliable_flag == "true":
                        if output + ":::::" + lineage not in gen_approach_result_set_out_lin:
                            result_flag = False
                            print("LINEAGE_NOT_CORRECT_ERROR: " + output)
                        results_set.add(output)
                    else:
                        result_flag = False
                        print("RELIABLE_FLAG_ERROR: " + output)
                        results_set.add(output)
                elif ts > chkts:
                    if reliable_flag == "false":
                        false_exist_flag = True
                    if output not in base_approach_result_set:
                        result_flag = False
                        print("NOT_FOUND_ERROR: " + output)
                    results_set.add(output)
                else:
                    if reliable_flag == "false":
                        false_exist_flag = True
                    if output not in gen_approach_result_set_out:
                        print("NOT_FOUNT_ERROR_G: " + output)

    result_flag &= len(base_approach_result_set) == len(results_set) and len(base_approach_result_set) == len(base_approach_result_set & results_set)
    return result_flag, false_exist_flag

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
                if ("Syn" in query and size == -1) or ("Syn" not in query and size != -1):
                    continue

                query_result = True
                chktimestamps = make_chk_ts_map(query, size)
                ws = get_ws(query)

                base_approach_result_set = make_results_set("{}/{}/{}".format(outputDir, query.lower(), "l3stream"), size, True)
                gen_approach_result_set_out = make_results_set("{}/{}/{}".format(outputDir, query.lower(), "genealog"), size, True)
                if len(base_approach_result_set) == len(gen_approach_result_set_out) and len(base_approach_result_set) == len(base_approach_result_set & gen_approach_result_set_out):
                    print("L3Stream == Gen ✅")
                else:
                    print("L3Stream == Gen ❌")
                    query_result = False

                gen_approach_result_set_out_lin = make_results_set("{}/{}/{}".format(outputDir, query.lower(), "genealog"), size, False)
                for replay_idx in range(2, 5+1):
                    base_approach_result_set = make_results_set_ts("{}/{}/{}".format(outputDir, query.lower(), "l3stream"), size, chktimestamps[replay_idx], True)
                    current_result, false_exist_flag = cmp_base_and_current_result(base_approach_result_set, gen_approach_result_set_out, gen_approach_result_set_out_lin, "{}/{}/l3streamlin/{}".format(outputDir, query.lower(), replay_idx), size, chktimestamps[replay_idx], ws)
                    if current_result == True:
                        print("{},{},{}: ✅".format(query, replay_idx, false_exist_flag))
                    else:
                        print("{},{},{}: ❌".format(query, replay_idx, false_exist_flag))

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
