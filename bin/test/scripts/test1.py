import sys
import glob

def arg_parser(elements):
    queries = elements[0].split()
    approaches = elements[1].split()
    dataSizes = list(map(int, elements[2].split()))
    outputDir = elements[3]
    return queries, approaches, dataSizes, outputDir

def make_results_set(approach_path, size, flag):
    results_set = set()
    files = glob.glob("{}/{}_*.csv".format(approach_path, size))
    for file in files:
        with open(file) as f:
            for line in f:
                if flag == True:
                    results_set.add(line.split(":::::")[0].replace("\n", ""))
                else:
                    elements = line.split(":::::")
                    elements[-1].replace("\n", "")
                    results_set.add(":::::".join(elements[:2]))
    return results_set

def cmp_base_and_current_result(base_approach_result_set, approach_path, size, flag):
    result_flag = True
    files = glob.glob("{}/{}_*.csv".format(approach_path, size))
    for file in files:
        with open(file) as f:
            for line in f:
                if flag == True:
                    element = line.split(":::::")[0].replace("\n", "")
                else:
                    elements = line.split(":::::")
                    elements[-1].replace("\n", "")
                    element = ":::::".join(elements[:2])

                if element not in base_approach_result_set:
                    result_flag = False
                    print("Not Found: " + element)
    return result_flag

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("IllegalArguments: len(sys.argv) = {}".format(len(sys.argv)))
        exit(1)

    queries, approaches, dataSizes, outputDir = arg_parser(sys.argv[1:])

    print("queries = {}, type = {}".format(queries, type(queries)))
    print("approaches = {}, type = {}".format(approaches, type(approaches)))
    print("dataSizes = {}, type = {}".format(dataSizes, type(dataSizes)))
    print("outputDir = {}, type = {}".format(outputDir, type(outputDir)))

    with open("test1_result.log", "w") as w:
        # Test1-1 (Compare outputs of Baseline, GeneaLog, Ordinary, and Provenance)
        test_result = True
        for size in dataSizes:
            for query in queries:
                if ("Syn" in query and size == -1) or ("Syn" not in queries and size != -1):
                    continue

                base_approach = approaches[0]
                base_approach_result_set = make_results_set("{}/{}/{}".format(outputDir, query, base_approach), size, True)
                query_result = True
                for current_approach in approaches[1:]:
                    current_result = cmp_base_and_current_result(base_approach_result_set, "{}/{}/{}".format(outputDir, query, current_approach), size, True)
                    query_result &= current_result
                    if current_result == True:
                        print("1-1,{},{}vs.{},{}: ✅".format(query, base_approach, current_approach, size))
                        w.write("1-1,{},{}vs.{},{}: ✅\n".format(query, base_approach, current_approach, size))
                    else:
                        print("1-1,{},{}vs.{},{}: ❌".format(query, base_approach, current_approach, size))
                        w.write("1-1,{},{}vs.{},{}: ❌\n".format(query, base_approach, current_approach, size))
                if query_result == True:
                    print("1-1,{},{}: ✅".format(query, size))
                    w.write("1-1,{},{}: ✅\n".format(query, size))
                else:
                    print("1-1,{},{}: ❌".format(query, size))
                    w.write("1-1,{},{}: ❌\n".format(query, size))
                test_result &= query_result
        if test_result == True:
            print("1-1: ✅")
            w.write("1-1: ✅\n")
        else:
            print("1-1: ❌")
            w.write("1-1: ❌\n")
        # Test1-2 (Compare output and provenance of GeneaLog and Provenance)
        test_result = True
        for size in dataSizes:
            for query in queries:
                if ("Syn" in query and size == -1) or ("Syn" not in queries and size != -1):
                    continue

                base_approach = approaches[1]
                current_approach = approaches[3]
                base_approach_result_set = make_results_set("{}/{}/{}".format(outputDir, query, base_approach), size, False)
                current_result = cmp_base_and_current_result(base_approach_result_set, "{}/{}/{}".format(outputDir, query, current_approach), size, False)

                if current_result == True:
                    print("1-2,{},{}: ✅".format(query, size))
                    w.write("1-2,{},{}: ✅\n".format(query, size))
                else:
                    print("1-2,{},{}: ❌".format(query, size))
                    w.write("1-2,{},{}: ❌\n".format(query, size))
                test_result &= current_result
        if test_result == True:
            print("1-2: ✅")
            w.write("1-2: ✅\n")
        else:
            print("1-2: ❌")
            w.write("1-2: ❌\n")