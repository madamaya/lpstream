import sys
import glob

def arg_parser(elements):
    queries = elements[0].split()
    approaches = elements[1].split()
    dataSizes = list(map(int, elements[2].split()))
    outputDir = elements[3]
    return queries, approaches, dataSizes, outputDir

def remove_4th_attribute(line, syn10flag):
    if not syn10flag:
        return line
    else:
        tmp_line_elements = line.split(",")
        return ",".join(tmp_line_elements[:3] + tmp_line_elements[4:])

def make_results_set_list(approach_path, size):
    results_set = set()
    results_list = []
    files = glob.glob("{}/{}_*.csv".format(approach_path, size))
    for file in files:
        with open(file) as f:
            for line in f:
                results_set.add(remove_4th_attribute(line.replace("\n", "").split(":::::")[0], "Syn10" in approach_path))
                results_list.append(remove_4th_attribute(line.replace("\n", "").split(":::::")[0], "Syn10" in approach_path))
    return results_set, results_list

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("IllegalArguments: len(sys.argv) = {}".format(len(sys.argv)))
        exit(1)

    queries, approaches, dataSizes, outputDir = arg_parser(sys.argv[1:])

    print("queries = {}, type = {}".format(queries, type(queries)))
    print("approaches = {}, type = {}".format(approaches, type(approaches)))
    print("dataSizes = {}, type = {}".format(dataSizes, type(dataSizes)))
    print("outputDir = {}, type = {}".format(outputDir, type(outputDir)))

    with open("test1_set_result.log", "w") as w:
        # Test1-1 (Compare outputs of Baseline, GeneaLog, Ordinary, and Provenance)
        test_result = True
        for size in dataSizes:
            for query in queries:
                query_result = True

                if ("Syn" in query and size == -1) or ("Syn" not in query and size != -1):
                    continue

                for approach in approaches:
                    results_set, results_list = make_results_set_list("{}/{}/{}".format(outputDir, query, approach), size)
                    approach_result = (len(results_set) == len(results_list))

                    if approach_result == True:
                        print("{},{},{}: ✅({} == {})".format(query, approach, size, len(results_set), len(results_list)))
                    else:
                        print("{},{},{}: ❌({} != {})".format(query, approach, size, len(results_set), len(results_list)))

                    query_result &= approach_result

                if query_result == True:
                    print("{},{}: ✅".format(query, size))
                else:
                    print("{},{}: ❌".format(query, size))

                test_result &= query_result

    if test_result == True:
        print("TEST: ✅")
    else:
        print("TEST: ❌")