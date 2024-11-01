import os, sys
import glob
import json
import numpy as np

def arg_parser(elements):
    queries = elements[0].split()
    approaches = elements[1].split()
    dataSizes = list(map(int, elements[2].split()))
    dataPath = elements[3]
    return queries, approaches, dataSizes, dataPath

def getLatencyResults(queries, approaches, dataSizes, dataPath):
    results = {}
    for query in queries:
        for approach in approaches:
            for size in dataSizes:
                # Initialize results dir
                if size not in results:
                    results[size] = {}
                if query not in results[size]:
                    results[size][query] = {}
                if approach not in results[size][query]:
                    results[size][query][approach] = {}

                # File exists or not
                if not os.path.exists("{}/latency/{}/results/result-{}-{}.json".format(dataPath, query, size, approach)):
                    # If the file does not exist, 'nan' value is assigned. ( results[size][query][approach][{'median'|'mean'|'ifMed'|'ifMean'}] )
                    results[size][query][approach]["ifMed"] = np.nan
                    results[size][query][approach]["inputRate"] = np.nan
                else:
                    # If exists, open the file as a json.
                    f = open("{}/latency/{}/results/result-{}-{}.json".format(dataPath, query, size, approach))
                    jdata = json.load(f)
                    f.close()

                    results[size][query][approach]["ifMed"] = jdata["S2S"]["ifMed"]
                    results[size][query][approach]["inputRate"] = int(jdata["S2S"]["inputRate"])
    return results

def getCpuMemValues(queries, approaches, dataSizes, dataPath):
    return_cpu_map = {}
    return_mem_map = {}
    for dataSize in dataSizes:
        # add a new dataSize to return_map
        return_cpu_map[dataSize] = {}
        return_mem_map[dataSize] = {}

        # obtain file name
        print(dataPath + "/cpu-memory/results/cpumem.result.*.{}.txt".format(dataSize))
        file_cand = glob.glob(dataPath + "/cpu-memory/results/cpumem.result.*.{}.txt".format(dataSize))
        if len(file_cand) != 1:
            raise Exception
        filename = file_cand[0]

        # open cpu/mem result file and update return_cpu(mem)_map
        with open(filename) as f:
            header = f.readline().rstrip()
            approach2idx = {key:idx for idx,key in enumerate(header.split(","))}

            while True:
                line = f.readline()
                if line.replace("\n", "") == "":
                    break

                query = line.split(",")[0]
                if query not in queries:
                    continue

                return_cpu_map[dataSize][query] = {}
                for approach in approaches:
                    return_cpu_map[dataSize][query][approach] = float(line.split(",")[approach2idx[approach]])

            header = f.readline().rstrip()
            approach2idx = {key:idx for idx,key in enumerate(header.split(","))}
            while True:
                line = f.readline()
                if line == "":
                    break

                query = line.split(",")[0]
                if query not in queries:
                    continue

                return_mem_map[dataSize][query] = {}
                for approach in approaches:
                    return_mem_map[dataSize][query][approach] = float(line.split(",")[approach2idx[approach]])

    return return_cpu_map, return_mem_map

def getThroughputValues(queries, approaches, dataSizes, dataPath):
    return_map = {}
    for dataSize in dataSizes:
        # add a new dataSize to return_map
        return_map[dataSize] = {}

        # obtain file name
        print(dataPath + "/throughput/results/throughput.result.*.{}.txt".format(dataSize))
        file_cand = glob.glob(dataPath + "/throughput/results/throughput.result.*.{}.txt".format(dataSize))
        if len(file_cand) != 1:
            raise Exception
        filename = file_cand[0]

        # open throughput result file and update return_map
        with open(filename) as f:
            header = f.readline().rstrip()
            approach2idx = {key:idx for idx,key in enumerate(header.split(","))}

            while True:
                line = f.readline()
                if line.replace("\n", "") == "":
                    break

                query = line.split(",")[0]
                if query not in queries:
                    continue
                return_map[dataSize][query] = {}

                for approach in approaches:
                    return_map[dataSize][query][approach] = float(line.split(",")[approach2idx[approach]])
    return return_map

def isNotIncreasing(ifMed):
    return ifMed < 3

def containValue(dct, query, approach, dataSize):
    if dataSize in dct and query in dct[dataSize] and approach in dct[dataSize][query]:
        return True
    else:
        return False

def isStable(query, approach, dataSize, latency_values, throughput_values, cpu_values, mem_values, inputRate) -> bool:
    # stable:
    # c1. latency is not continuously increasing
    # c2. average cpu usage < 80 %
    # c4. evaluated throughput is comparable with inputRate

    if (containValue(cpu_values, query, approach, dataSize) and
        containValue(latency_values, query, approach, dataSize) and
        containValue(throughput_values, query, approach, dataSize)):
        line = ""
        # c1
        c1_result = isNotIncreasing(latency_values[dataSize][query][approach]["ifMed"])

        # c2
        c2_result = cpu_values[dataSize][query][approach] < 80

        # c4
        if query == "Nexmark" or query == "Nexmark2" or query == "Syn2":
            c4_result = throughput_values[dataSize][query][approach]/2 > inputRate * 0.9
        else:
            c4_result = throughput_values[dataSize][query][approach] > inputRate * 0.9

        if (np.isnan(cpu_values[dataSize][query][approach]) or
                np.isnan(latency_values[dataSize][query][approach]["ifMed"]) or
                np.isnan(throughput_values[dataSize][query][approach])):
            line += "nan"
        else:
            if c1_result == False:
                line += "c1"
            if c2_result == False:
                line += "c2"
            if c4_result == False:
                line += "c4"
        return line
    else:
        return "c0"

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("IllegalArguments: len(sys.argv) = {}".format(len(sys.argv)))
        exit(1)

    # argv[1]: queries, argv[2]: approaches, argv[3]: dataSize, argv[4]: dataPath
    queries, approaches, dataSizes, dataPath = arg_parser(sys.argv[1:])
    print("queries = {}, type = {}".format(queries, type(queries)))
    print("approaches = {}, type = {}".format(approaches, type(approaches)))
    print("dataSizes = {}, type = {}".format(dataSizes, type(dataSizes)))
    print("dataPath = {}, type = {}".format(dataPath, type(dataPath)))

    # Return a dictionary object (dict), consisting of each latency values
    # dict[size][query][approach][{'ifMed'|'inputRate'}] = xxx
    latency_values = getLatencyResults(queries, approaches, dataSizes, dataPath)

    # Return a dictionary object (dict), consisting of each throughput values
    # dict[dataSize][query][approach] = xxx
    throughput_values = getThroughputValues(queries, approaches, dataSizes, dataPath)

    # Return a dictionary object (dict), consisting of each cpu&memory usage values
    # dict[dataSize][query][approach] = xxx
    cpu_values, mem_values = getCpuMemValues(queries, approaches, dataSizes, dataPath)

    for query in queries:
        for approach in approaches:
            for dataSize in dataSizes:
                if dataSize == -1 and "Syn" in query:
                    continue
                if dataSize != -1 and "Syn" not in query:
                    continue

                inputRate = latency_values[dataSize][query][approach]["inputRate"]
                if np.isnan(inputRate):
                    continue

                result = isStable(query, approach, dataSize, latency_values, throughput_values, cpu_values, mem_values, inputRate)
                print(dataSize, query, approach, inputRate, result)

                # the result means that Flink was unstable
                with open("./finishedComb.csv", "a") as w:
                    if len(result) > 0:
                        w.write("unstable,{},{},{},{},{}\n".format(query, approach, dataSize, inputRate, result))
                    else:
                        w.write("stable,{},{},{},{}\n".format(query, approach, dataSize, inputRate))
