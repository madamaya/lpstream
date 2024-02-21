import sys
import glob
import numpy as np
import pandas as pd

# TODO
# query_name -> threshold
latency_threshold = {
    "LR": 60000,
    "Nexmark": 60000,
    "Nexmark2": 60000,
    "NYC": 60000,
    "NYC2": 60000,
    "YSB": 60000,
    "YSB2": 60000,
    "Syn1": 60000,
    "Syn2": 60000,
    "Syn3": 60000
}

def arg_parser(elements):
    queries = elements[0].split()
    approaches = elements[1].split()
    dataSizes = list(map(int, elements[2].split()))
    inputRate = int(elements[3])
    dataPath = elements[4]
    return queries, approaches, dataSizes, inputRate, dataPath

def calcLatency(filename, filterRate = 0.1):
    latency_all = []

    # calc K2K-latency
    print("read data")
    with open(filename) as f:
        while True:
            line = f.readline()
            if line == "":
                break
            elements = line.split(",")

            k2k_starttime = int(elements[2])
            k2k_endtime = int(elements[0])
            latency = k2k_endtime - k2k_starttime
            latency_all.append(latency)

    print("calc median")
    datanum = len(latency_all)
    filterednum = int(datanum * filterRate)
    np_latency_all = np.array(latency_all[filterednum:datanum-filterednum])
    latency = np.median(np_latency_all)
    return latency, np_latency_all

def getLatencyValues(queries, approaches, dataSizes, dataPath):
    return_latency_map = {}
    return_latency_all_map = {}
    for dataSize in dataSizes:
        return_latency_map[dataSize] = {}
        return_latency_all_map[dataSize] = {}
        # update queries
        if dataSize == -1: # Realdata
            c_queries = [q for q in queries if "Syn" not in q]
        else: # Syn data
            c_queries = [q for q in queries if "Syn" in q]

        for query in c_queries:
            return_latency_map[dataSize][query] = {}
            return_latency_all_map[dataSize][query] = {}
            for approach in approaches:
                # obtain file name
                print(dataPath + "/latency/metrics1/{}/{}/1_{}.log".format(query, approach, dataSize))
                file_cand = glob.glob(dataPath + "/latency/metrics1/{}/{}/1_{}.log".format(query, approach, dataSize))
                assert len(file_cand) <= 1, print(file_cand)

                if len(file_cand) == 1:
                    filename = file_cand[0]

                    print("getLatencyValues: calcLatency ({}, {}, {})".format(query, approach, dataSize))
                    latency, latency_all = calcLatency(filename)
                    return_latency_map[dataSize][query][approach] = latency
                    return_latency_all_map[dataSize][query][approach] = latency_all
                else:
                    pass

    return return_latency_map, return_latency_all_map

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
        assert len(file_cand) == 1, print(file_cand)
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
        print(dataPath + "/throughput/metrics1/results/throughput.metrics1.result.*.{}.txt".format(dataSize))
        file_cand = glob.glob(dataPath + "/throughput/metrics1/results/throughput.metrics1.result.*.{}.txt".format(dataSize))
        assert len(file_cand) == 1, print(file_cand)
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

def isNotIncreasing(latency_sequence):
    # TODO
    return True

def containValue(dct, query, approach, dataSize):
    if dataSize in dct and query in dct[dataSize] and approach in dct[dataSize][query]:
        return True
    else:
        return False

def isStable24(query, approach, dataSize, throughput_values, cpu_values, mem_values, inputRate) -> bool:
    # stable:
    # c1. latency is not continuously increasing
    # c2. average cpu usage < 80 %
    # c3. median latency < XX seconds
    # c4. evaluated throughput is comparable with inputRate

    if (containValue(cpu_values, query, approach, dataSize) and
        containValue(throughput_values, query, approach, dataSize)):
        # c2
        c2_result = cpu_values[dataSize][query][approach] < 80

        # c4
        c4_result = throughput_values[dataSize][query][approach] > inputRate * 0.8
        return c2_result and c4_result
    else:
        return False

def isStable(query, approach, dataSize, latency_values, latency_values_all, throughput_values, cpu_values, mem_values, inputRate) -> bool:
    # stable:
    # c1. latency is not continuously increasing
    # c2. average cpu usage < 80 %
    # c3. median latency < XX seconds
    # c4. evaluated throughput is comparable with inputRate

    if (containValue(latency_values_all, query, approach, dataSize) and
        containValue(cpu_values, query, approach, dataSize) and
        containValue(latency_values, query, approach, dataSize) and
        containValue(throughput_values, query, approach, dataSize)):
        # c1
        c1_result = isNotIncreasing(latency_values_all[dataSize][query][approach])

        # c2
        c2_result = cpu_values[dataSize][query][approach] < 80

        # c3
        c3_result = latency_values[dataSize][query][approach] < latency_threshold[query] # [ms]

        # c4
        c4_result = throughput_values[dataSize][query][approach] > inputRate * 0.8

        return c1_result and c2_result and c3_result and c4_result
    else:
        return False

if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("IllegalArguments: len(sys.argv) = {}".format(len(sys.argv)))
        exit(1)

    # argv[1]: queries, argv[2]: approaches, argv[3]: dataSize, argv[4]: inputRate, argv[5]: dataPath
    queries, approaches, dataSizes, inputRate, dataPath = arg_parser(sys.argv[1:])
    print("queries = {}, type = {}".format(queries, type(queries)))
    print("approaches = {}, type = {}".format(approaches, type(approaches)))
    print("dataSizes = {}, type = {}".format(dataSizes, type(dataSizes)))
    print("inputRate = {}, type = {}".format(inputRate, type(inputRate)))
    print("dataPath = {}, type = {}".format(dataPath, type(dataPath)))

    # Return a dictionary object (dict), consisting of each latency values
    # dict[dataSize][query][approach] = xxx
    # latency_values has latency valus at each case.
    # latency_values_all has all latency data points at each case.
    # latency_values, latency_values_all = getLatencyValues(queries, approaches, dataSizes, dataPath)

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

                #result = isStable(query, approach, dataSize, latency_values, latency_values_all, throughput_values, cpu_values, mem_values, inputRate)
                result = isStable24(query, approach, dataSize, throughput_values, cpu_values, mem_values, inputRate)
                print(dataSize, query, approach, result)

                # the result means that Flink was unstable
                with open("./finishedComb.csv", "a") as w:
                    if result == False:
                        w.write("unstable,{},{},{},{}\n".format(query, approach, dataSize, inputRate))
                    else:
                        w.write("stable,{},{},{},{}\n".format(query, approach, dataSize, inputRate))
