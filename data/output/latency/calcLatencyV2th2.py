import os, sys
import heapq
import time
import json
import numpy as np
import matplotlib.pyplot as plt

def getIncreaseFactorTh(value_start, value_end, function=np.median):
    head_val = function(np.array(value_start))
    tail_val = function(np.array(value_end))
    return tail_val/head_val, head_val, tail_val

def idx2name(idx):
    if idx == 0:
        return "S2S"
    elif idx == 1:
        return "K2K"
    elif idx == 2:
        return "DOM"
    elif idx == 3:
        return "TRAVERSE"
    elif idx == 4:
        return "SIZE(Output)"
    elif idx == 5:
        return "SIZE(Lineage)"
    else:
        raise Exception


# Given:
## parallelism (int)
## outputFileDir (str)
## size (str)
## flag (str) {"latency", "throughput"}
if __name__ == "__main__":
    assert len(sys.argv) == 6

    parallelism = int(sys.argv[1])
    outputFileDir = sys.argv[2]
    size = sys.argv[3]
    inputRate = int(sys.argv[4])
    flag = sys.argv[5]
    assert flag == "throughput", print("flag = {}".format(flag))
    query = outputFileDir.split("/")[-2]
    approach = outputFileDir.split("/")[-1]

    if not os.path.exists("{}/../results/fig".format(outputFileDir)):
        os.makedirs("{}/../results/fig".format(outputFileDir))

    start_time = time.time()
    """
    Initialize a priority queue
    """
    all_data_num = 0
    pq = []
    f_list = []
    for idx in range(parallelism):
        dataPath = "{}/{}_{}.csv".format(outputFileDir, size, idx)
        print("count: {}".format(idx))
        all_data_num += sum(1 for _ in open(dataPath))
        f = open(dataPath)
        f_list.append(f)

        line = f.readline()
        if (line != ""):
            data = list(map(int, line.split(",")))
            ts = data[1]
            heapq.heappush(pq, (ts, data))
        else:
            f.close()

    current_window_time = pq[0][0] // 10000

    head_drop_idx = all_data_num // 10
    tail_drop_idx = (all_data_num * 9) // 10
    pivot_idx = (tail_drop_idx - head_drop_idx + 1) // 10
    head_drop_end_idx = head_drop_idx + pivot_idx
    tail_drop_start_idx = tail_drop_idx - pivot_idx

    """
    Calculate latency trends every seconds
    """
    current_idx = -1
    with open("{}/../results/trend-{}-{}.csv".format(outputFileDir, size, approach), "w") as w:
        latency_values_start = []
        latency_values_end = []
        while True:
            if len(pq) == 0:
                break

            # elements format in pq: [ts, data]
            data = heapq.heappop(pq)[1]
            partition = data[0]
            ts = data[1]
            current_idx += 1
            if head_drop_idx <= current_idx < head_drop_end_idx:
                latency_values_start.append([data[2],])
            elif current_idx == head_drop_end_idx:
                start_val_med = np.median(np.array(latency_values_start))
                latency_values_start.clear()
            elif tail_drop_start_idx <= current_idx < tail_drop_idx:
                latency_values_end.append([data[2],])
            elif current_idx == tail_drop_idx:
                end_val_med = np.median(np.array(latency_values_end))
                latency_values_end.clear()
            elif current_idx > tail_drop_idx:
                break

            line = f_list[partition].readline()
            if (line != ""):
                data = list(map(int, line.split(",")))
                ts = data[1]
                heapq.heappush(pq, (ts, data))
            else:
                f_list[partition].close()

    """
    Calculate latency for whole data
    """
    increaseFactorWithMedian = end_val_med / start_val_med

    end_time = time.time()

    print("Time duration: {} [s]".format(end_time-start_time))

    results = {"S2S": {}}

    # increasing factor with median (ifMed)
    results["S2S"]["ifMed"] = increaseFactorWithMedian
    results["S2S"]["startValMed"] = start_val_med
    results["S2S"]["endValMed"] = end_val_med
    results["S2S"]["inputRate"] = inputRate

    with open("{}/../results/result-{}-{}.json".format(outputFileDir, size, approach), "w") as w:
        json.dump(results, w, indent=2)
