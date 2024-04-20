import os, sys
import heapq
import time
import json
import numpy as np
import matplotlib.pyplot as plt

# value_list
# [ [ s2s, k2k, dom, traverse ], [ ... ], ... ]
def getIncreaseFactor(value_list, function=np.median, rate=0.1):
    pivot_idx = int(len(value_list) * 0.1)
    result = []
    for idx in range(len(value_list[0])):
        head_val = function(np.array(value_list)[:,idx][:pivot_idx])
        tail_val = function(np.array(value_list)[:,idx][-pivot_idx:])
        result.append(tail_val/head_val)
    return result


def idx2name(idx):
    if idx == 0:
        return "S2S"
    elif idx == 1:
        return "K2K"
    elif idx == 2:
        return "DOM"
    elif idx == 3:
        return "TRAVERSE"
    else:
        raise Exception


# Given:
## parallelism (int)
## outputFileDir (str)
## key (str)
## flag (str) {"latency", "throughput"}
if __name__ == "__main__":
    assert len(sys.argv) == 5

    parallelism = int(sys.argv[1])
    outputFileDir = sys.argv[2]
    key = sys.argv[3]
    flag = sys.argv[4]
    query = outputFileDir.split("/")[-2]
    approach = outputFileDir.split("/")[-1]

    if not os.path.exists("{}/results".format(outputFileDir)):
        os.makedirs("{}/results".format(outputFileDir))
        os.makedirs("{}/../results/fig".format(outputFileDir))

    start_time = time.time()
    """
    Initialize a priority queue
    """
    pq = []
    f_list = []
    for idx in range(parallelism):
        f = open("{}/{}_{}.csv".format(outputFileDir, key, idx))
        f_list.append(f)

        line = f.readline()
        if (line != ""):
            data = list(map(int, line.split(",")))
            ts = data[1]
            heapq.heappush(pq, (ts, data))
        else:
            f.close()

    current_window_time = pq[0][0] // 1000

    """
    Calculate latency trends every seconds
    """
    with open("{}/results/{}-trend.csv".format(outputFileDir, key), "w") as w:
        latency_values_one_sec = []
        latency_values_all = []
        latency_trends = []
        while True:
            if len(pq) == 0:
                medians = np.median(latency_values_one_sec, axis=0)
                means = np.mean(latency_values_one_sec, axis=0)
                stds = np.std(latency_values_one_sec, axis=0)

                print("{},{},{},{},{}".format(current_window_time, list(medians), list(means), list(stds), len(latency_values_one_sec)))
                w.write("{},{},{},{},{}\n".format(current_window_time, list(medians), list(means), list(stds), len(latency_values_one_sec)))

                latency_values_all += latency_values_one_sec
                latency_trends.append(list(medians) + list(means))
                latency_values_one_sec.clear()
                break

            # elements format in pq: [ts, data]
            data = heapq.heappop(pq)[1]
            partition = data[0]
            ts = data[1]

            if ts // 1000 != current_window_time:
                medians = np.median(latency_values_one_sec, axis=0)
                means = np.mean(latency_values_one_sec, axis=0)
                stds = np.std(latency_values_one_sec, axis=0)

                print("{},{},{},{},{}".format(current_window_time, list(medians), list(means), list(stds), len(latency_values_one_sec)))
                w.write("{},{},{},{},{}\n".format(current_window_time, list(medians), list(means), list(stds), len(latency_values_one_sec)))

                latency_values_all += latency_values_one_sec
                latency_trends.append(list(medians) + list(means))
                latency_values_one_sec.clear()
                current_window_time = ts // 1000

            if flag == "latency":
                latency_values_one_sec.append(data[2:])
            else:
                latency_values_one_sec.append([data[2],])

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
    #with open("{}/results/{}-result.csv".format(outputFileDir, key), "w") as w:
    head_drop_size = int(len(latency_values_all)*0)
    tail_drop_size = int(len(latency_values_all)*1)
    medians = np.median(latency_values_all[head_drop_size:tail_drop_size], axis=0)
    means = np.mean(latency_values_all[head_drop_size:tail_drop_size], axis=0)
    stds = np.std(latency_values_all[head_drop_size:tail_drop_size], axis=0)
    increaseFactorWithMedian = getIncreaseFactor(latency_values_all, np.median)
    increaseFactorWithMean = getIncreaseFactor(latency_values_all, np.mean)

    end_time = time.time()

    print("{},{},{},{}".format(list(medians), list(means), list(stds), len(latency_values_all)))
    #w.write("{},{},{},{}\n".format(list(medians), list(means), list(stds), len(latency_values_all)))
    print("Time duration: {} [s]".format(end_time-start_time))
    #w.write("Time duration: {} [s]\n".format(end_time-start_time))

    if flag == "latency":
        results = {"S2S": {}, "K2K": {}, "DOM": {}, "TRAVERSE": {}}
    else:
        results = {"S2S": {}}
    # median
    for index, median_val in enumerate(medians):
        results[idx2name(index)]["median"] = median_val
    # mean
    for index, mean_val in enumerate(means):
        results[idx2name(index)]["mean"] = mean_val
    # std
    for index, std_val in enumerate(stds):
        results[idx2name(index)]["std"] = std_val

    if flag == "throughput":
        # increasing factor with median (ifMed)
        for index, ifMed in enumerate(increaseFactorWithMedian):
            results[idx2name(index)]["ifMed"] = ifMed
        # increasing factor with mean(ifMean)
        for index, ifMean in enumerate(increaseFactorWithMean):
            results[idx2name(index)]["ifMean"] = ifMean

    with open("{}/results/{}-result.json".format(outputFileDir, key), "w") as w:
        json.dump(results, w, indent=2)

    """
    Translate lists to nparray
    """
    latency_trends = np.array(latency_trends)
    latency_values_all = np.array(latency_values_all)

    """
    Plot results
    """
    if flag == "latency":
        # Plot all data with line graph
        column_num = len(latency_values_all[0])
        for idx in range(column_num):
            plt.title("{} Latency".format(idx2name(idx)))
            plt.plot(latency_values_all[:,idx])

            #plt.ylim(bottom=0)
            unit = "ms" if idx == 2 else "ns"
            plt.ylabel("Latency [{}]".format(unit))

            plt.savefig("{}/../results/fig/line-{}-{}.png".format(outputFileDir, idx2name(idx), approach))
            plt.close()

    # Plot trend data
    column_num = len(latency_trends[0])
    for shift in range(2):
        for idx in range(column_num//2):
            l_type = "median" if shift == 0 else "mean"
            plt.title("{} Latency ({})".format(idx2name(idx), l_type))
            plt.plot(latency_trends[:,idx+shift*column_num//2])

            #plt.ylim(bottom=0)
            unit = "ms" if idx == 2 else "ns"
            plt.ylabel("Latency [{}]".format(unit))

            plt.savefig("{}/../results/fig/trend-{}-{}-{}.png".format(outputFileDir, l_type, idx2name(idx), approach))
            plt.close()