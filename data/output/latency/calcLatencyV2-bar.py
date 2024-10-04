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
    assert len(sys.argv) == 5

    parallelism = int(sys.argv[1])
    outputFileDir = sys.argv[2]
    size = sys.argv[3]
    flag = sys.argv[4]
    query = outputFileDir.split("/")[-2]
    approach = outputFileDir.split("/")[-1]

    if not os.path.exists("{}/../results-bar/fig".format(outputFileDir)):
        os.makedirs("{}/../results-bar/fig".format(outputFileDir))

    start_time = time.time()
    """
    Initialize a priority queue
    """
    pq = []
    f_list = []
    for idx in range(parallelism):
        f = open("{}/{}_{}.csv".format(outputFileDir, size, idx))
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
    with open("{}/../results-bar/trend-{}-{}.csv".format(outputFileDir, size, approach), "w") as w:
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
                if len(data) == 6:
                    data += [0, 0]
                    data[6] = data[5]
                    data[5] = 0
                assert len(data) == 8, "len(data) != 8"
                latency_values_one_sec.append(data[2:])
            else:
                raise Exception

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
    head_drop_size = len(latency_values_all) * 25 // 100
    tail_drop_size = len(latency_values_all) * 90 // 100
    medians = np.median(latency_values_all[head_drop_size:tail_drop_size], axis=0)
    means = np.mean(latency_values_all[head_drop_size:tail_drop_size], axis=0)
    stds = np.std(latency_values_all[head_drop_size:tail_drop_size], axis=0)
    increaseFactorWithMedian = getIncreaseFactor(latency_values_all[head_drop_size:tail_drop_size], np.median)
    increaseFactorWithMean = getIncreaseFactor(latency_values_all[head_drop_size:tail_drop_size], np.mean)

    """
    Write valid data
    """
    with open("{}/../results-bar/valid_data-{}-{}.log".format(outputFileDir, size, approach), "w") as w:
        for line in latency_values_all[head_drop_size:tail_drop_size]:
            w.write(",".join(map(str, line)) + "\n")

    end_time = time.time()

    print("{},{},{},{}".format(list(medians), list(means), list(stds), len(latency_values_all)))
    #w.write("{},{},{},{}\n".format(list(medians), list(means), list(stds), len(latency_values_all)))
    print("Time duration: {} [s]".format(end_time-start_time))
    #w.write("Time duration: {} [s]\n".format(end_time-start_time))

    if flag == "latency":
        results = {"S2S": {}, "K2K": {}, "DOM": {}, "TRAVERSE": {}, "SIZE(Output)": {}, "SIZE(Lineage)": {}}
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

    with open("{}/../results-bar/result-{}-{}.json".format(outputFileDir, size, approach), "w") as w:
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

            plt.ylim(bottom=0)
            if idx == 2:
                unit = "ms"
            elif idx == 4:
                unit = "chars"
            elif idx == 5:
                unit = "tuples"
            else:
                unit = "ns"
            plt.ylabel("Latency [{}]".format(unit))

            plt.savefig("{}/../results-bar/fig/line-{}-{}.png".format(outputFileDir, idx2name(idx), approach))
            plt.close()

    # Plot trend data
    column_num = len(latency_trends[0])
    for shift in range(2):
        for idx in range(column_num//2):
            l_type = "median" if shift == 0 else "mean"
            plt.title("{} Latency ({})".format(idx2name(idx), l_type))
            plt.plot(latency_trends[:,idx+shift*column_num//2])

            plt.ylim(bottom=0)
            if idx == 2:
                unit = "ms"
            elif idx == 4:
                unit = "chars"
            elif idx == 5:
                unit = "tuples"
            else:
                unit = "ns"
            plt.ylabel("Latency [{}]".format(unit))

            plt.savefig("{}/../results-bar/fig/trend-{}-{}-{}.png".format(outputFileDir, l_type, idx2name(idx), approach))
            plt.close()