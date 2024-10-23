import os, sys
import heapq
import time
import glob
import numpy as np
import matplotlib.pyplot as plt

def obtain_start_idx(elements, value):
    for idx, element in enumerate(elements):
        if element >= value:
            return idx
    raise Exception

def obtain_end_idx(elements, value):
    for idx, element in enumerate(elements):
        if element > value:
            return idx
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
    filter_rate = 0.1

    if not os.path.exists("{}/../results-thtest/fig".format(outputFileDir)):
        os.makedirs("{}/../results-thtest/fig".format(outputFileDir))

    if len(glob.glob("{}/{}_[0-9]*.csv".format(outputFileDir, size))) != parallelism:
        print("EXIT")
        sys.exit()

    start_time = time.time()
    count_all = 0
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
            count_all += 1
            heapq.heappush(pq, (ts, data))
        else:
            f.close()

    current_window_time = pq[0][0] // 1000

    """
    Calculate latency trends every seconds
    """
    with open("{}/../results-thtest/trend-{}-{}.csv".format(outputFileDir, size, approach), "w") as w:
        latency_values_one_sec = []
        count_history = []
        latency_trends = []
        while True:
            if len(pq) == 0:
                median = np.median(latency_values_one_sec)

                print("{},{},{}".format(current_window_time, median, len(latency_values_one_sec)))
                w.write("{},{},{}\n".format(current_window_time, median, len(latency_values_one_sec)))

                latency_trends.append(median)
                count_history.append(count_all)
                latency_values_one_sec.clear()
                break

            # elements format in pq: [ts, data]
            data = heapq.heappop(pq)[1]
            partition = data[0]
            ts = data[1]

            if ts // 1000 != current_window_time:
                median = np.median(latency_values_one_sec)

                print("{},{},{}".format(current_window_time, median, len(latency_values_one_sec)))
                w.write("{},{},{}\n".format(current_window_time, median, len(latency_values_one_sec)))

                latency_trends.append(median)
                count_history.append(count_all)
                latency_values_one_sec.clear()

                while (ts//1000) - 1 > current_window_time:
                    latency_trends.append(np.nan)
                    count_history.append(count_all)
                    current_window_time += 1
                current_window_time = ts // 1000

            latency_values_one_sec.append(data[2])

            line = f_list[partition].readline()
            if (line != ""):
                data = list(map(int, line.split(",")))
                ts = data[1]
                count_all += 1
                heapq.heappush(pq, (ts, data))
            else:
                f_list[partition].close()

    end_time = time.time()

    print("Time duration: {} [s]".format(end_time-start_time))
    #w.write("Time duration: {} [s]\n".format(end_time-start_time))

    start_idx = count_all * 25 // 100
    end_idx = count_all * 90 // 100
    pivot_idx = (end_idx - start_idx) * 10 // 100
    start_idx_end = start_idx + pivot_idx
    end_idx_start = end_idx - pivot_idx
    """
    Translate lists to nparray
    """
    latency_trends = np.array(latency_trends)

    # Plot trend data
    column_num = len(latency_trends)
    plt.title("S2S Latency (median)")
    plt.plot(latency_trends)
    plt.vlines(obtain_start_idx(count_history, start_idx) - 0.5, 0, max(latency_trends))
    plt.vlines(obtain_end_idx(count_history, start_idx_end) - 0.5, 0, max(latency_trends))
    plt.vlines(obtain_start_idx(count_history, end_idx_start) - 0.5, 0, max(latency_trends))
    plt.vlines(obtain_end_idx(count_history, end_idx) - 0.5, 0, max(latency_trends))
    plt.ylim(bottom=0)
    plt.ylabel("Latency [ns]")

    plt.savefig("{}/../results-thtest/fig/trend-median-S2S-{}-{}.png".format(outputFileDir, approach, size))
    plt.close()