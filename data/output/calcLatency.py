import os, sys
import heapq
import time
import numpy as np

# Given:
## parallelism (int)
## outputFileDir (str)
## key (str)
if __name__ == "__main__":
    assert len(sys.argv) == 4

    parallelism = int(sys.argv[1])
    outputFileDir = sys.argv[2]
    key = sys.argv[3]

    start_time = time.time()
    pq = []
    f_list = []
    for idx in range(parallelism):
        f = open("{}/{}_{}.csv".format(outputFileDir, key, idx))
        f_list.append(f)

        line = f.readline()
        if (line != ""):
            elements = line.split(",")
            partition = int(elements[0])
            ts = int(elements[1])
            latency = int(elements[2])
            heapq.heappush(pq, (ts, partition, latency))
        else:
            f.close()

    current_window_time = pq[0][0] // 1000

    with open("{}/{}.csv".format(outputFileDir, key), "w") as w:
        latency_values_one_sec = []
        latency_values_all = []
        while True:
            if len(pq) == 0:
                median = np.median(latency_values_one_sec)
                mean = np.mean(latency_values_one_sec)
                std = np.std(latency_values_one_sec)

                print("{},{},{},{},{}".format(current_window_time, median, mean, std, len(latency_values_one_sec)))
                w.write("{},{},{},{},{}\n".format(current_window_time, median, mean, std, len(latency_values_one_sec)))

                latency_values_all += latency_values_one_sec
                latency_values_one_sec.clear()
                break

            tuple = heapq.heappop(pq)
            ts = tuple[0]
            partition = tuple[1]
            latency = tuple[2]

            if ts // 1000 != current_window_time:
                median = np.median(latency_values_one_sec)
                mean = np.mean(latency_values_one_sec)
                std = np.std(latency_values_one_sec)

                print("{},{},{},{},{}".format(current_window_time, median, mean, std, len(latency_values_one_sec)))
                w.write("{},{},{},{},{}\n".format(current_window_time, median, mean, std, len(latency_values_one_sec)))

                latency_values_all += latency_values_one_sec
                latency_values_one_sec.clear()
                current_window_time = ts // 1000

            latency_values_one_sec.append(latency)

            line = f_list[partition].readline()
            if (line != ""):
                elements = line.split(",")
                partition = int(elements[0])
                ts = int(elements[1])
                latency = int(elements[2])
                heapq.heappush(pq, (ts, partition, latency))
            else:
                f_list[partition].close()

        median = np.median(latency_values_all)
        mean = np.mean(latency_values_all)
        std = np.std(latency_values_all)

        end_time = time.time()
        print("All,{},{},{},{}".format(median, mean, std, len(latency_values_all)))
        w.write("All,{},{},{},{}\n".format(median, mean, std, len(latency_values_all)))
        print("Time duration: {} [ms]".format(end_time-start_time))
        w.write("Time duration: {} [ms]\n".format(end_time-start_time))
