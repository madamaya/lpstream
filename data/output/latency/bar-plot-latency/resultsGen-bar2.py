import os, sys
import time
import json
import numpy as np
import matplotlib.pyplot as plt

startTime = time.time()

def arg_parser(elements):
    queries = elements[0].split()
    approaches = elements[1].split()
    dataSizes = list(map(int, elements[2].split()))
    return queries, approaches, dataSizes

def make_sub_graph(s2s_list_all, query, size, idx, ax):
    if query == "LR" or query == "Syn1":
        #s2s_list_all = s2s_list_all / 1000
        ax[idx//4,idx%4].set_ylabel("Latency [µs]")
    else:
        #s2s_list_all = s2s_list_all / 1000000000
        ax[idx//4,idx%4].set_ylabel("Latency [s]")

    ax[idx//4,idx%4].boxplot(s2s_list_all, showfliers=False)
    ax[idx//4,idx%4].set_xticklabels(["baseline", "genealog", "ordinary", "provenance"])
    if size == -1:
        ax[idx//4,idx%4].set_title("{}".format(query))
    else:
        ax[idx//4,idx%4].set_title("{} ({})".format(query, size))


def type2unit(latency_type):
    if latency_type == "S2S":
        return "ns"
    elif latency_type == "K2K":
        return "ms"
    elif latency_type == "DOM":
        return "ns"
    elif latency_type == "TRAVERSE":
        return "ns"
    elif latency_type == "SIZE(Output)":
        return "chars"
    else:
        raise Exception


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("IllegalArguments: len(sys.argv) = {}".format(len(sys.argv)))
        exit(1)

    # argv[1]: queries, argv[2]: approaches, argv[3]: dataSizes
    queries, approaches, dataSizes = arg_parser(sys.argv[1:])

    print("queries = {}, type = {}".format(queries, type(queries)))
    print("approaches = {}, type = {}".format(approaches, type(approaches)))
    print("dataSizes = {}, type = {}".format(dataSizes, type(dataSizes)))

    if not os.path.exists("./results-bar2/figs"):
        os.makedirs("./results-bar2/figs")

    fig, ax = plt.subplots(4, 4, figsize=(16,10))
    idx = 0
    for query in queries:
        for size in dataSizes:
            # invalid cases
            if ("Syn" in query and size == -1) or ("Syn" not in query and size != -1):
                continue

            s2s_list_all = []
            for approach in approaches:
                s2s_list = []
                with open("./{}/results-bar/valid_data-{}-{}.log".format(query, size, approach)) as f:
                    while True:
                        line = f.readline()
                        if line == "":
                            break
                        elements = list(map(int, line.split(",")))
                        assert len(elements) == 6
                        if query == "LR" or query == "Syn1":
                            s2s_list.append(elements[0]/1000)
                        else:
                            s2s_list.append(elements[0]/1000000000)

                s2s_list_all.append(s2s_list)

            # Write subplot
            make_sub_graph(s2s_list_all, query, size, idx, ax)
            idx += 1

    fig.tight_layout()
    plt.savefig("./results-bar2/figs/latencies.png")