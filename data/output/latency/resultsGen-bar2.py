import os, sys
import time
import json
import numpy as np
import matplotlib.pyplot as plt


def arg_parser(elements):
    queries = elements[0].split()
    approaches = elements[1].split()
    dataSizes = list(map(int, elements[2].split()))
    return queries, approaches, dataSizes


def format_print_results(results, queries, approaches, size, latency_type, val_type):
    with open("./results-bar/{}.csv".format(size), "a") as w:
        w.write("#{},{}\n".format(latency_type, val_type))
        w.write(",{}\n".format(",".join(approaches)))
        for query in queries:
            w.write("{}".format(query))
            for approach in approaches:
                if (query not in results) or \
                        (approach not in results[query]) or \
                        (latency_type not in results[query][approach]) or \
                        (val_type not in results[query][approach][latency_type]):
                    w.write(",{}".format("nan"))
                else:
                    w.write(",{}".format(results[query][approach][latency_type][val_type]))
            w.write("\n")
        w.write("\n")

def make_sub_graph(s2s_list_all, query, size, idx, ax):
    if query == "LR" or query == "Syn1":
        ax[idx//6,idx%6].set_ylabel("[µs]", rotation=0)
    else:
        ax[idx//6,idx%6].set_ylabel("[s]", rotation=0)
    ax[idx//6,idx%6].yaxis.set_label_coords(-0.1, 1)

    ax[idx//6,idx%6].boxplot(s2s_list_all, showfliers=False)
    ax[idx//6,idx%6].set_xticklabels(["B", "G", "O", "P"])

    if "Syn" not in query:
        title = query
    else:
        if query == "Syn1":
            title = "SynA"
        elif query == "Syn2":
            title = "SynB"
        elif query == "Syn3":
            title = "SynC"
        else:
            raise Exception

    if size == -1:
        ax[idx//6,idx%6].set_title("{}".format(title))
    else:
        ax[idx//6,idx%6].set_title("{} ({})".format(title, size))


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

    if not os.path.exists("./results-bar/figs"):
        os.makedirs("./results-bar/figs")

    for size in dataSizes:
        results = {}
        for query in queries:
            for approach in approaches:
                # invalid cases
                if ("Syn" in query and size == -1) or ("Syn" not in query and size != -1):
                    continue

                filePath = "./{}/results/result-{}-{}.json".format(query, size, approach)
                if os.path.exists(filePath):
                    with open(filePath) as f:
                        j_data = json.load(f)
                    if query not in results:
                        results[query] = {}
                    results[query][approach] = j_data

        # Write results with csv format
        for l_type in ["S2S", "K2K", "DOM", "TRAVERSE", "SIZE(Output)", "SIZE(Lineage)"]:
            for v_type in ["median", "mean", "std", "ifMed", "ifMean"]:
                format_print_results(results, queries, approaches, size, l_type, v_type)


    fig, ax = plt.subplots(3, 6, figsize=(16,10))
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

    while idx < 18:
        ax[idx//6,idx%6].set_axis_off()
        idx += 1

    fig.tight_layout()
    plt.savefig("./results-bar/figs/latencies-box.pdf")