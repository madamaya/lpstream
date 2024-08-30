import os, sys
import time
import json
import numpy as np
import matplotlib.pyplot as plt

filterRate = 0.1
startTime = time.time()

def arg_parser(elements):
    queries = elements[0].split()
    approaches = elements[1].split()
    dataSizes = list(map(int, elements[2].split()))
    filepath = elements[3]
    return queries, approaches, dataSizes, filepath


def format_print_results(results, queries, approaches, size, latency_type, val_type, filepath):
    with open("{}/results-throughput-detail/{}.csv".format(filepath, size), "a") as w:
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

def type2unit(latency_type):
    if latency_type == "S2S":
        return "ns"
    elif latency_type == "K2K":
        return "ms"
    elif latency_type == "DOM":
        return "ns"
    elif latency_type == "TRAVERSE":
        return "ns"
    else:
        raise Exception


def make_plot_graph(results, queries, approaches, size, latency_type, val_type, filepath):
    for query in queries:
        value_list = []
        colorList = []
        for approach in approaches:
            # Color
            if approach == "baseline":
                colorList.append("b")
            elif approach == "genealog":
                colorList.append("g")
            elif approach == "l3stream":
                colorList.append("r")
            else:
                colorList.append("m")

            if (query not in results) or \
                    (approach not in results[query]) or \
                    (latency_type not in results[query][approach]) or \
                    (val_type not in results[query][approach][latency_type]):
                value_list.append(np.nan)
            else:
                value_list.append(results[query][approach][latency_type][val_type])

        plt.bar(range(len(value_list)), value_list, tick_label=approaches, color=colorList)
        plt.title("{} - {} - {} - {}".format(query, latency_type, val_type, size))
        plt.ylabel("{} [{}]".format(latency_type, type2unit(latency_type)))
        plt.savefig("{}/results-throughput-detail/figs/{}-{}-{}-{}.png".format(filepath, query, latency_type, val_type, size))
        plt.close()


if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("IllegalArguments: len(sys.argv) = {}".format(len(sys.argv)))
        exit(1)

    # argv[1]: queries, argv[2]: approaches, argv[3]: dataSizes
    queries, approaches, dataSizes, filepath = arg_parser(sys.argv[1:])

    print("queries = {}, type = {}".format(queries, type(queries)))
    print("approaches = {}, type = {}".format(approaches, type(approaches)))
    print("dataSizes = {}, type = {}".format(dataSizes, type(dataSizes)))
    print("filepath = {}, type = {}".format(filepath, type(filepath)))

    if not os.path.exists("{}/results-throughput-detail/figs".format(filepath)):
        os.makedirs("{}/results-throughput-detail/figs".format(filepath))

    for size in dataSizes:
        results = {}
        for query in queries:
            for approach in approaches:
                # invalid cases
                if ("Syn" in query and size == -1) or ("Syn" not in query and size != -1):
                    continue

                filePath = "{}/{}/results-throughput-detail/result-{}-{}.json".format(filepath, query, size, approach)
                if os.path.exists(filePath):
                    with open(filePath) as f:
                        j_data = json.load(f)
                    if query not in results:
                        results[query] = {}
                    results[query][approach] = j_data

        # Write results with csv format
        for l_type in ["S2S", "K2K", "DOM", "TRAVERSE"]:
            for v_type in ["median", "mean", "std"]:
                format_print_results(results, queries, approaches, size, l_type, v_type, filepath)

        # Plot result
        for l_type in ["S2S", "K2K", "DOM", "TRAVERSE"]:
            for v_type in ["median", "mean"]:
                make_plot_graph(results, queries, approaches, size, l_type, v_type, filepath)