import os, sys
import glob
import numpy as np
from matplotlib import pyplot as plt

def get_sizes(files):
    ret = set()
    for file in files:
        elements = os.path.basename(file).split("-")
        print(elements)
        if len(elements) == 2:
            size = int(elements[0])
        elif len(elements) == 3:
            size = -1 * int(elements[1])
        else:
            print(files, file, elements)
            raise Exception
        ret.add(size)
    return sorted(list(ret))

def write_log(results, queries, name, w):
    header = name + "\n"
    for query in queries:
        for size in list(results[query].keys()):
            if size != -1:
                header = header + "," + query + "-" + str(size)
            else:
                header = header + "," + query
    w.write(header + "\n")
    data_line = name
    for query in queries:
        for size in list(results[query].keys()):
            data_line = data_line + "," + str(results[query][size][name])
    w.write(data_line + "\n\n")


if __name__ == "__main__":
    results = {}
    queries = ["LR", "Nexmark", "Nexmark2", "NYC", "NYC2", "YSB", "YSB2", "Syn1", "Syn2", "Syn3"]

    if not os.path.exists("./results"):
        os.makedirs("./results")
    if not os.path.exists("./results/fig"):
        os.makedirs("./results/fig")

    for query in queries:
        # init 'results'
        results[query] = {}

        # get sizelist
        files = glob.glob("./{}/*".format(query))
        sizes = get_sizes(files)
        for size in sizes:
            # init 'results'
            results[query][size] = {}

            # obtain data
            trigger_raw_data = np.loadtxt("./{}/{}-trigger.log".format(query, size), delimiter=",")
            monitor_raw_data = np.loadtxt("./{}/{}-monitor.log".format(query, size), delimiter=",")

            # finding checkpoint duration
            trigger_data = trigger_raw_data[:,2] - trigger_raw_data[:,1]
            results[query][size]["trigger_mean"] = np.mean(trigger_data)
            results[query][size]["trigger_median"] = np.median(trigger_data)
            results[query][size]["trigger_std"] = np.std(trigger_data)
            results[query][size]["trigger_count"] = len(trigger_data)
            plt.hist(trigger_data)
            plt.savefig("./results/fig/{}-{}-trigger.png".format(query, size))
            plt.close()

            # replay duration
            replay_data = monitor_raw_data[:,2] - trigger_raw_data[:,2]
            results[query][size]["replay_mean"] = np.mean(replay_data)
            results[query][size]["replay_median"] = np.median(replay_data)
            results[query][size]["replay_std"] = np.std(replay_data)
            results[query][size]["replay_count"] = len(replay_data)
            plt.hist(replay_data)
            plt.savefig("./results/fig/{}-{}-replay.png".format(query, size))
            plt.close()

    # Create bar plot (mean)
    d_vals = []
    std_d_vals = []
    t_vals = []
    std_t_vals = []
    labels = []
    for query in queries:
        for size in list(results[query].keys()):
            d_vals.append(results[query][size]["replay_mean"])
            std_d_vals.append(results[query][size]["replay_std"])
            t_vals.append(results[query][size]["trigger_mean"])
            std_t_vals.append(results[query][size]["trigger_std"])
            if size != -1:
                labels.append("{}-{}".format(query, size))
            else:
                labels.append("{}".format(query))
    plt.bar(labels, d_vals, yerr=std_d_vals)
    plt.xticks(rotation=45)
    plt.savefig("./results/replay_mean.png")
    plt.close()
    plt.bar(labels, t_vals, yerr=std_t_vals)
    plt.xticks(rotation=45)
    plt.savefig("./results/trigger_mean.png")
    plt.close()

    # Create bar plot (median)
    d_vals = []
    t_vals = []
    labels = []
    for query in queries:
        for size in list(results[query].keys()):
            d_vals.append(results[query][size]["replay_median"])
            t_vals.append(results[query][size]["trigger_median"])
            if size != -1:
                labels.append("{}-{}".format(query, size))
            else:
                labels.append("{}".format(query))
    plt.bar(labels, d_vals)
    plt.xticks(rotation=45)
    plt.savefig("./results/replay_median.png")
    plt.close()
    plt.bar(labels, t_vals)
    plt.xticks(rotation=45)
    plt.savefig("./results/trigger_median.png")
    plt.close()

    # Write results to a file
    with open("./results/results.log", "w") as w:
        ## trigger (mean)
        write_log(results, queries, "trigger_mean", w)
        ## trigger (std)
        write_log(results, queries, "trigger_std", w)
        ## trigger (median)
        write_log(results, queries, "trigger_median", w)

        ## replay (mean)
        write_log(results, queries, "replay_mean", w)
        ## replay (std)
        write_log(results, queries, "replay_std", w)
        ## replay (median)
        write_log(results, queries, "replay_median", w)

        ## trigger (count)
        write_log(results, queries, "trigger_count", w)
        ## replay (count)
        write_log(results, queries, "replay_count", w)
