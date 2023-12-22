import glob
import pandas as pd
import matplotlib.pyplot as plt

def shapeData(queries, approaches, results, rawData, maxCount):
    cpu = []
    cpu_raw = []
    memory = []
    memory_raw = []
    for idx in range(maxCount):
        cpu_tmp = {}
        cpu_raw_tmp = {}
        memory_tmp = {}
        memory_raw_tmp = {}
        for query in queries:
            cpu_tmp[query] = []
            cpu_raw_tmp[query] = []
            memory_tmp[query] = []
            memory_raw_tmp[query] = []
            for approach in approaches:
                cpu_value = results[query][approach]["CPU"]["value"][idx] if idx < len(results[query][approach]["CPU"]["value"]) else None
                cpu_std = results[query][approach]["CPU"]["std"][idx] if idx < len(results[query][approach]["CPU"]["std"]) else None
                cpu_raw_list = rawData[query][approach]["CPU"][idx] if idx < len(rawData[query][approach]["CPU"]) else []
                memory_value = results[query][approach]["Memory"]["value"][idx] if idx < len(results[query][approach]["Memory"]["value"]) else None
                memory_std = results[query][approach]["Memory"]["std"][idx] if idx < len(results[query][approach]["Memory"]["std"]) else None
                memory_raw_list = rawData[query][approach]["Memory"][idx] if idx < len(rawData[query][approach]["Memory"]) else []

                cpu_tmp[query].append([cpu_value, cpu_std])
                cpu_raw_tmp[query].append(cpu_raw_list)
                memory_tmp[query].append([memory_value, memory_std])
                memory_raw_tmp[query].append(memory_raw_list)

        cpu.append(cpu_tmp)
        cpu_raw.append(cpu_raw_tmp)
        memory.append(memory_tmp)
        memory_raw.append(memory_raw_tmp)

    return cpu, cpu_raw, memory, memory_raw

def plotData(queries, approaches, cpu, memory, cpu_raw, memory_raw, maxCount):
    for idx in range(maxCount):
        for query in queries:
            plt.bar(range(len(cpu[idx][query])), [tuple[0] for tuple in cpu[idx][query]], tick_label=approaches)
            plt.title("CPU - {} - {}".format(query, idx))
            plt.ylabel("CPU Usage [%]")
            plt.savefig("./results/cpu.{}.{}.pdf".format(query, idx))
            plt.close()

            plt.bar(range(len(memory[idx][query])), [tuple[0] for tuple in memory[idx][query]], tick_label=approaches)
            plt.title("Memory - {} - {}".format(query, idx))
            plt.ylabel("Memory Used [B]")
            plt.savefig("./results/memory.{}.{}.pdf".format(query, idx))
            plt.close()

    for idx in range(maxCount):
        for query in queries:
            for value in cpu_raw[idx][query]:
                plt.plot(range(len(value)), value)
            plt.title("CPU - {} - {}".format(query, idx))
            plt.xlabel("Timestamp [s]")
            plt.ylabel("CPU Usage [%]")
            plt.legend(approaches)
            plt.savefig("./results/figs/cpu.{}.{}.pdf".format(query, idx))
            plt.close()

            for value in memory_raw[idx][query]:
                plt.plot(range(len(value)), value)
            plt.title("Memory - {} - {}".format(query, idx))
            plt.xlabel("Timestamp [s]")
            plt.ylabel("Memory Used [B]")
            plt.legend(approaches)
            plt.savefig("./results/figs/memory.{}.{}.pdf".format(query, idx))
            plt.close()


def calcResults(queries, approaches, filterRate, plotTrends, startTime):
    results = {}
    rawData = {}
    maxCount = 0
    for query in queries:
        for approach in approaches:
            files = glob.glob("./{}/{}/*".format(query, approach))
            for file in files:
                df = pd.read_csv(file, header=None, names=["ts", "CPU", "Memory"])
                df = df.iloc[int(df.shape[0]*filterRate):]

                cpuList = df["CPU"].tolist()
                memoryList = df["Memory"].tolist()
                means = df.mean()
                stds = df.std(ddof=0)

                # Store results
                if query not in results:
                    results[query] = {}
                if approach not in results[query]:
                    results[query][approach] = {}
                    results[query][approach]["CPU"] = {}
                    results[query][approach]["Memory"] = {}

                    results[query][approach]["CPU"]["value"] = []
                    results[query][approach]["CPU"]["std"] = []
                    results[query][approach]["Memory"]["value"] = []
                    results[query][approach]["Memory"]["std"] = []
                results[query][approach]["CPU"]["value"].append(means["CPU"])
                maxCount = max(maxCount, len(results[query][approach]["CPU"]["value"]))
                results[query][approach]["CPU"]["std"].append(stds["CPU"])
                results[query][approach]["Memory"]["value"].append(means["Memory"])
                maxCount = max(maxCount, len(results[query][approach]["Memory"]["value"]))
                results[query][approach]["Memory"]["std"].append(stds["Memory"])

                # Store raw data
                if query not in rawData:
                    rawData[query] = {}
                if approach not in rawData[query]:
                    rawData[query][approach] = {}
                    rawData[query][approach]["CPU"] = []
                    rawData[query][approach]["Memory"] = []
                rawData[query][approach]["CPU"].append(cpuList)
                rawData[query][approach]["Memory"].append(memoryList)

    cpu, cpu_raw, memory, memory_raw = shapeData(queries, approaches, results, rawData, maxCount)
    plotData(queries, approaches, cpu, memory, cpu_raw, memory_raw, maxCount)
    return cpu, cpu_raw, memory, memory_raw