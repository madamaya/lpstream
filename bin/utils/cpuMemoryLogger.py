import sys
import time
import requests
import numpy as np

getInterval = 1 # 1 sec
metrics = {"CPU": ["Status.JVM.CPU.Load"],
           "Memory": ["Status.JVM.Memory.Heap.Used",
                      "Status.JVM.Memory.NonHeap.Used",
                      "Status.JVM.Memory.Mapped.MemoryUsed",
                      "Status.JVM.Memory.Direct.MemoryUsed"]
           }

if __name__ == "__main__":
    assert len(sys.argv) > 2
    flinkJM = sys.argv[1]
    TMids = [sys.argv[idx+2].replace("\"", "") for idx in range(len(sys.argv)-2)]

    prevTime = time.time()
    while True:
        # Get CPU metrics
        cpuUsedList = []
        for TMid in TMids:
            res = requests.get("http://{}/taskmanagers/{}/metrics?get={}".format(flinkJM, TMid, ",".join(metrics["CPU"])))
            print("res.text = {}".format(res.text))
            cpuUsedList.append(float(res.json()[0]["value"]) * 100)
        cpuUsed = sum(cpuUsedList)

        # Get Memory metrics
        memoryUsedList = []
        for TMid in TMids:
            res = requests.get("http://" + flinkJM + "/taskmanagers/{}/metrics?get={}".format(TMid, ",".join(metrics["Memory"])))
            memoryUsedList.append(sum([float(element["value"]) for element in res.json()]))
        memoryUsed = sum(memoryUsedList)

        print("{}".format(time.time()))
        print("CPU:\t{}".format(cpuUsed))
        print("Memory:\t{}".format(memoryUsed))

        time.sleep(getInterval - (time.time() - prevTime))
        prevTime = time.time()
