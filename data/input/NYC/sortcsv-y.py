import os
import glob
import time
from tqdm import tqdm

def str2list(str, delemeter=","):
    return str.split(delemeter)

def list2str(e, delemeter=","):
    return delemeter.join(e)

if __name__ == "__main__":
    targetDir = "../data/csv"
    tmpList = list()

    if not os.path.exists("../data/csv"):
        os.makedirs("../data/csv")

    print("*** Get files ***")
    files = glob.glob(targetDir + "/*")
    for file in files:
        print("now reading" + str(file))
        with open(file) as f:
            lines = f.readlines()
            tmpList = tmpList + lines

    #print("*** Remove newline from each data ***")
    #mainList = [line.rstrip("\n") for line in tqdm(tmpList)]

    print("*** Sort data ***")
    #mainList.sort(key=lambda x: x.split(',')[2])
    tmpList.sort(key=lambda x: x.split(',')[2])

    print("*** Write ***")
    cnt = 0
    #lgth = len(mainList)
    lgth = len(tmpList)
    numofonepercent = lgth // 100

    start_time = time.time()
    prev_time = time.time()
    current_time = time.time()
    with open("../data/nyc.csv", "w") as w:
        #for line in mainList:
        for line in tmpList:
            cnt = cnt + 1
            #w.write('"' + line  + '"' + "\n")
            w.write(line)

            if cnt % numofonepercent == 0:
                current_time = time.time()
                print("writing: {}/{} {}%".format(cnt, lgth, cnt * 100 // lgth))
                print("Duration for all: {}[s]".format(current_time - start_time))
                print("Duration for this one segment: {}[s]".format(current_time - prev_time))
                prev_time = current_time

    print("*** End of the program (sortcsv-y.py) ***")
    print("*** Execution Time: {}[s] ***".format(time.time() - start_time))