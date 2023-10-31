import os
import glob
import time
from datetime import datetime
from tqdm import tqdm

def extractYearMonth(line):
    # (sample) tsLine = 2001-01-01 00:16:31
    tsLine = line.split(",")[2]
    dtime = datetime.strptime(tsLine, "%Y-%m-%d %H:%M:%S")
    return dtime.strftime("%Y-%m")

if __name__ == "__main__":
    targetDir = "./csv/yellow"
    tmpList = list()

    print("*** Get files and Write data ***")
    files = glob.glob(targetDir + "/*")

    if not os.path.exists("{}/log".format(targetDir)):
        os.makedirs("{}/log".format(targetDir))

    logFileMap = {}
    for file in files:
        print("now reading" + str(file))
        with open(file) as f:
            while True:
                line = f.readline()
                if line == "":
                    break
                ym = extractYearMonth(line)

                if ym not in logFileMap:
                    logFileMap[ym] = open("{}/log/{}.log".format(targetDir, ym), "w")
                logFileMap[ym].write(line)

    print("*** Close files ***")
    for key in logFileMap.keys():
        logFileMap[key].close()

    print("*** Sort each file ***")
    for key in logFileMap.keys():
        print("*** Read ({}) ***".format(key))
        with open("{}/log/{}.log".format(targetDir, key)) as f:
            lines = f.readlines()

        print("*** Sort ({}) ***".format(key))
        lines.sort(key=lambda x: datetime.strptime(x.split(",")[2], "%Y-%m-%d %H:%M:%S"))

        print("*** Write ({}) ***".format(key))
        with open("{}/log/{}.sorted.log".format(targetDir, key), "w") as w:
            w.writelines(lines)
