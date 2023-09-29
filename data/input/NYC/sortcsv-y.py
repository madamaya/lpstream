import glob
import time

def str2list(str, delemeter=","):
    return str.split(delemeter)

def list2str(e, delemeter=","):
    return delemeter.join(e)

if __name__ == "__main__":
    targetDir = "./csv/yellow"
    tmpList = list()

    print("get files")
    files = glob.glob(targetDir + "/*")
    for file in files:
        print("now reading" + str(file))
        with open(file) as f:
            lines = f.readlines()
            tmpList = tmpList + lines

    print("remove newline code")
    mainList = [line.rstrip("\n") for line in tmpList]

    print("sort mainList")
    mainList.sort(key=lambda x: x.split(',')[2])

    print("writing")
    cnt = 0
    lgth = len(mainList)
    numofonepercent = lgth // 100

    start_time = time.time()
    prev_time = time.time()
    current_time = time.time()
    with open("yellow.csv", "w") as w:
        for line in mainList:
            cnt = cnt + 1
            w.write('"' + line  + '"' + "\n")

            if cnt % numofonepercent == 0:
                current_time = time.time()
                print("writing: {}/{} {}%".format(cnt, lgth, cnt * 100 // lgth))
                print("Duration for all: {}[s]".format(current_time - start_time))
                print("Duration for this one segment: {}[s]".format(current_time - prev_time))
                prev_time = current_time

    print("End of the program")
    print("Execution Time: {}[s]".format(time.time() - start_time))