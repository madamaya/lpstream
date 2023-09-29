import glob

def str2list(str, delemeter=","):
    return str.split(delemeter)

def list2str(e, delemeter=","):
    return delemeter.join(e)

if __name__ == "__main__":
    targetDir = "./csv/fhvhv"
    tmpList = list()

    files = glob.glob(targetDir + "/*")
    for file in files:
        print(file)
        with open(file) as f:
            lines = f.readlines()
            tmpList = tmpList + lines

    mainList = [line.rstrip("\n") for line in tmpList]

    mainList.sort(key=lambda x: x.split(',')[3])

    with open("fhvhv.csv", "w") as w:
        for line in mainList:
            w.write(line + "\n")
