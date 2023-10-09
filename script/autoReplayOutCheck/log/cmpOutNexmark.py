import time
import json
import bisect
from tqdm import tqdm

def extractTupleOUT(line):
    return json.loads(line)["OUT"]

def extractTupleCpID(line):
    return json.loads(line)["CPID"]

# Duplication exists -> True, Otherwise -> False
def checkDuplication(lines):
    s = set()
    for line in lines:
        if line in s:
            return True
        s.add(line)
    return False

def createSetOUT(lines):
    s = set()
    for line in lines:
        s.add(extractTupleOUT(line))
    return s

def createList(lines):
    s = list()
    for line in lines:
        s.append(line)
    return s

def createSetOUTFromFile(filePath):
    with open(filePath) as f:
        lines = f.readlines()
    return createSetOUT(lines)

def createListFromFile(filePath):
    with open(filePath) as f:
        lines = f.readlines()
    return createList(lines)

def extractTs(line):
    return int(line.split(",")[5].replace(" ", "").replace("}", "").split("=")[1])

if __name__ == "__main__":
    testName = "Nexmark"
    baselinePath = "./{}/{}0.log.log".format(testName.lower(), testName)

    print("baselineOut = createSetFromFile(baselinePath)")
    baselineOut = createListFromFile(baselinePath)

    print("baselineOutSet = createSetOUTFromFile(baselinePath)")
    baselineOutSet = createSetOUTFromFile(baselinePath)

    cpmp = {}
    for line in baselineOut:
        e = json.loads(line)
        cpid = e["CPID"]
        ts = extractTs(e["OUT"])
        if cpid not in cpmp:
            cpmp[cpid] = ts
        else:
            cpmp[cpid] = max(cpmp[cpid], ts)
    cplist = sorted(list(cpmp.values()))

    numOfChk = 1
    for idx in range(1, numOfChk + 1):
        testPath = "./{}/{}{}.log.log".format(testName.lower(), testName, idx)

        print("testOut = createSetFromFile(testPath)")
        testOut = createListFromFile(testPath)

        print("testOutSet = createSetOUTFromFile(testPath)")
        testOutSet = createSetOUTFromFile(testPath)

        # check all output of s2 is included in baseline's ones
        # included
        mp = {}
        errorlst = []
        for e in tqdm(testOut):
            jdata = json.loads(e)
            if jdata["OUT"] not in baselineOutSet:
                ts = extractTs(jdata["OUT"])
                idx = bisect.bisect_left(cplist, ts) + 1
                if idx not in mp:
                    mp[idx] = 1
                else:
                    mp[idx] += 1
                errorlst.append(jdata)

        # check all cpid of baseline output from testOut < starting cpid
        errorlst2 = []
        mp2 = {}
        for e in tqdm(baselineOut):
            jdata = json.loads(e)
            if jdata["OUT"] not in testOutSet:
                cpid = int(jdata["CPID"])
                if cpid > idx:
                    errorlst2.append(jdata)
                    if cpid not in mp2:
                        mp2[cpid] = 1
                    else:
                        mp2[cpid] += 1

        f = True
        if len(errorlst) > 0:
            print("idx={} ❌".format(idx))
            print("mp={}".format(mp))
            print("ERROR: errorlst")
            f = False
        if len(errorlst2) > 0:
            print("idx={} ❌".format(idx))
            print("mp2={}".format(mp2))
            print("ERROR: errorlst2")
            f = False

        if f:
            print("idx={} ✅".format(idx))
