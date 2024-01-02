import sys
import json
from tqdm import tqdm

def getOutputList(filePath):
    with open(filePath) as f:
        return [line.replace("\n", "") for line in f.readlines()]

def parseNYCOutputAndRoundAvg(line):
    jdata = json.loads(line.replace("NYCResultTuple", "").replace("vendorId", "\"vendorId\"").replace("dropoffLocationId", "\"dropoffLocationId\"").replace("count", "\"count\"").replace("avgDistance", "\"avgDistance\"").replace("ts", "\"ts\"").replace("=", ":"))
    # jdata["avgDistance"] = round(jdata["avgDistance"], 2)
    jdata["avgDistance"] = 0
    return json.dumps(jdata)

if __name__ == "__main__":
    # assert len(sys.argv) == 4

    logDir = sys.argv[1]
    startChkID = int(sys.argv[2])
    finalChkID = int(sys.argv[3])
    parseFlag = int(sys.argv[4])

    # Create nonlineageMode output set
    nonlinPath = logDir + "/nonlineageMode.log"
    nonlinSet = set()
    nonlinList = getOutputList(nonlinPath)
    print("*** CREATE NONLINEAGEMODE OUTPUT SET ***")
    for line in tqdm(nonlinList):
        if parseFlag == 1:
            nonlinSet.add(parseNYCOutputAndRoundAvg(json.loads(line)["OUT"]))
        elif parseFlag == 2:
            nonlinSet.add(json.loads(line.replace("\\", ""))["OUT"])
        else:
            nonlinSet.add(json.loads(line)["OUT"])

    result = True
    # Create baseline output set
    baselinePath = logDir + "/baseline.log"
    baselineSet = set()
    baselineList = getOutputList(baselinePath)
    baselineErrList = []
    print("*** CREATE BASELINE OUTPUT SET ***")
    for line in tqdm(baselineList):
        if parseFlag == 2:
            line = line.replace("\\", "")
        baselineSet.add(line)
        if line not in nonlinSet:
            baselineErrList.append(line)

    # Create genealog output set
    genealogPath = logDir + "/genealog.log"
    genealogSet = set()
    genealogList = getOutputList(genealogPath)
    genealogErrList = []
    print("*** CREATE Genealog OUTPUT SET ***")
    for line in tqdm(genealogList):
        if parseFlag == 2:
            line = line.replace("\\", "")
        genealogSet.add(line)
        if line not in nonlinSet:
            genealogErrList.append(line)

    for idx in range(startChkID, finalChkID + 1):
        replayPath = logDir + "/lineageMode{}.log".format(idx)

        # TEST1: Replay -> Baseline
        print("*** TEST1: Replay -> Baseline ***")
        idx = 0
        removedIdx = []
        removedOut = []
        replaySet = set()
        replayList = getOutputList(replayPath)
        for line in tqdm(replayList):
            idx = idx + 1
            if parseFlag == 2:
                jdata = json.loads(line.replace("\\", ""))
            else:
                jdata = json.loads(line)
            if parseFlag == 1:
                output = parseNYCOutputAndRoundAvg(jdata["OUT"])
            else:
                output = jdata["OUT"]
            replaySet.add(output)
            if output not in nonlinSet:
                removedIdx.append(idx)
                removedOut.append(output)

        # TEST2: Baseline -> Replay
        print("*** TEST2: Baseline -> Replay ***")
        idx2 = 0
        removedIdx2 = []
        removedOut2 = []
        for line in tqdm(nonlinList):
            idx2 = idx2 + 1
            if parseFlag == 2:
                jdata = json.loads(line.replace("\\", ""))
            else:
                jdata = json.loads(line)
            cpid = int(jdata["CPID"])

            if parseFlag == 1:
                output = parseNYCOutputAndRoundAvg(jdata["OUT"])
            else:
                output = jdata["OUT"]

            if cpid > idx and output not in replaySet:
                removedIdx2.append(idx2)
                removedOut2.append(output)

        print("::::::::::RESULT::::::::::")
        print("\t[ Replay -> Nonlineage ] len(removedIdx) = {},".format(len(removedIdx)))
        print("\t[ Nonlineage -> Replay ] len(removedIdx2) = {},".format(len(removedIdx2)))
        print("\t[ Baseline -> Nonlineage ] len(baselineErrList) = {}".format(len(baselineErrList)))
        print("\t[ Genealog -> Nonlineage ] len(genealogErrList) = {}".format(len(genealogErrList)))
        print()
        print("::::::::::VALIABLES::::::::::")
        print("\tlen(nonlinList) = {},".format(len(nonlinList)))
        print("\tlen(nonlinSet) = {},".format(len(nonlinSet)))
        print("\tlen(baselineSet) = {}".format(len(baselineSet)))
        print("\tlen(genealogSet) = {}".format(len(genealogSet)))
        print("\tlen(replaySet) = {})".format(len(replaySet)))

        result = result and len(removedIdx) == 0 and len(removedIdx2) == 0

        if len(removedIdx) == 0 and len(removedIdx2) == 0:
            print("idx = {} ✅".format(idx))
        else:
            print("idx = {} ❌".format(idx))

    result = result and (baselineSet == genealogSet) and (baselineSet == nonlinSet)
    if result == True:
        print("result = ✅")
    else:
        print("result = ❌")