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

    # Create baseline output set
    baselinePath = logDir + "/groundTruth.log"
    baselineSet = set()
    baselineList = getOutputList(baselinePath)
    print("*** CREATE BASELINE OUTPUT SET ***")
    for line in tqdm(baselineList):
        if parseFlag == 1:
            baselineSet.add(parseNYCOutputAndRoundAvg(json.loads(line)["OUT"]))
        else:
            baselineSet.add(json.loads(line)["OUT"])

    result = True
    for idx in range(startChkID, finalChkID + 1):
        replayPath = logDir + "/output_from_chk-{}.log".format(idx)

        # TEST1: Replay -> Baseline
        print("*** TEST1: Replay -> Baseline ***")
        idx = 0
        removedIdx = []
        removedOut = []
        replaySet = set()
        replayList = getOutputList(replayPath)
        for line in tqdm(replayList):
            idx = idx + 1
            if parseFlag == 1:
                output = parseNYCOutputAndRoundAvg(json.loads(line)["OUT"])
            else:
                output = json.loads(line)["OUT"]
            replaySet.add(output)
            if output not in baselineSet:
                removedIdx.append(idx)
                removedOut.append(output)

        # TEST2: Baseline -> Replay
        print("*** TEST2: Baseline -> Replay ***")
        idx2 = 0
        removedIdx2 = []
        removedOut2 = []
        for line in tqdm(baselineList):
            idx2 = idx2 + 1
            if parseFlag == 1:
                jdata = json.loads(line)
                output = parseNYCOutputAndRoundAvg(jdata["OUT"])
                cpid = int(jdata["CPID"])
            else:
                jdata = json.loads(line)
                output = jdata["OUT"]
                cpid = int(jdata["CPID"])

            if cpid > idx and output not in replaySet:
                removedIdx2.append(idx2)
                removedOut2.append(output)

        print("RESULT:::")
        print("  [ Replay -> Baseline ] len(removedIdx) = {},".format(len(removedIdx)))
        print("  [ Baseline -> Replay ] len(removedIdx2) = {},".format(len(removedIdx2)))
        print()
        print("VALIABLES:::")
        print("  len(baselineList) = {},".format(len(baselineList)))
        print("  len(baselineSet) = {},".format(len(baselineSet)))
        print("  len(replaySet) = {})".format(len(replaySet)))

        result = result and len(removedIdx) == 0 and len(removedIdx2) == 0

        if len(removedIdx) == 0 and len(removedIdx2) == 0:
            print("idx = {} ✅".format(idx))
        else:
            print("idx = {} ❌".format(idx))

    if result == True:
        print("result = ✅".format(idx))
    else:
        print("result = ❌".format(idx))