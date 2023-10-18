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

    # argv[1]: baseline out path (ALL), argv[2]: baseline out path (SPLIT), argv[3]: replay out path, argv[4]: starting checkpoint id
    baselineALLpath = sys.argv[1]
    baselineSPLITpath = sys.argv[2]
    replayPath = sys.argv[3]
    startingCheckpointID = int(sys.argv[4])

    # Create baseline output set
    baselineALLset = set()
    baselineALLlist = getOutputList(baselineALLpath)
    print("*** CREATE BASELINE OUTPUT SET ***")
    for line in tqdm(baselineALLlist):
        baselineALLset.add(parseNYCOutputAndRoundAvg(json.loads(line)["OUT"]))

    # Check split output
    idx = 0
    removedIdx = []
    removedOut = []
    baselineSPLITset = set()
    baselineSPLITlist = []
    print("*** CHECK SPLIT OUTPUT ***")
    for line in tqdm(getOutputList(baselineSPLITpath)):
        output = parseNYCOutputAndRoundAvg(json.loads(line)["OUT"])
        baselineSPLITset.add(output)
        cpid = int(json.loads(line)["CPID"])
        idx = idx + 1
        if output not in baselineALLset:
            removedIdx.append(idx)
            removedOut.append(output)
        if cpid > startingCheckpointID:
            baselineSPLITlist.append(output)
    # print("removedIdx = {}".format(removedIdx))

    idx2 = 0
    removedIdx2 = []
    removedOut2 = []
    for line in tqdm(getOutputList(baselineALLpath)):
        idx2 = idx2 + 1
        output = parseNYCOutputAndRoundAvg(json.loads(line)["OUT"])
        if output not in baselineSPLITset:
            removedIdx2.append(idx2)
            removedOut2.append(output)

    # Check replay output
    idx3 = 0
    removedIdx3 = []
    removedOut3 = []
    replaySet = set()
    print("*** CHECK SPLIT OUTPUT ***")
    print("*** PHASE1: ARE ALL SPLIT OUTPUTS INCLUDED IN BASELINE ONES? ***")
    for line in tqdm(getOutputList(replayPath)):
        output = parseNYCOutputAndRoundAvg(json.loads(line)["OUT"])
        replaySet.add(output)
        idx3 = idx3 + 1
        if output not in baselineSPLITset:
            removedIdx3.append(idx3)
            removedOut3.append(output)
    # print("removedIdx = {}".format(removedIdx))

    idx4 = 0
    removedIdx4 = []
    removedOut4 = []
    print("*** PHASE2: ARE ALL BASELINE OUTPUTS, WHOSE CPID IS GREATER THAN startingCheckpointID, INCLUDED IN SPLIT ONES? ***")
    for output in tqdm(baselineSPLITlist):
        idx4 = idx4 + 1
        if output not in replaySet:
            removedIdx4.append(idx4)
            removedOut4.append(output)

    print("RESULT::: len(removedIdx) = {}, len(removedIdx2) = {}, len(removedIdx3) = {}, len(removedIdx4) = {}".format(len(removedIdx), len(removedIdx2), len(removedIdx3), len(removedIdx4)))
    print("( len(baselineALLlist) = {}, len(baselineALLset) = {}, len(baselineSPLITlist) = {}, len(baselineSPLITset) = {}, len(replaySet) = {})".format(len(baselineALLlist), len(baselineALLset), len(baselineSPLITlist), len(baselineSPLITset), len(replaySet)))

    if len(removedIdx) == 0 and len(removedIdx2) == 0 and len(removedIdx3) == 0 and len(removedIdx4):
        print("✅")
    else:
        print("❌")
