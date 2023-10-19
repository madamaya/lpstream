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
    assert 1 == 0
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

    # TEST1: BASELINE(ALL) -> BASELINE(SPLIT)
    print("*** TEST1: BASELINE(ALL) -> BASELINE(SPLIT) ***")
    idx = 0
    removedIdx = []
    removedOut = []
    baselineSPLITset = set()
    baselineSPLITlist = []
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

    # TEST2: BASELINE(SPLIT) -> BASELINE(ALL)
    print("*** TEST2: BASELINE(SPLIT) -> BASELINE(ALL) ***")
    idx2 = 0
    removedIdx2 = []
    removedOut2 = []
    for line in tqdm(getOutputList(baselineALLpath)):
        idx2 = idx2 + 1
        output = parseNYCOutputAndRoundAvg(json.loads(line)["OUT"])
        if output not in baselineSPLITset:
            removedIdx2.append(idx2)
            removedOut2.append(output)

    # TEST3: REPLAY -> BASELINE(SPLIT)
    print("*** TEST3: REPLAY -> BASELINE(SPLIT) ***")
    print("(ARE ALL SPLIT OUTPUTS INCLUDED IN BASELINE ONES?)")
    idx3 = 0
    removedIdx3 = []
    removedOut3 = []
    replaySet = set()
    for line in tqdm(getOutputList(replayPath)):
        output = parseNYCOutputAndRoundAvg(json.loads(line)["OUT"])
        replaySet.add(output)
        idx3 = idx3 + 1
        if output not in baselineSPLITset:
            removedIdx3.append(idx3)
            removedOut3.append(output)

    # BASELINE(SPLIT(cpid > idx)) -> REPLAY
    print("*** TEST4: BASELINE(SPLIT(cpid > idx)) -> REPLAY ***")
    print("(ARE ALL BASELINE OUTPUTS, WHOSE CPID IS GREATER THAN startingCheckpointID, INCLUDED IN SPLIT ONES?)")
    idx4 = 0
    removedIdx4 = []
    removedOut4 = []
    for output in tqdm(baselineSPLITlist):
        idx4 = idx4 + 1
        if output not in replaySet:
            removedIdx4.append(idx4)
            removedOut4.append(output)

    print("RESULT:::")
    print("  [ BASELINE(ALL) -> BASELINE(SPLIT) ] len(removedIdx) = {},".format(len(removedIdx)))
    print("  [ BASELINE(SPLIT) -> BASELINE(ALL) ] len(removedIdx2) = {},".format(len(removedIdx2)))
    print("  [ REPLAY -> BASELINE(SPLIT) ] len(removedIdx3) = {},".format(len(removedIdx3)))
    print("  [ BASELINE(SPLIT(cpid > idx)) -> REPLAY ] len(removedIdx4) = {}".format(len(removedIdx4)))
    print()
    print("VALIABLES:::")
    print("  len(baselineALLlist) = {},".format(len(baselineALLlist)))
    print("  len(baselineALLset) = {},".format(len(baselineALLset)))
    print("  len(baselineSPLITlist) = {},".format(len(baselineSPLITlist)))
    print("  len(baselineSPLITset) = {},".format(len(baselineSPLITset)))
    print("  len(replaySet) = {})".format(len(replaySet)))

    # Correctness
    rule1 = (len(removedIdx) != 0 and len(removedIdx2) == 0) or (len(removedIdx) == 0 and len(removedIdx2) != 0)
    rule2 = len(baselineALLset) == len(baselineSPLITset)
    rule3 = len(removedIdx3) == 0 and len(removedIdx4) == 0

    if (rule1 ^ rule2) and rule3:
        print("✅")
    else:
        print("❌")
