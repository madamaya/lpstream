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
    baselineALLpath = sys.argv[1]
    genealogAllpath = sys.argv[2]
    l3streampath = sys.argv[3]
    parseFlag = int(sys.argv[4])

    # Create baseline output set
    baselineALLset = set()
    baselineALLlist = getOutputList(baselineALLpath)
    print("*** CREATE BASELINE OUTPUT SET ***")
    for line in tqdm(baselineALLlist):
        if parseFlag == 1:
            baselineALLset.add(parseNYCOutputAndRoundAvg(json.loads(line)["OUT"]))
        elif parseFlag == 2:
            baselineALLset.add(json.loads(line.replace("\\", ""))["OUT"])
        else:
            baselineALLset.add(json.loads(line)["OUT"])

    # Create genealog output set
    genealogALLset = set()
    genealogALLlist = getOutputList(genealogAllpath)
    print("*** CREATE BASELINE OUTPUT SET ***")
    for line in tqdm(genealogALLlist):
        if parseFlag == 1:
            genealogALLset.add(parseNYCOutputAndRoundAvg(json.loads(line)["OUT"]))
        elif parseFlag == 2:
            genealogALLset.add(json.loads(line.replace("\\", ""))["OUT"])
        else:
            genealogALLset.add(json.loads(line)["OUT"])

    # Create l3stream output set
    l3streamALLset = set()
    l3streamALLlist = getOutputList(l3streampath)
    print("*** CREATE BASELINE OUTPUT SET ***")
    for line in tqdm(l3streamALLlist):
        if parseFlag == 1:
            l3streamALLset.add(parseNYCOutputAndRoundAvg(json.loads(line)["OUT"]))
        elif parseFlag == 2:
            l3streamALLset.add(json.loads(line.replace("\\", ""))["OUT"])
        else:
            l3streamALLset.add(json.loads(line)["OUT"])

    baseline2genealog = baselineALLset & genealogALLset
    genealog2l3stream = genealogALLset & l3streamALLset
    baseline2l3stream = baselineALLset & l3streamALLset

    print("RESULT:::")
    print("  [ baselineALLset & genealogALLset ] len(baseline2genealog) = {},".format(len(baseline2genealog)))
    print("  [ genealogALLset & l3streamALLset ] len(genealog2l3stream) = {},".format(len(genealog2l3stream)))
    print("  [ baselineALLset & l3streamALLset ] len(baseline2l3stream) = {},".format(len(baseline2l3stream)))
    print()
    print("VALIABLES:::")
    print("  len(baselineALLlist) = {},".format(len(baselineALLlist)))
    print("  len(baselineALLset) = {},".format(len(baselineALLset)))
    print("  len(genealogALLlist) = {},".format(len(genealogALLlist)))
    print("  len(genealogALLset) = {},".format(len(genealogALLset)))
    print("  len(l3streamALLlist) = {},".format(len(l3streamALLlist)))
    print("  len(l3streamALLset) = {},".format(len(l3streamALLset)))

    if (baseline2genealog == 0 and genealog2l3stream == 0 and baseline2l3stream == 0):
        print("✅")
    else:
        print("❌")
