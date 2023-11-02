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

def parseLineageData(jdata):
    return str(sorted(jdata["LINEAGE"], key=lambda x: json.dumps(x)))

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
    genealogLineageSet = set()
    print("*** CREATE BASELINE OUTPUT SET ***")
    for line in tqdm(genealogALLlist):
        if parseFlag == 1:
            genealogALLset.add(parseNYCOutputAndRoundAvg(json.loads(line)["OUT"]))
            genealogLineageSet.add(parseLineageData(json.loads(line)))
        elif parseFlag == 2:
            genealogALLset.add(json.loads(line.replace("\\", ""))["OUT"])
            genealogLineageSet.add(parseLineageData(json.loads(line.replace("\\", ""))))
        else:
            genealogALLset.add(json.loads(line)["OUT"])
            genealogLineageSet.add(parseLineageData(json.loads(line)))

    # Create l3stream output set
    errorList = []
    l3streamALLlist = getOutputList(l3streampath)
    l3streamLineageSet = set()
    print("*** CREATE BASELINE OUTPUT SET ***")
    for line in tqdm(l3streamALLlist):
        l3streamLineageSet.add(line)
        if parseFlag == 1:
            jdata = json.loads(line)
        elif parseFlag == 2:
            jdata = json.loads(line.replace("\\", ""))
        else:
            jdata = json.loads(line)

        if jdata["FLAG"] == "true" and parseLineageData(json.loads(line)) not in genealogLineageSet:
            errorList.append(line)

    baseline2genealog = baselineALLset & genealogALLset
    #genealog2l3stream = genealogALLset & l3streamALLset
    #baseline2l3stream = baselineALLset & l3streamALLset
    #genealog2l3stream = genealogLineageSet & l3streamLineageSet

    print("RESULT:::")
    print("  [ baselineALLset & genealogALLset ] len(baseline2genealog) = {},".format(len(baseline2genealog)))
    #print("  [ genealogALLset & l3streamALLset ] len(genealog2l3stream) = {},".format(len(genealog2l3stream)))
    #print("  [ baselineALLset & l3streamALLset ] len(baseline2l3stream) = {},".format(len(baseline2l3stream)))
    #print("  [ genealogLineageSet & l3streamLineageset ] len(genealog2l3stream) = {},".format(len(genealog2l3stream)))
    print()
    print("VALIABLES:::")
    print("  len(baselineALLlist) = {},".format(len(baselineALLlist)))
    print("  len(baselineALLset) = {},".format(len(baselineALLset)))
    print("  len(genealogALLlist) = {},".format(len(genealogALLlist)))
    print("  len(genealogALLset) = {},".format(len(genealogALLset)))
    print("  len(genealogLineageSet) = {},".format(len(genealogLineageSet)))
    #print("  len(l3streamALLlist) = {},".format(len(l3streamALLlist)))
    #print("  len(l3streamALLset) = {},".format(len(l3streamALLset)))
    print("  len(l3streamLineageSet) = {},".format(len(l3streamLineageSet)))
    print("  len(errorList) = {}".format(len(errorList)))

    rule1 = len(baselineALLset) == len(genealogALLset)
    #rule2 = len(genealogALLset) == len(l3streamALLset)
    #rule3 = len(baselineALLset) == len(l3streamALLset)
    #rule4 = len(genealogLineageSet) == len(l3streamLineageSet)
    #if (rule1 and rule2 and rule3 and len(baseline2genealog) == len(baselineALLset) and len(genealog2l3stream) == len(baselineALLset)  and len(baseline2l3stream) == len(baselineALLset)):
    #if (rule1 and len(baseline2genealog) == len(baselineALLset) and rule4):
    if (rule1 and len(baseline2genealog) == len(baselineALLset) and len(errorList) == 0):
        print("✅")
    else:
        print("❌")
