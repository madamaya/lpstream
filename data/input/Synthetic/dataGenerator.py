import sys
import json

def returnTemplateTuple(lenFlag):
    with open("tempTuples.json") as f:
        small = json.loads(f.readline())
        large = json.loads(f.readline())
    if lenFlag == 0:
        return small
    elif lenFlag == 1:
        return large
    else:
        assert 1 == 0

assert len(sys.argv) == 3

keyRange = int(sys.argv[1])
dataSize = int(sys.argv[2])

with open("AMAZON_FASHION_5.json") as f:
    lines = f.readlines()

j_data = [json.loads(line) for line in lines if "asin" in json.loads(line) and "overall" in json.loads(line)]

currentKey = 0
count = 0
with open("synDataset_key{}_size{}.json".format(str(keyRange), str(dataSize)), "w") as f:
    while count < dataSize:
        cj_data = j_data[count % len(j_data)]
        cj_data["asin"] = str(currentKey).zfill(8)
        currentKey = (currentKey + 1) % keyRange

        f.write(json.dumps(cj_data) + "\n")
        count = count + 1

        if count % 100000 == 0:
            print(count)