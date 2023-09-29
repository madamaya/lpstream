import sys
import json

assert len(sys.argv) == 3

arguments = sys.argv

basefilepath = arguments[1]
compfilepath = arguments[2]
outfile = "./output.txt"

print(basefilepath)
print(compfilepath)
dist1 = {}
dist2 = {}

with open(compfilepath) as f2:
    while True:
        line = f2.readline()
        if line == "":
            break
        # print(line)
        output = json.loads(line)["OUT"]
        if output in dist2:
            # print("ERROR: f2: Duplicated output")
            # print("ERROR: {}".format(output))
            print("Duplicated: f2: {}".format(output))
            # exit(-1)
        else:
            dist2[output] = 1

with open(basefilepath) as f1:
    with open(outfile, 'w') as w:
        while True:
            line = f1.readline()
            if line == "":
                break
            output = json.loads(line)["OUT"]
            if output in dist1:
                # print("ERROR: f1: Duplicated output")
                # print("ERROR: {}".format(output))
                print("Duplicated: f1: {}".format(output))
                # exit(-1)
            else:
                dist1[output] = 1
                if output in dist2:
                    w.write("✅ " + line)
                else:
                    w.write(line)

f = True
for k in dist2.keys():
    if k not in dist1:
        print("{} is not found.".format(k))
        f = False

if f:
    print("✅")
else:
    print("❌")