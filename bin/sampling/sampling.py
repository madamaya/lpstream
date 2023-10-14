import os, sys
import random

assert len(sys.argv) == 3

filePath = sys.argv[1]
numOfSamples = int(sys.argv[2])

# count lines
count = 0
with open(filePath) as f:
    while True:
        line = f.readline()
        if line == "":
            break
        count = count + 1
print("count = {}".format(count))

# random sampling (seed = 137)
random.seed(137)
s_samples = set()
while len(s_samples) < numOfSamples:
    s_samples.add(random.randint(1, count))
print("s_samples = {}".format(s_samples))

# write down sampled lines at a file
idx = 0
writeFilePath = filePath + ".sampled.txt"
with open(filePath) as f:
    with open(writeFilePath, "w") as w:
        while True:
            line = f.readline()
            if line == "":
                break

            idx = idx + 1

            if idx in s_samples:
                w.write(line)

