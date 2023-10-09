import sys

assert len(sys.argv) == 2

path = sys.argv[1]

with open(path) as f:
    with open(path + ".log", "w") as w:
        while True:
            line = f.readline()
            if line == "":
                break
            w.write(line.replace("\\", ""))