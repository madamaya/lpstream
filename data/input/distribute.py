import sys
import random

if __name__ == "__main__":
    assert len(sys.argv) == 3

    file = sys.argv[1]
    parallelism = int(sys.argv[2])
    random.seed(137)

    with open(file) as f:
        wList = []
        for i in range(parallelism):
            wList.append(open(file + ".ingest." + str(i), "w"))

        count = 0
        while True:
            line = f.readline()
            if line == "":
                break
            count = count + 1

            #wList[count % parallelism].write(line)
            wList[random.randint(0, parallelism-1)].write(line)

            if count % 10000 == 0:
                print("\rcount = {}".format(count), end="")

    for i in range(parallelism):
        wList[i].close()

    print(" [END]")
