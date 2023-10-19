import sys

if __name__ == "__main__":
    assert len(sys.argv) == 2

    file = sys.argv[0]
    numOfLines = 59149718
    exit(1)
    numOf1File = numOfLines // 2
    numOf2File = numOfLines - numOf1File

    with open(file) as f:
        with open(file + ".1", "w") as w1:
            with open(file + ".2", "w") as w2:
                count = 0
                while True:
                    line = f.readline()
                    if line == "":
                        break
                    count = count + 1
                    if count < numOf1File:
                        w1.write(line)
                    else:
                        w2.write(line)

                    if count % 10000 == 0:
                        print("\rcount = {}".format(count), end="")

    print(" [END]")

