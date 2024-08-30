import os, sys

if __name__ == "__main__":
    assert len(sys.argv) == 2
    filePath = sys.argv[1]

    results = dict()
    if os.path.exists(filePath):
        with open(filePath) as f:
            while True:
                line = f.readline()
                if line == "":
                    break

                elements = line.split(",")
                flag = elements[0]
                query = elements[1]
                approach = elements[2]
                size = int(elements[3])
                inputRate = int(elements[4])

                key = query + "," + approach + "," + str(size)
                if key not in results:
                    results[key] = [[0], [9999999999]]

                if flag == "stable":
                    results[key][0].append(inputRate)
                elif flag == "unstable":
                    results[key][1].append(inputRate)
                else:
                    raise Exception
    else:
        queries = ["Syn1", "Syn2", "Syn3", "LR", "Nexmark", "Nexmark2", "NYC", "NYC2", "YSB", "YSB2"]
        approaches = ["baseline", "genealog", "l3stream", "l3streamlin"]
        sizes = [-1, 10, 50, 100]
        for query in queries:
            for approach in approaches:
                for size in sizes:
                    if "Syn" in query:
                        if size == -1:
                            continue
                    else:
                        if size != -1:
                            continue
                    key = query + "," + approach + "," + str(size)
                    if key not in results:
                        results[key] = [[-400000], [2600000]]

    with open("config.csv", "w") as w:
        for key in results.keys():
            start = sorted(results[key][0])[-1]
            end = sorted(results[key][1])[0]
            if start == -400000:
                increment = (end - start) // 6
            elif start == 0:
                increment = (end - start) // 10
            else:
                increment = (end - start) // 5
            w.write("{},{},{},{}\n".format(key, str(start), str(end), str(increment)))
