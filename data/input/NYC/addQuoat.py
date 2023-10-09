with open("_yellow.csv") as f:
    with open("yellow.csv", "w") as w:
        cnt = 0
        while True:
            line = f.readline()
            cnt = cnt + 1
            if line == "":
                break
            w.write("\"" + line.rstrip("\n") + "\"" + "\n")

            if cnt % 100000 == 0:
                print(cnt)