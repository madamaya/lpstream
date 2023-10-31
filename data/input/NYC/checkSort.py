from datetime import datetime

prevTime = datetime.strptime("1990-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
currentTime = datetime.strptime("1990-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")

flag = True
cnt = 0
with open("nyc.csv") as f:
    while True:
        line = f.readline()
        if line == "":
            break

        cnt += 1
        currentTime = datetime.strptime(line.split(",")[2], "%Y-%m-%d %H:%M:%S")

        if currentTime < prevTime:
            flag = False
            break

        if cnt % 100 == 0:
            print("\r{}".format(cnt), end="")

        prevTime = currentTime

if flag:
    print(" [end] ✅")
else:
    print(" [end] ❌")
