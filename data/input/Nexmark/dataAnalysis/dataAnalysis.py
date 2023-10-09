import json
import datetime

#testDataPath = "./testNex.json"
testDataPath = "../nexmark.json"

# {"event_type":1,"person":null,"auction":{"id":1001,"itemName":"pc","description":"gbyf","initialBid":2940,"reserve":4519,"dateTime":"2023-10-03 05:31:34.28","expires":"2023-10-03 05:31:34.292","seller":1010,"category":13,"extra":"oafyjc\\OM_S`ytfqwfUYSHI]UY]MXLraslfahludyeK[RQLZ[SY]YZ`V_YOONNQSPJ[NWZ]O_O^V\\`qznfrchvnxrvVMWWL\\pwbvqzlkudfsR`IPN_ildsskpjyiioLaLWO[MLKXYKrwtogg[WXQX`jxtgwhckhuzvvhmstrkpjtqtzefrzunbbyctO`OVMHSL[_XOXW`U`ZwxtbydgnwwtkTRNY_Yraydesozvsnq^MTMNLklhogoopcllnovmhltN^Y[VQtckyviwkcrec][ZSHasfbfvjiclgxkRSW_HIgutwbhrycicj_XLWTIsxsvlbkvratmqelcvwykztkcLRYa[Ldxbcvf[STV^SNX[PX^ZINWLScijpfbvzpptwH\\MRI\\lvizarcmgzjjX\\UIONnjjmsfP]M[SIzjcxre]\\S`VJhunyuhjlw"},"bid":null}

def makeRangeMap():
    mp = {}
    with open(testDataPath) as f:
        while True:
            line = f.readline()
            if line == "":
                break
            jdata = json.loads(line)
            if jdata["auction"] != None:
                aid = jdata["auction"]["id"]
                sts = jdata["auction"]["dateTime"]
                ets = jdata["auction"]["expires"]

                mp[aid] = [int(conv2UnixTime(sts)), int(conv2UnixTime(ets))]

    return mp

def checkBidRange():
    mp = makeRangeMap()
    bmp = {}
    flag = True
    count = 0
    with open(testDataPath) as f:
        while True:
            line = f.readline()
            if line == "":
                break
            jdata = json.loads(line)
            if jdata["bid"] != None:
                aid = jdata["bid"]["auction"]
                ts = int(conv2UnixTime(jdata["bid"]["dateTime"]))

                if aid == "1000" or aid == 1000:
                    continue
                if aid not in mp:
                    print("aid={} does not exist.".format(aid))
                    continue

                # ts is out of the corresponding window.
                if ts < mp[aid][0] or mp[aid][1] < ts:
                    #print("aid={}, ts={} is out of the corresponding window [{}, {}]".format(aid, ts, mp[aid][0], mp[aid][1]))
                    #flag = False
                    #count = count + 1
                    if aid in bmp:
                        bmp[aid][1] += 1
                    else:
                        bmp[aid] = [0, 1]
                else:
                    if aid in bmp:
                        bmp[aid][0] += 1
                    else:
                        bmp[aid] = [1, 0]

    return bmp

def calcOutElements(bmp):
    ret = {-1:0, 0:0, 1:0, 2:0, 3:0, 4:0, 5:0, 6:0, 7:0, 8:0, 9:0, 10:0}
    for k, v in bmp.items():
        outPercent = v[0] / (v[1] + v[0])
        index = int(outPercent * 100) // 10
        if index not in ret:
            print(k, v)
            continue
        ret[index] += 1
    return ret

def checkTsOrder():
    prevTs = -1
    flag = True
    with open(testDataPath) as f:
        while True:
            line = f.readline()
            if line == "":
                break
            jdata = json.loads(line)
            # bid
            if jdata["bid"] != None:
                ts = int(conv2UnixTime(jdata["bid"]["dateTime"]))
                if prevTs > ts:
                    flag = False
                prevTs = ts
            elif jdata["auction"] != None:
                ts = int(conv2UnixTime(jdata["auction"]["dateTime"]))
                if prevTs > ts:
                    flag = False
                prevTs = ts
    return flag

def auctionDurationRangeAvg():
    sum = 0
    num = 0
    with open(testDataPath) as f:
        with open("auctionDurationRangeAvg.txt", "w") as w:
            while True:
                line = f.readline()
                if line == "":
                    break

                jdata = json.loads(line)
                if jdata["auction"] != None:
                    aid = jdata["auction"]["id"]
                    sts = jdata["auction"]["dateTime"]
                    ets = jdata["auction"]["expires"]

                    range = conv2UnixTime(ets) - conv2UnixTime(sts)
                    sum = sum + range
                    num = num + 1

                    w.write("{},{},{},{}\n".format(aid, sts, ets, range))

    return sum, num, sum / num

def checkDuplicationAuctionId():
    s = set()
    flag = True
    with open(testDataPath) as f:
        while True:
            line = f.readline()
            if line == "":
                break

            jdata = json.loads(line)
            if jdata["auction"] != None:
                aid = jdata["auction"]["id"]
                if aid in s:
                    print("aid={} is duplicated.".format(aid))
                    flag = False
                s.add(aid)

    return flag

def conv2UnixTime(line):
    if "." in line:
        return datetime.datetime.strptime(line, "%Y-%m-%d %H:%M:%S.%f").timestamp() * 1000
    else:
        return datetime.datetime.strptime(line, "%Y-%m-%d %H:%M:%S").timestamp() * 1000

if __name__ == "__main__":
    print("auctionDurationRangeAvg()")
    print(auctionDurationRangeAvg())

    print("checkDuplicationAuctionId()")
    print(checkDuplicationAuctionId())

    print("bmp = checkBidRange()")
    bmp = checkBidRange()

    print("calcOutElements(bmp)")
    print(calcOutElements(bmp))

    print("checkTsOrder()")
    print(checkTsOrder())