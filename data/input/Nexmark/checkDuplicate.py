import sys
import json
import collections

if __name__ == "__main__":
    assert len(sys.argv) == 3
    parallelism = int(sys.argv[1])
    N = int(sys.argv[2])

    fList = []
    for idx in range(parallelism):
        fList.append(open("../data/nexmark.json.ingest.{}".format(idx)))

    result_flag = True
    count = 0
    active = True
    end_idx = set()
    while active:
        current_list = []
        for idx in range(parallelism):
            if idx in end_idx:
                continue
            current_count = 0
            while True:
                line = fList[idx].readline()
                if line == "":
                    end_idx.add(idx)
                    fList[idx].close()
                    if len(end_idx) == parallelism:
                        active = False
                    break
                jdata = json.loads(line)
                current_count += 1

                if jdata["auction"] != None:
                    auction = jdata["auction"]
                    data = str(auction["id"]) + ":::::" + auction["itemName"] + ":::::" + auction["description"] + ":::::" + str(auction["initialBid"]) + ":::::" + str(auction["reserve"]) + ":::::" + str(auction["seller"]) + ":::::" + str(auction["category"]) + ":::::" + auction["extra"]
                    current_list.append(data)
                elif jdata["bid"] != None:
                    bid = jdata["bid"]
                    data = str(bid["auction"]) + ":::::" + str(bid["bidder"]) + ":::::" + str(bid["price"]) + ":::::" + bid["channel"] + ":::::" + bid["url"] + ":::::" + bid["extra"]
                    current_list.append(data)
                else:
                    pass

                if current_count >= N:
                    count += current_count
                    print("\rcount = {}".format(count), end="")
                    break

        if len(current_list) == len(set(current_list)): # OK
            current_list.clear()
            pass
        else: # NG
            r = collections.Counter(current_list)
            print(list(filter(lambda x: x[1] > 1, r.items())))
            result_flag = False
            active = False

    if result_flag == True:
        print("Nexmark: ✅")
    else:
        print("Nexmark: ❌")