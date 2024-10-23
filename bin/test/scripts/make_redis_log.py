import sys
import redis

if __name__ == "__main__":
    assert len(sys.argv) == 6

    query = sys.argv[1]
    size = int(sys.argv[2])
    redisIP = sys.argv[3]
    redisPort = sys.argv[4]
    parallelism = int(sys.argv[5])

    r = redis.Redis(host=redisIP, port=int(redisPort))
    idx = 1
    factor = 2 if ("Nexmark" in query or query == "Syn2") else 1
    with open("../redis_log/{}_{}.log".format(query, size), "w") as w:
        while True:
            current_keys = [key.decode() for key in r.scan_iter("{},*".format(idx))]
            if len(current_keys) == parallelism * factor:
                chkts = -1
                for key in current_keys:
                    chkts = max(chkts, int(r.get(key)))
                w.write("{},{}\n".format(idx, chkts))
            elif 0 < len(current_keys) < parallelism * factor:
                continue
            elif len(current_keys) == 0:
                break
            else:
                raise Exception
            idx += 1


