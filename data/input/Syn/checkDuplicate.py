import sys

if __name__ == "__main__":
    assert len(sys.argv) == 3
    parallelism = int(sys.argv[1])
    size = int(sys.argv[2])

    log_set = set()
    for idx in range(parallelism):
        with open("../data/syn1.{}.csv.ingest.{}".format(size, idx)) as f:
            for line in f:
                elements = line.replace("\n", "").split(",")
                if len(elements) == 4:
                    log = line.replace("\n", "").split(",")[3]
                elif len(elements) == 5:
                    log = line.replace("\n", "").split(",")[4]
                else:
                    raise Exception
                if log in log_set:
                    print("Syn (idx={}, size={}): ❌ (LOG_DUPLICATE_ERROR)".format(idx, size))
                    exit(1)
                log_set.add(log)
    print("Syn (size={}): ✅".format(size))