import sys

if __name__ == "__main__":
    log_set = set()
    with open("../data/lr.csv") as f:
        for line in f:
            log = line.replace("\n", "").split(",")[15]
            if log in log_set:
                print("LR: ❌ (LOG_DUPLICATE_ERROR)")
                print(line)
                exit(1)
            log_set.add(log)
    print("LR: ✅")