import os
import time
import pandas as pd

def load_parquet(file_name):
    try:
        df = pd.read_parquet(file_name)
    except:
        return pd.DataFrame()
    return df

def df2csv(df, file_name):
    df.to_csv("./csv/yellow/" + file_name + ".csv", header=False, index=False)

def getbasename(file_name):
    return os.path.splitext(os.path.basename(file_name))[0]

def parquet2csv(file_name):
    df2csv(load_parquet(file_name), getbasename(file_name))

if __name__ == "__main__":
    for year in range(2018, 2024):
        if year == 2023:
            mx = 6
        else:
            mx = 12

        for i in range(1, mx + 1):
            file_name = "./parquet/yellow/yellow_tripdata_{}-{}.parquet".format(year, str(i).zfill(2))

            print("***START: {}***".format(file_name))

            stime1 = time.time()
            df = load_parquet(file_name)
            if df.empty == True:
                continue
            etime1 = time.time()

            stime2 = time.time()
            basename = getbasename(file_name)
            etime2 = time.time()

            stime3 = time.time()
            df2csv(df, basename)
            etime3 = time.time()

            print("time interval 1: {}[s]".format(etime1 - stime1))
            print("time interval 2: {}[s]".format(etime2 - stime2))
            print("time interval 3: {}[s]".format(etime3 - stime3))

            print("***END: {}***".format(file_name))
    # parquet2csv(file_name)