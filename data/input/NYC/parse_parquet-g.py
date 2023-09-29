import os
import time
import pandas as pd

def load_parquet(file_name):
    return pd.read_parquet(file_name)

def df2csv(df, file_name):
    df.to_csv("./csv/green/" + file_name + ".csv", header=False, index=False)

def getbasename(file_name):
    return os.path.splitext(os.path.basename(file_name))[0]

def parquet2csv(file_name):
    df2csv(load_parquet(file_name), getbasename(file_name))

if __name__ == "__main__":
    for i in range(1, 7):
        file_name = "./parquet/green/green_tripdata_2023-0{}.parquet".format(i)

        print("***START: {}***".format(file_name))

        stime1 = time.time()
        df = load_parquet(file_name)
        etime1 = time.time()

        stime15 = time.time()
        df_s = df.sort_values('lpep_dropoff_datetime')
        etime15 = time.time()

        stime2 = time.time()
        basename = getbasename(file_name)
        etime2 = time.time()

        stime3 = time.time()
        df2csv(df_s, basename)
        etime3 = time.time()

        print("time interval 1: {}[s]".format(etime1 - stime1))
        print("time interval 15: {}[s]".format(etime15 - stime15))
        print("time interval 2: {}[s]".format(etime2 - stime2))
        print("time interval 3: {}[s]".format(etime3 - stime3))

        print("***END: {}***".format(file_name))
    # parquet2csv(file_name)