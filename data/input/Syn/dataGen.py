import sys
import string
import random
import time
import numpy as np

def temp_data_gen(machine_id: int, sensor_id: int, temperature: float, log: str) -> str:
    temp_str = format(temperature, ".1f")
    return "0,{},{},{},{}\n".format(machine_id, sensor_id, temp_str, log)

def power_data_gen(machine_id: int, power_usage: float, log: str) -> str:
    power_str = format(power_usage, ".1f")
    return "1,{},{},{}\n".format(machine_id, power_str, log)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise Exception

    cases = [
        [10, 0.1],
        #[50, 0.3],
        [100, 0.4],
        [400, 1.4]
    ]
    for case in cases:
        # Parameters
        #sensor_num = 500 2024/03/28
        sensor_num = 99
        machine_num = 10
        temp_range = [100, 200]
        power_range = [1000, 2000]
        log_size_0 = case[0] # *8 bytes
        log_size_1 = case[0] # *8 bytes
        minimum_generated_data_size = case[1] # Gbytes
        parallelism = int(sys.argv[1])

        # Preprocessing
        rng = np.random.default_rng(137)
        ## Define a mean temp value for each machine
        temp_mean_dist = {key: rng.uniform(temp_range[0], temp_range[1]) for key in range(machine_num * parallelism)}
        ## Define a mean power value for each machine
        power_mean_dist = {key: rng.uniform(power_range[0], power_range[1]) for key in range(machine_num * parallelism)}
        ## Generate a specified-bytes log string for type0
        #log0 = "t" * log_size_0
        ## Generate a specified-bytes log string for type0
        #log1 = "p" * log_size_1
        candidate_chars = string.ascii_letters + string.digits

        # Generate data
        all_start_time = time.time()
        for current_parallel_id in range(parallelism):
            all_size = 0
            with open("../data/syn1.{}.csv.ingest.{}".format(log_size_0, current_parallel_id), "w") as w:
            #with open("syn.csv.{}".format(current_parallel_id), "w") as w:
                ## Start Time
                start_time = time.time()

                ## processing
                while all_size < minimum_generated_data_size * 1e9:
                    for current_machine_id in range(machine_num * current_parallel_id, machine_num * (current_parallel_id + 1)):
                        power = rng.normal(power_mean_dist[current_machine_id], power_mean_dist[current_parallel_id]/3)
                        log1 = "".join(random.choices(candidate_chars, k=log_size_1))
                        dataLine = power_data_gen(current_machine_id, power, log1)
                        all_size += len(dataLine)
                        w.write(dataLine)
                    for current_sensor_id in range(sensor_num * current_parallel_id, sensor_num * (current_parallel_id + 1)):
                        for current_machine_id in range(machine_num * current_parallel_id, machine_num * (current_parallel_id + 1)):
                            temp = rng.normal(temp_mean_dist[current_machine_id], temp_mean_dist[current_parallel_id]/3)
                            log0 = "".join(random.choices(candidate_chars, k=log_size_0))
                            dataLine = temp_data_gen(current_machine_id, current_sensor_id, temp, log0)
                            all_size += len(dataLine)
                            w.write(dataLine)
                            #print("\r{}% ( current_machine_id = {} (range: {}-{}), current_sensor_id = {} (range: {}-{}) )".format(count * 100 / (machine_num * sensor_num),current_machine_id, machine_num * current_parallel_id, machine_num * (current_parallel_id + 1) - 1, current_sensor_id, sensor_num * current_parallel_id, sensor_num * (current_parallel_id + 1) - 1), end="")
                            print("\rcurrent_parallel_id: {}, progress: {}%".format(current_parallel_id, all_size * 100 / (minimum_generated_data_size * 1e9)), end="")

                ## End Time
                end_time = time.time()
                print("\n*****************************")
                print("\tThis loop: {} [s]".format(end_time - start_time))
                print("\tDuration from starting: {} [s]".format(end_time - all_start_time))
                print("\tCurrent dataSize: {} [GB]".format(all_size / 1e9))
                print("\tProgress: {} / {} ({}%)".format(all_size / 1e9, minimum_generated_data_size, (all_size * 100 / 1e9) / minimum_generated_data_size))
                print("*****************************")

        print("Complete!✅ (Case = {})".format(case))
    print("Complete!✅ (All)")








