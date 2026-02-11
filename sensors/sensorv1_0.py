from os import wait
import pandas as pd
import time
import gzip

file = "../data/AoT_Chicago.complete.2022-08-31/data.csv.gz"


def stream_data(data):
    print(data)
    print("\n\n")


chunkIter = pd.read_csv(file, compression="gzip", chunksize=1)
t_last = None
data = []
for i, chunk in enumerate(chunkIter):
    current_time = pd.to_datetime(chunk["timestamp"].iloc[0])
    if t_last == None:
        t_last = current_time
        data.append(chunk.iloc[0].to_dict())
        continue

    if i > 500:
        break

    if t_last == current_time:
        data.append(chunk.iloc[0].to_dict())
        continue

    elif t_last < current_time:
        tdelta = current_time - t_last
        wait_time = tdelta.total_seconds()

        send_time = time.time()
        stream_data(data)
        lostTime = time.time() - send_time

        data = []
        data.append(chunk.iloc[0].to_dict())
        print(f"SLEEPING NOW: {wait_time}")
        time.sleep(max(0, wait_time - lostTime))
    else:
        print("Smaller time??? Bad news")
    t_last = current_time


"""
Printing 'chunk.iloc[0]'
timestamp    2018/01/01 00:00:06
node_id             001e0610e532
subsystem             lightsense
sensor             apds_9006_020
parameter              intensity
value_raw                      4
value_hrf                  0.322
Name: 19, dtype: object

Printing 'chunk'
             timestamp       node_id   subsystem         sensor  parameter  value_raw  value_hrf
19  2018/01/01 00:00:06  001e0610e532  lightsense  apds_9006_020  intensity          4      0.322


# Sending data

from dotenv import dotenv_values

config = dotenv_values("sensors.env")
print(config["SENSOR_SEND_TO_URL"])


timestamp    2018/01/01 00:00:06                                                                                                                                                                                                                                           │
node_id             001e0610e532                                                                                                                                                                                                                                           │
subsystem              chemsense                                                                                                                                                                                                                                           │
sensor            reducing_gases                                                                                                                                                                                                                                           │
parameter          concentration                                                                                                                                                                                                                                           │
value_raw                   4498                                                                                                                                                                                                                                           │
value_hrf                    NaN                                                                                                                                                                                                                                           │
Name: 12, dtype: object                                                                                                                                                                                                                                                    │
timestamp    2018/01/01 00:00:06                                                                                                                                                                                                                                           │
node_id             001e0610e532                                                                                                                                                                                                                                           │
subsystem              chemsense                                                                                                                                                                                                                                           │
sensor                     sht25                                                                                                                                                                                                                                           │
parameter               humidity                                                                                                                                                                                                                                           │
value_raw                   4993                                                                                                                                                                                                                                           │
value_hrf                  49.93                                                                                                                                                                                                                                           │
Name: 13, dtype: object

"""
