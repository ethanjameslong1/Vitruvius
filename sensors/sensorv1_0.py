import pandas as pd
import time
import requests

file = "../data/AoT_Chicago.complete.2022-08-31/data.csv.gz"


def stream_data(data):
    requests.post("http://localhost:8000/data", json=data)


chunkIter = pd.read_csv(file, compression="gzip", chunksize=1000)
t_last = None
data = []
i = 0
for chunk in chunkIter:
    for index, row in chunk.iterrows():
        clean_row = row.where(pd.notnull(row), None).to_dict()
        current_time = pd.to_datetime(clean_row["timestamp"])
        if t_last == None:
            t_last = current_time
            data.append(clean_row)
            continue

        if i > 2000:
            break
        else:
            i += 1

        if t_last == current_time:
            data.append(clean_row)
            continue

        elif t_last < current_time:
            tdelta = current_time - t_last
            wait_time = tdelta.total_seconds()

            send_time = time.time()
            stream_data(data)
            lostTime = time.time() - send_time
            wait_time -= lostTime

            data = []
            data.append(clean_row)
            print(f"SLEEPING NOW: {wait_time}")
            if wait_time > 0:
                time.sleep(wait_time)
        else:
            print("Smaller time??? Bad news")
            data.append(clean_row)
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
