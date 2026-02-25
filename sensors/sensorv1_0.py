import time
import threading
import pandas as pd
import requests


# **************************************************************8
# df_sample = pd.read_csv(file, compression="gzip", nrows=1)
# print(df_sample.info())
# print(df_sample.dtypes)
# print(df_sample.columns.tolist())
# print(df_sample)
# **************************************************************8

TARGET_HUM_NODE = "001e0610ba13"
TARGET_PRE_NODE = "001e0610eef4"


# **************************************************************8
def stream_data(data):
    requests.post("http://localhost:8000/data", json=data)


def read_from_file(file, target_node):
    chunkIter = pd.read_csv(file, compression="gzip", chunksize=1000)
    t_last = None
    data = []
    i = 0
    for chunk in chunkIter:
        i += 1
        if i % 3 != 0:
            continue
        filtered_chunk = chunk[chunk["node_id"] == target_node]
        for _, row in filtered_chunk.iterrows():
            clean_row = row.where(pd.notnull(row), None).to_dict()
            current_time = pd.to_datetime(clean_row["data.csv"])
            c_time = current_time.strftime("%Y-%m-%d %H:%M:%S")
            clean_row["timestamp"] = c_time

            if t_last == None:
                t_last = current_time
                data.append(clean_row)
                continue

            # if t_last == current_time: ** THIS IS FOR USING IT AS A LIVE SENSOR
            if len(data) <= 50:
                data.append(clean_row)
                continue

            # elif t_last < current_time: ** THIS IS FOR USING IT AS A LIVE SENSOR

            elif len(data) > 50:
                tdelta = current_time - t_last
                wait_time = tdelta.total_seconds()

                send_time = time.time()
                stream_data(data)
                lostTime = time.time() - send_time
                wait_time -= lostTime

                data = []
                data.append(clean_row)
                # print(f"SLEEPING NOW: {wait_time}") ** THIS IS FOR USING IT AS A LIVE SENSOR
                # if wait_time > 0: ** THIS IS FOR USING IT AS A LIVE SENSOR
                # time.sleep(wait_time) ** THIS IS FOR USING IT AS A LIVE SENSOR

            else:
                print("Smaller time??? Bad news")
                data.append(clean_row)
            t_last = current_time


# **************************************************************8


fileHum = "../data/AoT_Chicago.complete.humidity/data.csv.gz"
filePres = "../data/AoT_Chicago.complete.pressure/data.csv.gz"

t1 = threading.Thread(target=read_from_file, args=(fileHum, TARGET_HUM_NODE))
t2 = threading.Thread(target=read_from_file, args=(filePres, TARGET_PRE_NODE))

t1.start()
t2.start()


t1.join()
t2.join()

# RangeIndex: 1 entries, 0 to 0
# Data columns (total 7 columns):
#  #   Column     Non-Null Count  Dtype
# ---  ------     --------------  -----
#  0   data.csv   1 non-null      str
#  1   node_id    1 non-null      str
#  2   subsystem  1 non-null      str
#  3   sensor     1 non-null      str
#  4   parameter  1 non-null      str
#  5   value_raw  0 non-null      float64
#  6   value_hrf  1 non-null      float64
# dtypes: float64(2), str(5)
# memory usage: 188.0 bytes
# None
# data.csv         str
# node_id          str
# subsystem        str
# sensor           str
# parameter        str
# value_raw    float64
# value_hrf    float64
# dtype: object
# ['data.csv', 'node_id', 'subsystem', 'sensor', 'parameter', 'value_raw', 'value_hrf']
#               data.csv       node_id subsystem  sensor parameter  value_raw  value_hrf
# 0  2018/01/01 00:00:06  001e0610e532  metsense  htu21d  humidity        NaN      45.09
