# import sys
# import pandas as pd
# import os
# file_name = sys.argv[1]
# data_path = "/data/download/"
# status_path = "/data/status/"
# df = pd.read_csv(data_path+ file_name +".csv")
# a = df[["id", "type"]]
# arr = []
# for i in a.index:
#   t = a.loc[i].to_dict()
#   if t["type"] == "MATRIX":
#     arr.append({"code":t["id"]})
#   if t["type"] == "VECTOR":
#     arr.append({"code":"vec_count("+ t["id"] +")"})
#     arr.append({"code":"vec_avg("+ t["id"] +")"})
#     arr.append({"code":"vec_sum("+ t["id"] +")"})
# if not arr:
#   exit(0)
# os.makedirs(status_path, exist_ok=True)
# pd.DataFrame(arr).to_csv(status_path + "data.csv")
