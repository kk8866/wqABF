import pandas as pd
import sys
from dateutil.relativedelta import relativedelta
df = pd.read_csv("/data/check/os_alpha_ids.csv")
df["dateSubmitted"] = df["dateSubmitted"].apply(lambda x: x.split("T")[0])
df["dateSubmitted"] = pd.to_datetime(df["dateSubmitted"])
for i in range(4, 15):
    date1 = pd.Timestamp(f"2025-01-01") + relativedelta(months=i)
    date2 = date1 + relativedelta(months=3)
    df1 = df[(df["dateSubmitted"] >= date1 )& (df["dateSubmitted"] < date2) ]
    print(date1, "--->", date2)
    print(df1["region"].value_counts())
    print(df1["region"].mean())
    if df1.shape[0] == 0:
        break