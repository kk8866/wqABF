#!/usr/bin/python

import os
import sys
import api
import pandas as pd


def save_data(df: pd.DataFrame, file_name: str):
    data_path = "/data/download/"
    status_path = f"/data/status/{file_name}/"
    # df = pd.read_csv(data_path+ file_name +".csv")
#   a = df[["id", "type"]]
#   left = input("左边输入：")
    left = ""
#   right = input("右边输入")
    right = ""
    # ts_decay_exp_window(-inst_pnl(", ") ,15, factor=0.35)"
    left, right = "", ""
    a = df
    arr = []

    for i in a.index:
        t = a.loc[i].to_dict()
        if "fast_d1" in t["id"]:
            continue
        if t["type"] == "MATRIX":
            arr.append({"code": left+t["id"] + right})
           # arr.append({"code" : "sign(zscore("+t["id"] + "))"})

        elif t["type"] == "VECTOR":
            # continue
            for vec in ["vec_avg", "vec_stddev"]:
                # "vec_max"]: #["vec_count", "vec_avg",

                arr.append({"code": left + f"{vec}(" + t["id"] + ")" + right})

    if not arr:
        exit(0)
    os.makedirs(status_path, exist_ok=True)
    pd.DataFrame(arr).to_csv(status_path + "data.csv")


def get_datafields(instrument_type: str = 'EQUITY', region: str = 'GLB', delay: int = 1,
                   universe: str = 'TOP3000', dataset_id: str = 'pv96', search: str = ''):
    qua = api.quant()
    qua.login()
    if len(search) == 0:
        url_template = "https://api.worldquantbrain.com/data-fields?" +\
            f"&instrumentType={instrument_type}" +\
            f"&region={region}&delay={str(delay)}&universe={universe}&dataset.id={dataset_id}&limit=50" +\
            "&offset={x}"
        count = qua.sess.get(url_template.format(x=0)).json()['count']

    else:
        url_template = "https://api.worldquantbrain.com/data-fields?" +\
            f"&instrumentType={instrument_type}" +\
            f"&region={region}&delay={str(delay)}&universe={universe}&limit=50" +\
            f"&search={search}" +\
            "&offset={x}"
        count = qua.sess.get(url_template.format(x=2)).json()['count']

    datafields_list = []
    for x in range(0, count, 50):
        print(url_template.format(x=x))
        for a in range(5):
            try:
                datafields = qua.sess.get(url_template.format(x=x))
        # resul = datafields.json()
        # print(resul)i
                datafields_list.append(datafields.json()['results'])
                break
            except:
                pass
    qua.sess.close()
    datafields_list_flat = [
        item for sublist in datafields_list for item in sublist]
    print(len(datafields_list_flat))
    if not datafields_list_flat:
        return None
    print(datafields_list_flat[0])
    datafields_df = pd.DataFrame(datafields_list_flat)
    datafields_df.sort_values(by=["alphaCount"], ascending=False, inplace=True)
    file_name = f"{region}-{delay}-{universe}-{dataset_id}-{name}"
    datafields_df.to_csv("/tmp/"+file_name+".csv")
    save_data(datafields_df, file_name)
    print(file_name)


if __name__ == "__main__":
    name = sys.argv[-1]
    get_datafields(
        region=sys.argv[1],
        universe=sys.argv[2],
        dataset_id=sys.argv[3],
        search="",
        delay=1
    )
