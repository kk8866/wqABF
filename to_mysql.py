# #!/usr/bin/python

# import pymysql
# import save_db
# import threading
# import traceback
# import os, time
# import sys
# import api
# import pandas as pd
# from pydantic import BaseModel
# name = sys.argv[1]


# def save_data(df: pd.DataFrame, file_name: str):
#     data_path = "/data/download/"
#     status_path = f"/data/status/{file_name}/"
#     # df = pd.read_csv(data_path+ file_name +".csv")
#     a = df[["id", "type"]]
#     a = df
#     arr = []
#     for i in a.index:
#         t = a.loc[i].to_dict()
#         if t["type"] == "MATRIX":
#             arr.append({"code": t["id"]})
#         elif t["type"] == "VECTOR":
#             for vec in ["vec_count", "vec_avg", "vec_sum"]:
#                 arr.append({"code": f"{vec}(" + t["id"] + ")"})
#     if not arr:
#         exit(0)
#     os.makedirs(status_path, exist_ok=True)
#     pd.DataFrame(arr).to_csv(status_path + "data.csv")


# db_lock = threading.Lock()
# # import pandas as pd
# # 打开数据库连接
# save_db.cfg.db = pymysql.connect(host='127.0.0.1',
#                                  user='root',
#                                  password='root',
#                                  database='dataset')

# # 使用cursor()方法获取操作游标
# save_db.cfg.cursor = save_db.cfg.db.cursor()


# class dataset_model(BaseModel):
#     id: str = ""
#     category: str = ""
#     name: str = ""
#     description: str = ""
#     fieldCount: int = 0
#     delay: int = 1
#     region: str = ""
#     universe: str = ""


# class daset_columns(BaseModel):
#     id: str = "VARCHAR(25)"
#     category: str = "VARCHAR(20)"
#     name: str = "VARCHAR(200)"
#     description: str = "VARCHAR(5000)"
#     fieldCount: str = "SMALLINT(2)"
#     delay: str = "TINYINT"
#     region: str = "CHAR(3)"
#     universe: str = "VARCHAR(20)"


# a = daset_columns().model_dump()
# # print(a)
# fields = ["ix INT PRIMARY KEY AUTO_INCREMENT"] + [i + " " + a[i] for i in a]
# # print(fields)
# # # print([i + " " + a[i] for i in a])
# save_db.create_table("dataset_id", fields=",".join(fields))


# def insert_db(data: dict):
#     # print(data["id"], cfg.case_result_db)
#     # data = {i: data[i] for i in data if i in data.keys()}
#     fields = ",".join(list(data.keys()))
#     # print(fields)
#     values = str(list(data.values()))[1:-1]
#     sql = f'''INSERT INTO dataset_id ({fields})
#     SELECT  {values} FROM DUAL
#     WHERE NOT EXISTS (
#     SELECT id FROM dataset_id WHERE id = '{data["id"]}'
#     );'''
#     rst = save_db.exe_sql(sql)
#     print("insert", data["id"], rst)
#     # print
#     return rst


# def get_dataset(instrument_type: str = 'EQUITY', region: str = 'GLB', delay: int = 1,
#                 universe: str = 'TOP3000'):
#     qua = api.quant()
#     qua.login()
#     url_template = "https://api.worldquantbrain.com/data-sets?" +\
#         f"&instrumentType={instrument_type}" +\
#         f"&region={region}&delay={str(delay)}&universe={universe}&limit=1&offset=0"
#     res = qua.sess.get(url_template).json()
#     results = res['results']
#     print(res["count"])
#     count = res["count"]
#     arr = []
#     for i in range(0, count, 50):
#         url_template = "https://api.worldquantbrain.com/data-sets?" +\
#         f"&instrumentType={instrument_type}" +\
#         f"&region={region}&delay={str(delay)}&universe={universe}&limit=50&offset={i}"

#         for z in range(5):
#             res = qua.sess.get(url_template).json()
#             if res.get("message") != 'API rate limit exceeded':
#                         break
#             time.sleep(2)
#         results = res['results']

#     # ['results']
#         for result in results:
#             daset = dataset_model(id=result["id"], category=result["category"]["id"], name=result["name"],
#                                 description=result["description"], delay=result["delay"], universe=result["universe"], fieldCount=result["fieldCount"], region=region)
#             insert_db(daset.model_dump())
#             url_template = "https://api.worldquantbrain.com/data-fields?" +\
#             f"&instrumentType={instrument_type}" +\
#             f"&region={region}&delay={str(delay)}&universe={universe}&dataset.id={result['id']}&limit=1" +\
#             "&offset=0"
#             for z in range(5):
#                 data = qua.sess.get(url_template.format(x=0)).json()
#                 print(data)
#                 if data.get("message") != 'API rate limit exceeded':

#                     break
#                 time.sleep(2)

#             data = data["results"][0]
#             if data["type"] == "VECTOR":
#                 arr.append("vec_avg(" + data["id"] +")")
#             else:
#                 arr.append( data["id"])
#         print(arr)
#         pd.DataFrame(arr,columns=["code"]).to_csv(region + "_data.csv")


# if __name__ == "__main__":
#     import sys
#     get_dataset(region=sys.argv[1], universe=sys.argv[2])
