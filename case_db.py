import json
import pandas as pd
import save_db


def update(sid, data):
    data = ["a" + str(i) + "=" + d for i, d in enumerate(range(data))]
    sql = f'''UPDATE case_temp
   SET  = {','.join(data)}
   WHERE id={sid};  '''
    return save_db.exe_sql(sql)


def insert_case(data: list[dict]):
    for i in data:
        insert_case_db(i)
    pass


class cfg:
    simulations_db_name: str = ""
    case_result_db: str = ""
    columns: list = []


def create_simulations():
    '''创建保存状态sid的表'''
    data = '''id	CHAR(24) PRIMARY KEY	,
    result	BOOL'''
    return save_db.create_table(cfg.simulations_db_name, fields=data)


def insert_simulations_id(sid: str):

    sql = f'''INSERT INTO {cfg.simulations_db_name} (id)
   SELECT  '{sid}' FROM DUAL 
   WHERE NOT EXISTS (
   SELECT id FROM {cfg.simulations_db_name} WHERE id = '{sid}'
   );'''

    return save_db.exe_sql(sql)


def update_simulations_status(sid, result: bool):
    #    data = list(data.items())
    sql = f'''UPDATE {cfg.simulations_db_name}
   SET result = {result}
   WHERE id='{sid}';'''
    return save_db.exe_sql(sql)


def get_simulations_status(status: bool = False):
    sql = f"SELECT id FROM {cfg.simulations_db_name} WHERE result IS NULL;"
    return save_db.query(sql)


def create_case():
    '''创建用于保存结果的表'''
    # 用例开始前创建，用例结束后清空
    with open("alpha_results.json", "r") as f:
        db_cfg: dict = json.load(f)
    # db_name = f"case_all"
    fields = [i[0] + " " + i[1] for i in db_cfg.items()]

    save_db.create_table(cfg.case_result_db, fields=",".join(fields))
    columns = save_db.query(f"SHOW COLUMNS FROM {cfg.case_result_db};")[1]
    cfgs.columns = [i[0] for i in columns]
    return


class cfgs:
    columns: list = []


with open("alpha_results.json", "r") as f:
    db_cfg = json.load(f)
    columns = list(db_cfg.keys())
    # print(columns)


def insert_case_db(data: dict):
    print(data["id"], cfg.case_result_db)
    data = {i: data[i] for i in data if i in cfgs.columns}
    fields = ",".join(list(data.keys()))
    # print(fields)
    values = str(list(data.values()))[1:-1]
    sql = f'''INSERT INTO {cfg.case_result_db} ({fields})
    SELECT  {values} FROM DUAL 
    WHERE NOT EXISTS (
    SELECT id FROM {cfg.case_result_db} WHERE id = '{data["id"]}'
    );'''
    rst = save_db.exe_sql(sql)
    print("insert", data["id"], rst)
    # print
    return rst


def update_case_db(aid: str, data: dict):
    arr = []
    for i in list(data.items()):
        if i[0] not in cfgs.columns:
            continue
        if isinstance(i[1], str):
            arr.append(f'{i[0]}="{i[1]}"')
        else:
            arr.append(f'{i[0]}={i[1]}')


#   data = ",".join([i[0] + "=" + f'"{i[1]}"' for i in list(data.items()) if i[0] in cfgs.columns])
    data = ",".join(arr)
    sql = f'''UPDATE {cfg.case_result_db}
  SET {data}
  WHERE id='{aid}';  '''
    return save_db.exe_sql(sql)


def query_year_sharpe(aid):
    sql = f"SELECT zeroCount FROM {cfg.case_result_db} WHERE zeroCount IS NOT NULL AND id='{aid}'"
    return save_db.query(sql)
