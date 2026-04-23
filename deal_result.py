

import pandas as pd
import save_db
import model
from case_db import cfg


def get_data_from_db():

    sql = f"SHOW COLUMNS FROM {cfg.case_result_db};"
    columns = save_db.query(sql)[1]
    columns = [i[0] for i in columns]
    # WHERE zeroCount<3 用于统计年sharpe为0情况来筛选缺少数据的字段
    sql = f"SELECT * FROM {cfg.case_result_db} WHERE zeroCount<5 OR zeroCount IS NULL;"
    print(sql)
    data = save_db.query(sql)[1]
    return pd.DataFrame(data, columns=columns)


def deal_data(df: pd.DataFrame, sharpe: float = 0.9, fitness=0.3, n: int = 1,
              save_file: str = "", case_df: str = "") -> pd.DataFrame:
    # 变更sharpe和fitness，按照原始表达式分组。按照fitness+sharpe排序取前n。
    df = get_data_from_db()
    print("从数据库获取到的数据：")
    print(df)
    df = df[df["turnover"]<0.7]
    df["total"] = abs(df["fitness"] + df["sharpe"])
    # df =  df[df.apply(lambda x: (  x["LOW_2Y_SHARPE"] +1.58 >0.8) if x["sharpe"]>0
    #          else (x["LOW_2Y_SHARPE"] +1.58 <-0.8 ), axis=1)]
    df["CONCENTRATED_WEIGHT"].fillna(0, inplace=True)
    # CONCENTRATED_WEIGHT <40% 并且l两年sharpe需大于75%
    # df = df[(df["CONCENTRATED_WEIGHT"] < 0.3)]
    df["exp"] = df["code"].apply(lambda x: x.split("  ")[
                                 1] if "  " in x else x)
    df["op"] = df["code"].apply(lambda x: x.split("  ")[0])
    df.sort_values(by="total", inplace=True, ascending=False)

    # long+short<20的不考虑
    # df = df[df["longCount"]+df["shortCount"] > 20]
    # 按sharpe和fitness筛选
    arr = []
   # df = df[df["turnover"]<0.7]
    for i in df.index:
        curr = df.loc[i].to_dict()
        if abs(curr["investabilityConstrained_sharpe"]) > abs(curr["sharpe"]):
            curr["maxTrade"] = "ON"
            curr["sharpe"] = curr["investabilityConstrained_sharpe"]
            curr["fitness"] = curr["investabilityConstrained_fitness"]
        arr.append(curr)
    df = pd.DataFrame(arr)
    df = df[(abs(df["sharpe"]) >= sharpe) & (abs(df["fitness"]) >= fitness)]
    # CHN单独处理
    if model.yamldata.para.get("flip") == "YES":
        df = df[df["sharpe"]<0]
        if df.shape[0] ==0:
            return pd.DataFrame()
    elif model.yamldata.para.get("flip") == "NO":
        df = df[df["sharpe"]>0] 
    df = df.groupby(["exp", "op"]).head(n)
    df.reset_index(drop=True, inplace=True)
    print("处理之后的数据:", df.shape)
    print("sharpe, titness", sharpe, fitness)
    df["code"] = df.apply(lambda x: x["code"] if x["sharpe"]
                          > 0 else "-(" + x["code"] + ")", axis=1)
    df = init_settings(df)
    # df.to_json(case_df)
    return df
# def zero_sharpe_check(qua, df: pd.DataFrame, kwargs):
#     qua = quant()
#     qua.login()
#     zero = qua.zero_sharpe_count(df["id"].to_list())
#     return pd.concat([df, zero], join="inner", axis=1)

def init_settings(df: pd.DataFrame):
    arr = []
    settings = list(model.yamldata.settings.model_dump().keys())
    for i in df.index:
        js = df.loc[i].to_dict()
        js["settings"] = {i: js.get(i) for i in settings}
        js["visualization"] = False
        arr.append(js)
    return pd.DataFrame(arr)
