import tqdm
import os
import requests
import pandas as pd
import logging
import time
from typing import Optional, Tuple
from concurrent.futures import ThreadPoolExecutor
import pickle
from collections import defaultdict
from pydantic import BaseModel
import api
from pathlib import Path
import sys
from model import cfg as gcfg

import save_db
from plot import load_plt


def sign_in(username, password):
    s = requests.Session()
    s.auth = (username, password)
    try:
        response = s.post("https://api.worldquantbrain.com/authentication")
        response.raise_for_status()
        logging.info("Successfully signed in")
        return s
    except requests.exceptions.RequestException as e:
        logging.error(f"Login failed: {e}")
        return None


class check_para(BaseModel):
    low_sharpe: float = -0.26
    sharpe: float = 1.4
    fitness: float = 0.9
    margin: float = 5
    IS_LADDER_SHARPE: float = 0.9
    LOW_2Y_SHARPE: float = 0.9
    LOW_SUB_UNIVERSE_SHARPE: float = -0.1
    CONCENTRATED_WEIGHT: float = 0.0
    selfCorr: float = 0.7
    prodCorr: float = 0.7
    zeroCount: int = 3
    LOW_ASI_JPN_SHARPE: float = -0.2
    LOW_ROBUST_UNIVERSE_SHARPE: float = low_sharpe
    LOW_GLB_AMER_SHARPE: float = low_sharpe
    LOW_GLB_APAC_SHARPE: float = low_sharpe
    LOW_GLB_EMEA_SHARPE: float = low_sharpe
    LOW_ROBUST_UNIVERSE_RETURNS: float = low_sharpe
    LOW_INVESTABILITY_CONSTRAINED_SHARPE: float = -0.1


def gen_conditions(para: check_para, col):
    cons = {"sharpe": f"sharpe >={para.sharpe}",
            "fitness":  f"fitness > {para.fitness if cfg.db_type != 'SUPER' else 3}",
            "margin":  f"margin > {para.margin}",
            "IS_LADDER_SHARPE":  f"(IS_LADDER_SHARPE+1.58 >sharpe * {para.IS_LADDER_SHARPE} OR IS_LADDER_SHARPE IS NULL)",
            "CONCENTRATED_WEIGHT":  f"(CONCENTRATED_WEIGHT < {para.CONCENTRATED_WEIGHT} OR CONCENTRATED_WEIGHT IS NULL)",
            #  "selfCorr": f"(selfCorr < {para.selfCorr} OR selfCorr IS NULL)" ,
            "LOW_SUB_UNIVERSE_SHARPE": f"LOW_SUB_UNIVERSE_SHARPE > {para.LOW_SUB_UNIVERSE_SHARPE}",
            "LOW_2Y_SHARPE":  f"(LOW_2Y_SHARPE+1.58 > sharpe * {para.LOW_2Y_SHARPE} OR LOW_2Y_SHARPE>0 OR LOW_2Y_SHARPE IS NULL)",
            "zeroCount": f"zeroCount <= {para.zeroCount}",
            "LOW_ASI_JPN_SHARPE": f"(LOW_ASI_JPN_SHARPE > {para.LOW_ASI_JPN_SHARPE} OR LOW_ASI_JPN_SHARPE IS NULL)",
            "LOW_ROBUST_UNIVERSE_SHARPE": f"(LOW_ROBUST_UNIVERSE_SHARPE > {para.LOW_ROBUST_UNIVERSE_SHARPE} OR LOW_ROBUST_UNIVERSE_SHARPE IS NULL)",
            "LOW_GLB_AMER_SHARPE": f"(LOW_GLB_AMER_SHARPE > {para.LOW_GLB_AMER_SHARPE} OR LOW_GLB_AMER_SHARPE IS NULL)",
            "LOW_GLB_APAC_SHARPE": f"(LOW_GLB_APAC_SHARPE > {para.LOW_GLB_APAC_SHARPE} OR LOW_GLB_APAC_SHARPE IS NULL)",
            "LOW_GLB_EMEA_SHARPE": f"(LOW_GLB_EMEA_SHARPE > {para.LOW_GLB_EMEA_SHARPE} OR LOW_GLB_EMEA_SHARPE IS NULL)",
            "LOW_ROBUST_UNIVERSE_RETURNS": f"(LOW_ROBUST_UNIVERSE_RETURNS > {para.LOW_ROBUST_UNIVERSE_RETURNS} OR LOW_ROBUST_UNIVERSE_RETURNS IS NULL)",
            "LOW_INVESTABILITY_CONSTRAINED_SHARPE": f"(LOW_INVESTABILITY_CONSTRAINED_SHARPE>{para.LOW_INVESTABILITY_CONSTRAINED_SHARPE} OR LOW_INVESTABILITY_CONSTRAINED_SHARPE IS NULL)"
            }
    cons = {i: cons[i] for i in cons if i in col}
    return list(cons.values())


def get_os_alphas(limit: int = 10, count=0) -> pd.DataFrame:
    """
    获取OS阶段的alpha列表。
    limit：每次获取的数量，count：已获取的数量
    Returns:
            List[Dict]: 包含alpha信息的字典列表，每个字典表示一个alpha。
    """
    res_alphas = []
    # 获取当前所有alphas
    url = f"https://api.worldquantbrain.com/users/self/alphas?stage=OS&offset=0&limit=1"
    res = qua.wait_get(url).json()
    total_alphas = res["count"]
    print(f"alpha总数为: {total_alphas}, 需要下载的数量为: {total_alphas-count}")
    if total_alphas - count > 50:
        limit = 100
    columns = []
    for offset in range(0, total_alphas - count, limit):
        url = f"https://api.worldquantbrain.com/users/self/alphas?stage=OS&limit={limit}&offset={offset}&order=-dateSubmitted"
        res = qua.wait_get(url).json()
        result: list = res["results"]

        for alp in result:
            alpha_dict = dict()
            # "selfCorrelation","prodCorrelation" ]
            attr = ["id", "type", "dateSubmitted",]
            settings = ["region", "universe", "delay",
                        "neutralization", "startDate",]
            for i in attr:
                alpha_dict[i] = alp[i]
            alpha_dict["classifications"] = [i["name"]
                                             for i in alp["classifications"]]
            alpha_dict["RA"] = (
                True if "Regular Alpha" in alpha_dict["classifications"] else False
            )
            alpha_dict["selfCorrelation"] = alp["is"]["selfCorrelation"]
            alpha_dict["prodCorrelation"] = alp["is"]["prodCorrelation"]
            alpha_dict["os_sharpe"] = alp["os"].get("sharpe")
            alpha_dict["Ration"] = alp["os"].get("osISSharpeRatio")
            for i in settings:
                alpha_dict[i] = alp["settings"][i]
            alpha_dict["pyramids"] = []
            if alp["type"] == "SUPER":
                alpha_dict["code"] = " " + alp["combo"]["code"]
            else:
                alpha_dict["code"] = " " + alp["regular"]["code"]
                pyramids = [
                    p["pyramids"] for p in alp["is"]["checks"] if "pyramids" in p
                ][0]
                alpha_dict["pyramids"] = [i.get("name") for i in pyramids]
            columns = list(alpha_dict.keys())
            res_alphas.append(alpha_dict)
        offset += limit
    return pd.DataFrame(res_alphas, columns=columns)


def calc_all_corr(
    df: pd.DataFrame, region="USA", sim_max: int = 0.95, tag="RA"
) -> pd.DataFrame:
    print("开始下载aplha的pnl")
    print(df[["id", "selfCorr", "prodCorr"]].to_string())

    is_alpha_ids = df["id"].values.tolist()[:]
    alpha_results = []

    def alpha_res_func(alpha_id): return qua._get_alpha_pnl(
        alpha_id).set_index("Date")
    with ThreadPoolExecutor(max_workers=10) as executor:
        alpha_results = executor.map(alpha_res_func, is_alpha_ids)
    is_pnls = pd.concat(list(alpha_results), axis=1)
    # 算出增量，后一行减前一行的值
    is_pnls = is_pnls - is_pnls.ffill().shift(1)
    # 保留四年时间的增量pnls
    is_pnls_4y: pd.DataFrame = is_pnls[
        pd.to_datetime(is_pnls.index)
        > pd.to_datetime(is_pnls.index).max() - pd.DateOffset(years=4)
    ]
    arr = []
    # 求最大相关性，挑选出满足的alpha id
    similar_l = {}
    # 加载OS相关数据，包括原始数据和pnls
    os_df, os_pnls = load_os_data()
    os_pnls = os_pnls - os_pnls.ffill().shift(1)
    os_pnls_4y: pd.DataFrame = os_pnls[
        pd.to_datetime(os_pnls.index)
        > pd.to_datetime(os_pnls.index).max() - pd.DateOffset(years=4)
    ]
    # print(f"{region} os_ids's {tag} count", len(os_ids))
    df = df.set_index("id")
    region = db_name.replace("-", "_").split("_")[0]
    print(region)
    os_ids = os_df[os_df["region"] == region]["id"].tolist()
    # print(os_ids)
    region_pnls: pd.DataFrame = os_pnls_4y[os_ids]
    # 多线程计算相关性
    def sc_func(alpha_id): return (
        alpha_id, region_pnls.corrwith(is_pnls_4y[alpha_id]).max())
    with ThreadPoolExecutor(max_workers=10) as executor:
        alpha_corrs = tqdm.tqdm(executor.map(sc_func, is_alpha_ids))
    alpha_corrs = list(alpha_corrs)
    [save_db.exe_sql(
        f"UPDATE {cfg.db_name} SET selfCorr = {i[1]} WHERE id='{i[0]}';") for i in alpha_corrs]
    similar_l = [i[0] for i in alpha_corrs if float(i[1]) <= sim_max]
    print(similar_l)
    if not similar_l:
        return pd.DataFrame()
    similar_l = [i for i in is_alpha_ids if i in similar_l]
    print(similar_l, is_alpha_ids)
    # print(is_pnls_4y[arr])
    return is_pnls_4y[similar_l].corr()


def get_year_zero_sharpe_count(table, alpha_id: str) -> pd.DataFrame:
    """
    传入id值，返回index为年份， col为id的DataFrame
    """
    print(f"download year data:{alpha_id}")
    result = qua.wait_get(
        f"https://api.worldquantbrain.com/alphas/{alpha_id}/recordsets/yearly-stats"
    ).json()
    # result['records'] 0为年份，6为sharpe
    df = pd.DataFrame(
        [{records[0]: records[6] for records in result["records"]}], index=[alpha_id]
    )
    zc = [records[6] for records in result["records"]].count(0)
    sql = f"""UPDATE {table}
        SET zeroCount = {zc}
        WHERE id='{alpha_id}';"""
    save_db.exe_sql(sql)
    return df


def load_os_data() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    加载已提交历史数据。
    """
    # 1.检查存在os_ids则加载
    os_csv = cfg.data_path + "os_alpha_ids.csv"
    df = pd.DataFrame()
    if os.path.exists(os_csv):
        df = pd.read_csv(os_csv, index_col=0).drop_duplicates(["id"])
    os_aplha = get_os_alphas(limit=10, count=df.shape[0])
    df = pd.concat([df, os_aplha])
    df.drop_duplicates(["id"], inplace=True)
    df.to_csv(os_csv)
    pnls: pd.DataFrame = pd.read_pickle(cfg.data_path + "os_alpha_pnls.pickle")
    for alpha_id in df["id"].tolist():
        if alpha_id not in pnls.columns:
            pnls[alpha_id] = qua._get_alpha_pnl(
                alpha_id).set_index("Date")[alpha_id]
    pnls.to_pickle(cfg.data_path + "os_alpha_pnls.pickle")
    return df, pnls


class cfg:
    data_path = gcfg.check_path
    # with open(gcfg.path + "brain.txt") as f:
    #     username, password = json.load(f)
    sess: requests.Session = None
    db_name = ""
    db_type = "REGULAR"


def query(table: str):
    # 查询当前数据库的col
    columns = save_db.query(f"SHOW COLUMNS FROM {table};")[1]
    # 当前列是否在需要的列中
    get_col(table.split("_")[0])
    col = cols.RA_col if cfg.db_type != "SUPER" else cols.SA_col
    columns = [i[0] for i in columns]
    columns = [i for i in col if i in columns]
    col = cols.RA_col = columns
    if "SUPER" in table:
        col = cols.SA_col
    conditions = gen_conditions(check_para(), col)
    print(conditions)
    # neutralization IN ("REVERSION_AND_MOMENTUM", "FAST", "SLOW", "CROWDING")
    sql = f"""SELECT {",".join(col)}
        FROM {table} WHERE    {" AND ".join(conditions)} AND region='{region}' ORDER BY fitness DESC, sharpe DESC;"""
    data = save_db.query(sql)[1]
    print(sql)
    df = pd.DataFrame(data, columns=col)
    return df


def query_result(table, ids):
    col = cols.RA_col
    if "SUPER" in table:
        col = cols.SA_col
    rep = {
        "LOW_2Y_SHARPE": "2Y",
        "LOW_ROBUST_UNIVERSE_SHARPE": "RS",
        "LOW_GLB_AMER_SHARPE": "AMER",
        "LOW_GLB_APAC_SHARPE": "APAC",
        "LOW_GLB_EMEA_SHARPE": "EMEA",
        "LOW_SUB_UNIVERSE_SHARPE": "SUB",
        "LOW_ASI_JPN_SHARPE": "JPN",
        "LOW_ROBUST_UNIVERSE_RETURNS": "RR",
        "IS_LADDER_SHARPE": "ISS",
        "CONCENTRATED_WEIGHT": "WEIGHT",
        "LOW_INVESTABILITY_CONSTRAINED_SHARPE": "INVEST"
    }
    sql = f"""SELECT {",".join(col)} 
        FROM {table} WHERE id IN ({ids}) ORDER BY fitness DESC , sharpe DESC;"""
    data = save_db.query(sql)[1]
    col = [rep.get(i, i) for i in col]
    df = pd.DataFrame(data, columns=col)
    return df


def init(db_name, sim_max=0.95, check_count=10):
    df = query(db_name)
    # exit(0)
    print("符合条件的总数：", df.shape[0])
    if df.shape[0] == 0:
        return 0
    # 做自相关计算,返回行列均为alphaid的自相关DataFrame
    dfs = calc_all_corr(
        df, sim_max=check_para().selfCorr, region=cfg.db_name.split("_"[0]))
    # 获取需要计算的alpha id列表
    if dfs.shape[0] == 0:
        print("没有符合需要检查的alpha")
        return 0
    check: list = qua.get_list(dfs, max_corr=sim_max)
    print("check", check)
    df: pd.DataFrame = df[df["prodCorr"].isnull()]
    df = df.copy()
    df.fillna(0, inplace=True)
    print(df)
    arr = df["id"].values.tolist()
    print("可检查的总数：")
    # print(check, arr)
    check = [i for i in check if i in arr]
    print("当前需要检查的id", len(check), check)
    qua.login() if check else None
    for j, i in enumerate(check[:check_count]):
        print(j, df[df["id"] == i].to_string())
        pc = qua.prod_corr_check(i)
        save_db.exe_sql(
            f"""UPDATE {db_name} SET prodCorr = {pc} WHERE id='{i}';""")
    # 重新查询符合条件的alpha，并且pc小于预定pc，且id在首次查询中，并选取相关性表
    df = query(db_name)
    df = df[(df["prodCorr"] < check_para().prodCorr)]
    alphas = [i for i in df["id"].values.tolist() if i in dfs.index]
    show = dfs.loc[alphas, alphas]
    print(show)
    result: list[dir] = qua.get_list(show, max_corr=0.70)
    print(result)
    df = query_result(db_name, str(result)[1:-1])
    print(show.loc[result][result])
    df.dropna(axis=1, how='all', inplace=True)
    if df.shape[0] > 0:
        os.system(
            f"echo {df.shape[0]} {' '.join(df['id'].tolist())} >> result.txt")
    print(df.to_string())
    load_plt(result)
    return df


class cols:
    RA_col: list = None
    SA_col: list = None
    pass


def get_col(region):
    RA_col = ["id", "sharpe", "fitness", "margin", "LOW_2Y_SHARPE", "CONCENTRATED_WEIGHT", "region", "selfCorr",  "prodCorr",
              "IS_LADDER_SHARPE", "LOW_SUB_UNIVERSE_SHARPE", ]
    SA_col = ["id", "sharpe", "fitness", "margin",
              "region", "selfCorr", "prodCorr"]
    # if cfg.db_type == "SUPER":
    #     return SA_col
    if region == "GLB":
        RA_col += ["LOW_GLB_AMER_SHARPE",
                   "LOW_GLB_APAC_SHARPE", "LOW_GLB_EMEA_SHARPE"]
    elif region == "IND":
        RA_col += ["LOW_ROBUST_UNIVERSE_SHARPE"]
    elif region == "ASI":
        RA_col += ["LOW_ASI_JPN_SHARPE",
                   "LOW_ROBUST_UNIVERSE_SHARPE", "LOW_ROBUST_UNIVERSE_RETURNS"]
    elif region in ["JPN", "TWN", "ASI"]:
        RA_col += ["LOW_INVESTABILITY_CONSTRAINED_SHARPE"]
    RA_col.append("code")
    cols.RA_col = RA_col
    cols.SA_col = SA_col
    return RA_col, SA_col


if __name__ == "__main__":
    qua = api.quant()
    qua.login()
    cfg.sess = qua.sess
    db_name = sys.argv[1].replace("-", "_")
    if "_result" not in db_name:
        db_name += "_result"
    cfg.db_name = db_name
    save_db.inin_database('alphas')
    save_db.inin_database(db_name.split("_")[0])
   # init_yamldata(project)
    sql = f'SELECT id FROM {db_name}'
    save_db.query(sql)[1]
    if "SUPER" in db_name:
        cfg.db_type = "SUPER"

    region = db_name.split("_")[0].upper() if len(
        sys.argv) < 3 else sys.argv[2]
    result = init(db_name, sim_max=0.99,
                  check_count=10)
