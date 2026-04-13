import json, os
import requests
import pandas as pd
import logging
import time
import requests
from typing import Optional, Tuple
from typing import Tuple, Dict, List
from typing import Union, List, Tuple
from concurrent.futures import ThreadPoolExecutor
import pickle
from collections import defaultdict
import datetime

# from tqdm import tqdm
from pathlib import Path
import sys
from model import cfg as gcfg

# import prodcheck
import save_db


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


def save_obj(obj: object, name: str) -> None:
    filename = os.path.join(cfg.data_path, name + ".pickle")
    with open(filename, "wb") as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)


def load_obj(name: str) -> pd.DataFrame:
    filename = os.path.join(cfg.data_path, name + ".pickle")
    if os.path.exists(filename):
        with open(filename, "rb") as f:
            return pickle.load(f)
    return {}


def wait_get(url: str, max_retries: int = 10):
    """
    发送带有重试机制的 GET 请求，直到成功或达到最大重试次数。
    此函数会根据服务器返回的 `Retry-After` 头信息进行等待，并在遇到 401 状态码时重新初始化配置。

    Args:
            url (str): 目标 URL。
            max_retries (int, optional): 最大重试次数，默认为 10。

    Returns:
            Response: 请求的响应对象。
    """
    print(url)
    retries = 0
    while retries < max_retries:
        while True:
            simulation_progress = cfg.sess.get(url)
            if simulation_progress.status_code == 401:
                cfg.sess = sign_in(cfg.username, cfg.password)
            if simulation_progress.headers.get("Retry-After", 0) == 0:
                break
            time.sleep(float(simulation_progress.headers["Retry-After"]))
        if simulation_progress.status_code < 400:
            break
        else:
            time.sleep(2**retries)
            retries += 1
    return simulation_progress


def _get_alpha_pnl(alpha_id: str) -> pd.DataFrame:
    """
    获取指定 alpha 的 PnL数据，并返回一个包含日期和 PnL 的 DataFrame。
    此函数通过调用 WorldQuant Brain API 获取指定 alpha 的 PnL 数据，
    并将其转换为 pandas DataFrame 格式，方便后续数据处理。
    Args:
            alpha_id (str): Alpha 的唯一标识符。
    Returns:
            pd.DataFrame: 包含日期和对应 PnL 数据的 DataFrame，列名为 'Date' 和 alpha_id。
    """
    save_path = cfg.data_path + f"pnls/{alpha_id}.pkl"
    if os.path.exists(save_path):
        return pd.read_pickle(save_path)
    print(f"开始下载:{alpha_id}")
    pnl = wait_get(
        "https://api.worldquantbrain.com/alphas/" + alpha_id + "/recordsets/pnl"
    ).json()
    df = pd.DataFrame(
        pnl["records"], columns=[item["name"] for item in pnl["schema"]["properties"]]
    )
    df = df.rename(columns={"date": "Date", "pnl": alpha_id})
    df = df[["Date", alpha_id]]
    # 最后一个值为负时，反转pnls值
    if df.loc[df.index[-1]][alpha_id] < 0:
        df[alpha_id] = -df[alpha_id]
    df.to_pickle(save_path)
    return df


def get_alphas_pnl(alpha_ids):
    """
    传入alpha-ids列表返回，list[df]
    id对应的pnl列
    """
    fetch_pnl_func = lambda alpha_id: _get_alpha_pnl(alpha_id).set_index("Date")
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = executor.map(fetch_pnl_func, [item["id"] for item in alpha_ids])
    alpha_pnls = pd.concat(list(results), axis=1)
    alpha_pnls.sort_index(inplace=True)
    return alpha_ids, alpha_pnls


def get_alpha_pnls(
    alphas: list[dict],
    alpha_pnls=None,  #: Optional[pd.DataFrame]
    alpha_ids=None,  # Optional[dict[str, list]]
) -> Tuple[dict[str, list], pd.DataFrame]:
    """
    获取 alpha 的 PnL 数据，并按区域分类 alpha 的 ID。
    Args:
            alphas (list[dict]): 包含 alpha 信息的列表，每个元素是一个字典，包含 alpha 的 ID 和设置等信息。
            alpha_pnls (Optional[pd.DataFrame], 可选): 已有的 alpha PnL 数据，默认为空的 DataFrame。
            alpha_ids (Optional[dict[str, list]], 可选): 按区域分类的 alpha ID 字典，默认为空字典。
    Returns:
            Tuple[dict[str, list], pd.DataFrame]:
                    - 按区域分类的 alpha ID 字典。
                    - 包含所有 alpha 的 PnL 数据的 DataFrame。
    """
    if alpha_ids is None:
        alpha_ids = defaultdict(list)
    if alpha_pnls is None:
        alpha_pnls = pd.DataFrame()

    new_alphas = [item for item in alphas if item["id"] not in alpha_pnls.columns]
    if not new_alphas:
        return alpha_ids, alpha_pnls

    for item_alpha in new_alphas:
        alpha_ids[item_alpha["settings"]["region"]].append(item_alpha["id"])
    fetch_pnl_func = lambda alpha_id: _get_alpha_pnl(alpha_id).set_index("Date")
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = executor.map(fetch_pnl_func, [item["id"] for item in new_alphas])
    alpha_pnls = pd.concat([alpha_pnls] + list(results), axis=1)
    alpha_pnls.sort_index(inplace=True)
    return alpha_ids, alpha_pnls


def get_os_alphas(limit: int = 10, count=0) -> List[Dict]:
    """
    获取OS阶段的alpha列表。
    limit：每次获取的数量，count：已获取的数量
    Returns:
            List[Dict]: 包含alpha信息的字典列表，每个字典表示一个alpha。
    """
    res_alphas = []
    # 获取当前所有alphas
    url = f"https://api.worldquantbrain.com/users/self/alphas?stage=OS&offset=0"
    res = wait_get(url).json()
    total_alphas = res["count"]
    for offset in range(0, total_alphas - count, limit):
        url = f"https://api.worldquantbrain.com/users/self/alphas?stage=OS&limit={limit}&offset={offset}&order=-dateSubmitted"
        res = wait_get(url).json()
        alphas = res["results"]
        res_alphas.extend(alphas)
        offset += limit
    return res_alphas


def get_list(df: pd.DataFrame, corr):
    finally_alphas = []

    def get_llll(df: pd.DataFrame, arr=[], father: list = []):
        if len(arr) <= 1:
            finally_alphas.append(father + arr)
            return None
        # 选择第一个

        get_llll(df, [i for i in arr[1:] if df.loc[arr[0]][i] < corr], father + arr[:1])
        # 不选择第一个

    #     if father:
    #         get_llll(df, arr[1:], father)
    if df is None:
        return 
    alpha_ids = df.columns.to_list()
    get_llll(df, alpha_ids, [])
    # print(finally_alphas)
    #     return alpha_ids
    return finally_alphas
def get_list2(df: pd.DataFrame, max_corr):
    result = []
    # 自相关性<0.72 && 与该相关性*pc<0.8?
    for i in df.columns.tolist():


        if not result:
            result.append(i)
            continue
        flag = True
        for _is in result:
            if df.loc[i, _is] > max_corr:
                flag = False
                break
        if flag:
            result.append(i)
    print(result)
    return result

def calc_all_corr(alpha_ids, sim_max: int = 0.95, prod_check=False) -> pd.DataFrame:
    # 加载已os的pnls数据
    #
    # 下载传入alpha_ids数据
    print("开始下载aplha的pnl")
    alpha_results = []
    alpha_res_func = lambda alpha_id: _get_alpha_pnl(alpha_id).set_index("Date")
    with ThreadPoolExecutor(max_workers=10) as executor:
        alpha_results = executor.map(alpha_res_func, alpha_ids)
    alpha_results = list(alpha_results)
    is_pnls = pd.concat(alpha_results, axis=1)

    # 算出增量，后一行减前一行的值
    is_rets = is_pnls - is_pnls.ffill().shift(1)
    # 保留四年时间的增量pnls
    is_rets = is_rets[
        pd.to_datetime(is_rets.index)
        > pd.to_datetime(is_rets.index).max() - pd.DateOffset(years=4)
    ]
    arr = []
    # 求最大相关性，挑选出满足的alpha id
    similar_l = {}
    col = is_rets.columns
    # return col
    # if not prod_check:
    #     return is_rets.corr()
    os_alpha_ids, os_alpha_rets = load_data()
    print(os_alpha_ids.keys())
    for alpha_id in alpha_ids:
        if alpha_id not in col:
            continue
        self_corr = (
            os_alpha_rets[os_alpha_ids[region]].corrwith(is_rets[alpha_id]).max()
        )
        update_corr(db_name, alpha_id, self_corr, "selfCorr")
        if self_corr < sim_max and self_corr<0.72:
            arr.append(alpha_id)
            similar_l[alpha_id] = self_corr
        # print(alpha_id, self_corr)
    # 计算满足条件的is数据的相关性
    if not similar_l:
        return None
    # 按照自相关系数排序
    df = is_rets[arr].corr()
    # print(df)
    df["selfcheck"] = list(similar_l.values())
    # print(df["selfcheck"].to_string())
    return df


def get_all_year_sharpe(table, alpha_id: str) -> pd.DataFrame:
    """
    传入id值，返回index为年份， col为id的DataFrame
    """
    print(f"download year data:{alpha_id}")
    result = wait_get(
        "https://api.worldquantbrain.com/alphas/"
        + alpha_id
        + "/recordsets/yearly-stats"
    ).json()
    # result['records'] 0为年份，6为sharpe
    df = pd.DataFrame(
        [{records[0]: records[6] for records in result["records"]}], index=[alpha_id]
    )
    # zc = case_db.query_year_sharpe(alpha_id)[1]
    zc = [records[6] for records in result["records"]].count(0)
    # save_db.insert_db(alpha_id, {"zeroCount": zc})
    sql = f"""UPDATE {table}
        SET zeroCount = {zc}
        WHERE id='{alpha_id}';"""
    save_db.exe_sql(sql)
    return df


def download_data(flag_increment=True):
    """
    下载数据并保存到指定路径。
    此函数会检查数据是否已经存在，如果不存在，则从 API 下载数据并保存到指定路径。
    Args:
            flag_increment (bool): 是否使用增量下载，默认为 True。
    """
    if flag_increment:
        os_alpha_ids = load_obj("os_alpha_ids")
        os_alpha_pnls = load_obj("os_alpha_pnls")
        ppac_alpha_ids = load_obj("ppac_alpha_ids")
        exist_alpha = [alpha for ids in os_alpha_ids.values() for alpha in ids]
    else:
        os_alpha_ids = None
        os_alpha_pnls = None
        exist_alpha = []
        ppac_alpha_ids = []
    if not os_alpha_ids:
        alphas = get_os_alphas(limit=100, count=0)
    else:
        alphas = get_os_alphas(limit=10, count=len(exist_alpha))

    alphas = [item for item in alphas if item["id"] not in exist_alpha]
    # ppac_alpha_ids += [item['id'] for item in alphas for item_match in item['classifications'] if item_match['name'] == 'Power Pool Alpha']
    # print(alphas, os_alpha_pnls,os_alpha_ids)
    os_alpha_ids, os_alpha_pnls = get_alpha_pnls(
        alphas, alpha_pnls=os_alpha_pnls, alpha_ids=os_alpha_ids
    )
    save_obj(os_alpha_ids, "os_alpha_ids")
    save_obj(os_alpha_pnls, "os_alpha_pnls")
    # save_obj(ppac_alpha_ids, 'ppac_alpha_ids')
    print(
        f"新下载的alpha数量: {len(alphas)}, 目前总共alpha数量: {os_alpha_pnls.shape[1]}"
    )
    return os_alpha_ids, os_alpha_pnls, ppac_alpha_ids


def load_data(tag=None):
    """
    加载数据。
    此函数会检查数据是否已经存在，如果不存在，则从 API 下载数据并保存到指定路径。
    Args:
            tag (str): 数据标记，默认为 None。
    """
    os_alpha_ids, os_alpha_pnls, ppac_alpha_ids = download_data()
    if tag == "PPAC":
        for item in os_alpha_ids:
            os_alpha_ids[item] = [
                alpha for alpha in os_alpha_ids[item] if alpha in ppac_alpha_ids
            ]
    elif tag == "SelfCorr":
        for item in os_alpha_ids:
            os_alpha_ids[item] = [
                alpha for alpha in os_alpha_ids[item] if alpha not in ppac_alpha_ids
            ]
    else:
        os_alpha_ids = os_alpha_ids
    exist_alpha = [alpha for ids in os_alpha_ids.values() for alpha in ids]
    os_alpha_pnls = os_alpha_pnls[exist_alpha]
    os_alpha_rets = os_alpha_pnls - os_alpha_pnls.ffill().shift(1)
    os_alpha_rets = os_alpha_rets[
        pd.to_datetime(os_alpha_rets.index)
        > pd.to_datetime(os_alpha_rets.index).max() - pd.DateOffset(years=4)
    ]
    return os_alpha_ids, os_alpha_rets


class cfg:
    data_path = gcfg.check_path
    with open(gcfg.path + "brain.txt") as f:
        username, password = json.load(f)
    sess: requests.Session = None


def query(table):

    col = [
        "id",
        "sharpe",
        "fitness",
        "margin",
        "LOW_2Y_SHARPE",
        "prodCorr",
        "zeroCount",
    ]
    y2 = ""
    if "SUPER" in table:
        col.remove("LOW_2Y_SHARPE")
        fitness = 4.95
    else:
        
        y2="AND ( abs(sharpe)*0.8 <  abs(LOW_2Y_SHARPE +1.58) OR LOW_2Y_SHARPE>0) "
        fitness = 0.9
    sql = f"""SELECT {",".join(col)}
        FROM {table} WHERE   abs(fitness) > {fitness} AND margin>5
        AND abs(sharpe)>1.55 {y2} AND zeroCount<4
        
        AND (CONCENTRATED_WEIGHT<0.05 OR CONCENTRATED_WEIGHT IS NULL) ORDER BY fitness DESC, sharpe DESC;"""
    #  AND prodCorr IS NULL
    data = save_db.query(sql)[1]
    print(sql)
    df = pd.DataFrame(data, columns=col)
    return df


def query_result(table, ids):

    col = [
        "id",
        "sharpe",
        "fitness",
        "turnover",
        "margin",
        "LOW_2Y_SHARPE",
        "LOW_ROBUST_UNIVERSE_SHARPE",
        "neutralization",
        "selfCorr",
        "prodCorr",
        
    ]
    if "GLB" in table:
        col += ["LOW_GLB_AMER_SHARPE", "LOW_GLB_APAC_SHARPE", "LOW_GLB_EMEA_SHARPE"]
    col.append("zeroCount")
    rep = {
        "LOW_2Y_SHARPE": "2Y",
        "LOW_ROBUST_UNIVERSE_SHARPE": "RS",
        "LOW_GLB_AMER_SHARPE": "AMER",
        "LOW_GLB_APAC_SHARPE": "APAC",
        "LOW_GLB_EMEA_SHARPE": "EMEA",
        "neutralization": "neu",

    }
    sql = f"""SELECT {",".join(col)} 
        FROM {table} WHERE id IN ({ids}) ORDER BY fitness DESC , sharpe DESC;"""
    #  AND prodCorr IS NULL
    data = save_db.query(sql)[1]
    col = [rep.get(i, i) for i in col]
    df = pd.DataFrame(data, columns=col)
    return df


def update_corr(table, sid, corr, name):
    sql = f"""UPDATE {table}
        SET {name} = {corr}
        WHERE id='{sid}';"""
    return save_db.exe_sql(sql)


def prod_corr_check(sess: requests.Session, alpha_id):
    # 查询prodcorr
    url = "https://api.worldquantbrain.com/alphas/" + alpha_id + "/correlations/prod"
    while True:
        result = wait_get(url).json()
        try:
            print(alpha_id, result["max"], result["min"])
            return result["max"]
        except Exception as e:
            print(e)
            break


def init(df: pd.DataFrame, sim_max=0.95, prod_check=False):
    #
    alphas = df["id"].values.tolist()[:]
    cfg.sess = sign_in(cfg.username, cfg.password)
    # 做自相关计算
    dfs = calc_all_corr(alphas, sim_max=sim_max, prod_check=prod_check)
    cfg.sess.close()

    check: list = get_list(dfs, corr=sim_max)[0]
    if not prod_check:
        print(query_result(db_name, str(dfs.index.to_list())[1:-1]).to_string())
        return check
    df.fillna(0, inplace=True)
    arr = df[df["prodCorr"] == 0]["id"].values.tolist()
    check.remove("selfcheck")
    print("可检查的总数：", len(arr), arr)
    check = [i for i in check if i in arr]
    check = [i for i in check if not df[df["id"] == i]["prodCorr"].values.tolist()[0]]
    print("当前需要检查的id", len(check), check)
    for j, i in enumerate(check[:15]):
        print(j, i)
        print(df[df["id"] == i])
        pc = prod_corr_check(cfg.sess, i)
        update_corr(db_name, i, pc, "prodCorr")
    df = query(db_name)
    df = df[df["prodCorr"] < 0.72]
    alphas = df["id"].values.tolist()[:]
    alphas = [i for i in alphas if i in dfs.columns]
    show = dfs[alphas]
    show = show[show.index.isin(alphas)]
    # print(show)
    result = get_list(show, corr=0.72)[0]

    show = dfs[result]
    show = show[show.index.isin(result)]
    print(result)
    print(show)
    df = query_result(db_name, str(result)[1:-1])
    df = df[df["zeroCount"].isnull()]
    for i in df["id"].values.tolist():
        get_all_year_sharpe(db_name, i)
    print(query_result(db_name, str(result)[1:-1]).to_string())
    # return arr


db_name = sys.argv[1].replace("-", "_")
if "_result" not in db_name:
    db_name += "_result"
region = db_name.split("_")[0].upper() if len(sys.argv) < 3 else sys.argv[2]
df = query(db_name)
print(df.shape[0])
if df.shape[0] == 0:
    exit(0)
result = init(df, sim_max=0.99, prod_check=False)
