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

    
def sign_in(username, password):
        s = requests.Session()
        s.auth = (username, password)
        try:
                response = s.post('https://api.worldquantbrain.com/authentication')
                response.raise_for_status()
                logging.info("Successfully signed in")
                return s
        except requests.exceptions.RequestException as e:
                logging.error(f"Login failed: {e}")
                return None
def save_obj(obj: object, name: str) -> None:
    filename = os.path.join(cfg.data_path, name+ '.pickle')
    with open(filename, 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)
def load_obj(name: str) -> pd.DataFrame:
    filename = os.path.join(cfg.data_path, name+'.pickle')
    if os.path.exists(filename):
        with open(filename, 'rb') as f:
            return pickle.load(f)
    return []

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
        retries = 0
        while retries < max_retries:
                while True:
                        simulation_progress = cfg.sess.get(url)
                        if simulation_progress.headers.get("Retry-After", 0) == 0:
                                break
                        time.sleep(float(simulation_progress.headers["Retry-After"]))
                if simulation_progress.status_code < 400:
                        break
                else:
                        time.sleep(2 ** retries)
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
        save_path = cfg.data_path+f"pnls/{alpha_id}.pkl"
        if os.path.exists(save_path):
                return pd.read_pickle(save_path)
        print(f"开始下载:{alpha_id}")
        pnl = wait_get("https://api.worldquantbrain.com/alphas/" + alpha_id + "/recordsets/pnl").json()
        df = pd.DataFrame(pnl['records'], columns=[item['name'] for item in pnl['schema']['properties']])
        df = df.rename(columns={'date':'Date', 'pnl':alpha_id})
        df = df[['Date', alpha_id]]
        # 最后一个值为负时，反转pnls值
        if df.loc[df.index[-1]][alpha_id] < 0:
            df[alpha_id] = - df[alpha_id]
        df.to_pickle(save_path)
        return df
def get_alphas_pnl(alpha_ids):
    '''
    传入alpha-ids列表返回，list[df]
    id对应的pnl列
    '''
    fetch_pnl_func = lambda alpha_id: _get_alpha_pnl(alpha_id).set_index('Date')
    with ThreadPoolExecutor(max_workers=10) as executor:
            results = executor.map(fetch_pnl_func, [item['id'] for item in alpha_ids])
    alpha_pnls = pd.concat(list(results), axis=1)
    alpha_pnls.sort_index(inplace=True)
    return alpha_ids, alpha_pnls

def get_alpha_pnls(
        alphas: list[dict],
) -> Tuple[dict[str, list], pd.DataFrame]:
        """
        获取 alpha 的 PnL 数据，并按区域分类 alpha 的 ID。
        Args:
                alphas (list[str]): 包含 alpha id信息。
        Returns:
                Tuple[dict[str, list], pd.DataFrame]:
                        - alpha id信息。
                        - 包含所有 alpha 的 PnL 数据的 DataFrame。
        """
        fetch_pnl_func = lambda alpha_id: _get_alpha_pnl(alpha_id).set_index('Date')
        with ThreadPoolExecutor(max_workers=10) as executor:
                results = executor.map(fetch_pnl_func, [item for item in alphas])
        alpha_pnls = pd.concat(list(results), axis=1)
        alpha_pnls.sort_index(inplace=True)
        return alphas, alpha_pnls

def check_pnl_zero(alpha_ids, sim_max: int = 0.7)->pd.DataFrame:
        print("开始获取aplha pnl信息")
        _, is_pnls = get_alpha_pnls(alpha_ids)
        # 算出增量，后一行减前一行的值
        is_rets = is_pnls - is_pnls.ffill().shift(1)
        arr = []
        print(is_rets)
        total = is_rets.shape[0]
        for i in is_rets.columns:
            zero_count = is_rets[is_rets[i]==0.0].shape[0]
            print(i, zero_count/total)
            if zero_count/total <0.1:
                arr.append(i)
                continue
        print(arr)
        return arr
class cfg:
        data_path =  "/storage/emulated/0/qua/check/" \
        if sys.platform == "linux"\
        else "C:\\Project\\qua\\check\\"
        with open(data_path + '../brain.txt') as f:
               username, password = json.load(f)
        sess = None
def check(data_name: str)->pd.DataFrame:          
    cfg.sess = sign_in(cfg.username, cfg.password) 
    alphas = ["XAwb8vX","ZvwAgbY","pegxKwX","1Avq0Ym","vo7N8KG","3vww68N","YoLozz6","Ojl8Jgv","GNZRzjO","lYPmrKx","a8wWzEW", "Ro5ON1o","oXPbMrm","6QGLaXJ","Oj6jomv","PbPAamM","jJVOKjQ"]
    data_name = "analyst48-EUR-1-TOP2500-div1-ff.csv.csv"
    path = f"C:\\Project\\qua\\{data_name.split('-')[0]}\\"
    df = pd.read_csv(path+data_name)
    alphas = df["id"].values.tolist() 
    # alphas = ["dJAembX"]
    ids = check_pnl_zero(alphas, sim_max=0.7)
    now = datetime.datetime.now()
    formatted_date = now.strftime("%Y%m%d-%H%M%S")
    df = df[df["id"].isin(ids)]
    df.to_csv(cfg.data_path+f"{formatted_date}-check.csv")
    return df
# df.drop("selfcheck", axis=1, inplace=True)
check("11")
