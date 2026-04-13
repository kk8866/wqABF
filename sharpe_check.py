import json, os, sys
import requests
import pandas as pd
import logging
import time
import requests
from typing import Tuple, Dict, List,Union
from concurrent.futures import ThreadPoolExecutor

class cfg:
        data_path =  "/storage/emulated/0/qua/check/" \
        if sys.platform == "linux"\
        else "C:\\Project\\qua\\check\\"
        with open(data_path + '../brain.txt') as f:
               username, password = json.load(f)
        sess :requests.Session= None
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
def get_all_year_sharpe(alpha_id: str) -> pd.DataFrame:
        """
        获取指定 alpha 的 PnL数据，并返回一个包含日期和 PnL 的 DataFrame。
        此函数通过调用 WorldQuant Brain API 获取指定 alpha 的 PnL 数据，
        并将其转换为 pandas DataFrame 格式，方便后续数据处理。
        Args:
                alpha_id (str): Alpha 的唯一标识符。
        Returns:
                pd.DataFrame: 包含日期和对应 PnL 数据的 DataFrame，col为年份，index为id，字段为sharpe。
        """
        print(f"开始下载:{alpha_id}")
        result = wait_get("https://api.worldquantbrain.com/alphas/" + alpha_id + "/recordsets/yearly-stats").json()
        # result['records'] 0为年份，6为sharpe
        df = pd.DataFrame([{records[0]: records[6] for records in result['records']}], index=[alpha_id])
        return df
def get_produc_corr(alpha_list: list[str],sess:requests.Session=None, sharpe:int=0, count: int=1, use_abs:bool=True)->List:
        '''
        alpha_list: alpha list
        sharpe：不超过abs(sharpe)
        count: 最大多少年sharpe小于abs(sharpe),默认值1 year
        传入alpha id list 返回有满足
        use_abs:是否使用绝对值
        '''
        if type(sess) == type(None):
                cfg.sess = sign_in(cfg.username, cfg.password)
        else:
                cfg.sess=sess
        def get_sharpe(usharpe, uabs):
                return abs(usharpe) if uabs else usharpe
        with ThreadPoolExecutor(max_workers=10) as executor:
                #   多线程获取alpha 每年year数据
                results = executor.map(lambda alpha_id: get_all_year_sharpe(alpha_id), alpha_list)
        df = pd.concat(list(results), axis=0)
        df["count"] = df.apply(lambda x: len([i for i in x if get_sharpe(i, use_abs) <= sharpe]))
        cfg.sess.close()
        return df[df["count"] <= count].index.tolist()

