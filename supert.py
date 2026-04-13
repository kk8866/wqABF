import os
import requests
import time
import json
from requests.auth import HTTPBasicAuth
import queue
import pandas as pd
from time import sleep
import traceback
import case_db
from loger import log
# from config import yamldata
from model import cfg, load_status, save_status, yamldata, current_model, status
from concurrent.futures import ThreadPoolExecutor
import threading
import save_db

lock_alpha_result = threading.Lock()
lock_submit = threading.Lock()


class quant:
    def __init__(self) -> None:
        self.arr = {}
        self.count = 0
        self.que = queue.Queue()

        self.sess = requests.Session()
        self.ids = []

    def login(self,):
        with open(cfg.path + 'brain.txt') as f:
            self.username, self.password = json.load(f)
        self.sess.auth = HTTPBasicAuth(self.username, self.password)
        #self.sess.close()
        response = self.sess.post(
            'https://api.worldquantbrain.com/authentication')
        print(response.status_code)

    def deal_single_alpha_result(self, alpha_id) -> dict:
        '''传入获取的数量和开始的时间参数
        返回df'''
        # 2025-07-04T22:52:30

        url = f"https://api.worldquantbrain.com/alphas/{alpha_id}"
        log.log(url)
        alphas_p = self.wait_get(url).json()
        if not alphas_p:
            return
        result = dict()
        result["id"] = alphas_p["id"]
        # 获取表达式
        result["code"] = alphas_p["combo"]["code"]
        result["combo"] = alphas_p["combo"]["code"]
        result["selection"] = alphas_p["selection"]
        # 检查checks中是否有fail
        result["result"] = False if [i.get("name") 
            for i in alphas_p["is"]["checks"] if i.get("result") == "FAIL"] else True
        # is中单keys数据类型的值直接保留
        alpha_is: dict[str, dict] = alphas_p["is"]
        result.update({i: alpha_is[i] for i in alpha_is if isinstance(
            alpha_is[i], (float, str, int))})
        # 保存checks中的详细数据
        no_check = ["LOW_SHARPE", "LOW_FITNESS",
                    "HIGH_TURNOVER", "LOW_TURNOVER"]
        checks = {i["name"]: round(i.get("value", -4) - i.get("limit"), 4) for i in alphas_p["is"]["checks"]
                  if ("limit" in i and i.get("name") not in no_check)}
        # 暂定2y sharpe保留为原始值
        result.update(checks)

        # 保存其他项的sharpe和fitness
        _fit = {i+"_fitness": alpha_is[i].get("fitness")
                for i in alpha_is if isinstance(alpha_is[i], dict)}
        _sharpe = {i+"_sharpe": alpha_is[i].get("fitness")
                   for i in alpha_is if isinstance(alpha_is[i], dict)}
        result.update(_fit)
        result.update(_sharpe)
        result.update(alphas_p["settings"])
        result["settings"] = alphas_p["settings"]
        # 删除settings中不需要的字段
        del result["settings"]["startDate"]
        del result["settings"]["endDate"]
        result["dateCreated"] = alphas_p["dateCreated"]
        result["margin"] = result["margin"]*1e4
        return result
    def get_all_alpha(self, region="USA"):

        '''传入获取的数量和开始的时间参数
        返回df'''
        # 2025-07-04T22:52:30
        
        self.login()
        # print(alpha_num, s_time)
        for i in range(0, 300, 100):
            print(i)
            url = f"https://api.worldquantbrain.com/users/self/alphas?limit=100&region={region}&type=SUPER&offset=%d"%(i) \
                    + "&status=UNSUBMITTED%1FIS_FAIL&" + "dateCreated%3E=2025-11-27T00:00:00-05:00" #is.sharpe
            print(url)
            response = self.sess.get(url)
            print(response)
            # print(response.json())
            alpha_list = response.json()["results"]
            arr = []
            print(len(alpha_list))
            if len(alpha_list)==0:
                return pd.DataFrame(arr)
            for alphas_p in alpha_list:
                result = dict()
                result["id"] = alphas_p["id"]
                
                # 获取表达式
                
                result["code"] = alphas_p["combo"]["code"]
                result["combo"] = alphas_p["combo"]["code"]
                # result["code"] = alphas_p["regular"]["code"]
                # if "snt23" not in alphas_p["regular"]["code"]:
                #     print(alphas_p["regular"]["code"])
                #     continue
                # 检查checks中是否有fail
                # result["code"] = alphas_p["regular"]["code"]
        # 检查checks中是否有fail
                result["result"] = False if [i.get("name") 
                    for i in alphas_p["is"]["checks"] if i.get("result") == "FAIL"] else True
                # is中单keys数据类型的值直接保留
                alpha_is: dict[str, dict] = alphas_p["is"]
                result.update({i: alpha_is[i] for i in alpha_is if isinstance(
                    alpha_is[i], (float, str, int))})
                # 保存checks中的详细数据
                no_check = ["LOW_SHARPE", "LOW_FITNESS",
                            "HIGH_TURNOVER", "LOW_TURNOVER"]
                checks = {i["name"]: round(i.get("value", -4) - i.get("limit"), 4) for i in alphas_p["is"]["checks"]
                        if ("limit" in i and i.get("name") not in no_check)}
                if "CONCENTRATED_WEIGHT" in checks:
                    for con in alphas_p["is"]["checks"]:
                        if con["result"] == "FAIL" and "limit" not in con:
                            checks["CONCENTRATED_WEIGHT"] = -1
                            break
                # 暂定2y sharpe保留为原始值
                result.update(checks)

                # 保存其他项的sharpe和fitness
                _fit = {i+"_fitness": alpha_is[i].get("fitness")
                        for i in alpha_is if isinstance(alpha_is[i], dict)}
                _sharpe = {i+"_sharpe": alpha_is[i].get("fitness")
                        for i in alpha_is if isinstance(alpha_is[i], dict)}
                result.update(_fit)
                result.update(_sharpe)
                result.update(alphas_p["settings"])
                result["settings"] = alphas_p["settings"]
                # 删除settings中不需要的字段
                del result["settings"]["startDate"]
                del result["settings"]["endDate"]
                result["dateCreated"] = alphas_p["dateCreated"]
                result["margin"] = result["margin"]*1e4
                arr.append(result)
                # print(arr)
            case_db.insert_case(arr)
    
    def prod_corr_check(self, alpha_id):
        url = "https://api.worldquantbrain.com/alphas/" + alpha_id + "/corrrelations/prod"
        result = self.wait_get(url)
        try:
            result = self.wait_get(url).json()
            print(alpha_id, result["max"], result["min"])
            return result["max"]
        except Exception as e:
            print(e)
            sleep(1)
        return None

    def zero_sharpe_count(self, alpha_list: list[str], sharpe: int = 0, count: int = 1, use_abs: bool = True) -> pd.DataFrame:

        # """"
        # alpha_list: alpha list
        # sharpe：不超过abs(sharpe)
        # count: 最大多少年sharpe小于abs(sharpe),默认值1 year
        # 传入alpha id list 返回有满足
        # use_abs:是否使用绝对值
        # """
        # if type(sess) == type(None):
        #         cfg.sess = sign_in(cfg.username, cfg.password)
        # else:
        #         cfg.sess=sess
        def get_sharpe(usharpe, uabs):
            return abs(usharpe) if uabs else usharpe
        with ThreadPoolExecutor(max_workers=10) as executor:
            #   多线程获取alpha 每年year数据
            results = executor.map(
                lambda alpha_id: self.get_all_year_sharpe(alpha_id), alpha_list)
        df = pd.concat(list(results), axis=0)
        df["zeroCount"] = df.apply(lambda x: len(
            [i for i in x if get_sharpe(i, use_abs) <= sharpe]), axis=1)
        print(df)
        print(df.columns)
        df.reset_index(inplace=True)
        df.apply(lambda x: case_db.update_case_db(
            x["index"], {"zeroCount": x["zeroCount"]}), axis=1)
        self.sess.close()
        return df["index"].values.tolist()

    def wait_get(self, url: str, max_retries: int = 100):
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
            try:
                while True:
                    simulation_progress = self.sess.get(url)
                    if simulation_progress.status_code == 401:
                        # and simulation_progress.json().get('detail') ==  'Incorrect authentication credentials.':
                        self.login()
                        continue
                    if simulation_progress.headers.get("Retry-After", 0) == 0:
                        break
                    time.sleep(
                        float(simulation_progress.headers["Retry-After"]))
                if simulation_progress.status_code < 400:
                    break
                else:
                    print(url, simulation_progress.status_code)
                    time.sleep(4)
                    retries += 1
            except:
                traceback.print_exc()
                # self.sess.close()
                self.login()
        # print(simulation_progress.json())
        return simulation_progress

    def wait_post(self, url: str, data: list, retry: int = 300):
        i = 0
        while i < retry:
            try:
                i += 1
                sim = self.sess.post(url, json=data, timeout=15)
                if sim.status_code == 401:
                    self.login()
                    continue
                elif sim.status_code < 299:
                    return sim
                elif sim.status_code == 429:
                    print("POST状态: 429")
                    sleep(15)
                    continue
                print("POST失败")
                return None
            except:
                traceback.print_exc()
                self.login()
                continue

    def get_all_year_sharpe(self, alpha_id: str) -> pd.DataFrame:
        """
        传入id值，返回index为年份， col为id的DataFrame
        """
        print(f"开始下载:{alpha_id}")
        result = self.wait_get("https://api.worldquantbrain.com/alphas/" +
                               alpha_id + "/recordsets/yearly-stats").json()
        # result['records'] 0为年份，6为sharpe
        df = pd.DataFrame([{records[0]: records[6]
                          for records in result['records']}], index=[alpha_id])
        return df

    def save_alpha_info(self, s_id: str, childrens: list) -> pd.DataFrame:
        # 根据children 获取alpha id，
        # 根据id获取详细数据，保存至case表中
        # 更新状态
        lock_alpha_result.acquire()
        # self.save_alpha_info(url.split("/")[-1], childrens=children)

        def childrens_to_alpha_details(children):
            alphaid = children
            if len(children)>10:
                url = "https://api.worldquantbrain.com/simulations/"
                alphaid = self.wait_get(url + children).json().get("alpha")
            if alphaid:
                return self.deal_single_alpha_result(alphaid)
            case_db.insert_case([{"id": alphaid}])
        # get alpha id
        with ThreadPoolExecutor(max_workers=10) as executor:
            details = executor.map(
                lambda x: childrens_to_alpha_details(x), childrens)
        # 生成alpha id的列表
        # save_db.insert_db(cfg,details)
        # case时待处理是否满足标准
        # print(list(details))
        case_db.insert_case(list(details))
        case_db.update_simulations_status(s_id, True)
        lock_alpha_result.release()

    def finall_save_db(self, ):
        # 获取未保存的id表
        no_save = case_db.get_simulations_status()[1]
        self.login()
        # print(no_save)
        url = "https://api.worldquantbrain.com/simulations/"
        for i in no_save:
            childrens = self.wait_get(
                url + i[0]).json().get("children") or [i[0]]
            # print(i[0], childrens)
            self.save_alpha_info(i[0], childrens)
            case_db.update_simulations_status(i[0], True)
            # exit(0)

    def sub(self, index, df: pd.DataFrame, retry: int = 300):
        '''提交回测'''
        try:
            lock_submit.acquire()
            df = df.copy(deep=True)
            
            df["type"] = yamldata.type
            if yamldata.type == "SUPER":
                df.rename(columns={"code": "combo"}, inplace=True)
                ids = eval(df.loc[df.index[0]]["idlist"])
                self.set_color(ids, "PURPLE")
                if "combo" not in df.columns:
                    df["combo"] = yamldata.combo
                if "selection" not in df.columns:
                    df["selection"] = yamldata.selection
                df = df[["type", "combo","selection", "settings"]]
            alpha_list = [df.loc[i].to_dict() for i in df.index]
            print(alpha_list[0])
            if len(alpha_list) == 1:
                alpha_list = alpha_list[0]
            result = self.wait_post(
                'https://api.worldquantbrain.com/simulations', data=alpha_list)
            if result is None:
                return
            url = result.headers['Location']
            log.log(url)
            case_db.insert_simulations_id(url.split("/")[-1])
            save_status(index + yamldata.one_slot_number)
            print(url, "submit success!!!")
            # self.set_color(ids, None)
            lock_submit.release()
            self.check_sub(url, ids)
        except:
            traceback.print_exc()

    def check_sub(self, url: str, ids:str =""):
        # 检查回测状态
        try:
            result = self.wait_get(url)
            sts = result.json().get("status")
            if sts == "ERROR":
                log.log(f"status ERROR: {url}")
                print(result.json())
                case_db.update_simulations_status(url.split("/")[-1], True)
                return True
            children = result.json().get("alpha") or [
                url.split("/")[-1]]   # 获取alpha id
            if isinstance(children, str):
                children = [children]
            self.que.put((url, children))
            print(children, ids)
            return True
        except:
            traceback.print_exc()
            return True

    def subs(self, df: pd.DataFrame):
        def single_sub(x): return self.sub(
            x, df[x: x + yamldata.one_slot_number])
        with ThreadPoolExecutor(max_workers=yamldata.slots_counts) as extor:
            extor.map(single_sub, [i for i in range(
            status.current.index,  df.shape[0], yamldata.one_slot_number)])

    def saves(self, sub: threading.Thread):
        print(" saves is startings...")
        while sub.is_alive() or not self.que.empty():
            if self.que.empty():
                print("waiting queue, sleep 120s !!!")
                sleep(120)
                continue
            data = self.que.get()
            self.save_alpha_info(data[0], data[1])

    def muti(self, df: pd.DataFrame):
        '''可增加到最大槽位'''
        print(df)
        self.simed = True
        print("测试总数", df.shape[0])
        self.login()
        sub = threading.Thread(target= self.subs, args=(df, ))
        sub.start()
        saves = threading.Thread(target= self.saves, args=(sub,))
        saves.start()
        saves.join()

    def _get_alpha_pnl(self, alpha_id: str) -> pd.DataFrame:
        """
        获取指定 alpha 的 PnL数据，并返回一个包含日期和 PnL 的 DataFrame。
        Args:
                alpha_id (str): Alpha 的唯一标识符。
        Returns:
                pd.DataFrame: 包含日期和对应 PnL 数据的 DataFrame，列名为 'Date' 和 alpha_id。
        """
        save_path = cfg.check_path+f"pnls/{alpha_id}.pkl"
        if os.path.exists(save_path):
            return pd.read_pickle(save_path)
        print(f"开始下载:{alpha_id}")
        pnl = self.wait_get(
            "https://api.worldquantbrain.com/alphas/" + alpha_id + "/recordsets/pnl").json()
        df = pd.DataFrame(pnl['records'], columns=[item['name']
                        for item in pnl['schema']['properties']])
        df = df.rename(columns={'date': 'Date', 'pnl': alpha_id})
        df = df[['Date', alpha_id]]
        # 最后一个值为负时，反转pnls值
        if df.loc[df.index[-1]][alpha_id] < 0:
            df[alpha_id] = - df[alpha_id]
        df.to_pickle(save_path)
        return df

    def get_list(self, df: pd.DataFrame, max_corr=0.95):
        finally_alphas = []

        def get_llll(df: pd.DataFrame, arr=[], father: list = []):
            if len(arr) <= 1:
                finally_alphas.append(father+arr)
                return None
            # 选择第一个

            get_llll(df, [i for i in arr[1:] if df.loc[arr[0]]
                     [i] <= max_corr], father+arr[:1])
            # 不选择第一个
            #     if father:
            #         get_llll(df, arr[1:], father)
        alpha_ids = df.columns.to_list()
        get_llll(df, alpha_ids, [])
        print(finally_alphas)
        return finally_alphas

    def corr_check(self, df: pd.DataFrame, max_corr=0.95):
        '''
        传入alpha-ids列表返回，list[df]
        id对应的pnl列
        '''
        alpha_ids = df["id"].values.tolist()

        def alpha_res_func(alpha_id): return self._get_alpha_pnl(
            alpha_id).set_index('Date')
        with ThreadPoolExecutor(max_workers=10) as executor:
            alpha_results = executor.map(alpha_res_func, alpha_ids)
        alpha_results = list(alpha_results)
        is_pnls = pd.concat(alpha_results, axis=1)
        print(is_pnls)
        # 算出增量，后一行减前一行的值
        is_rets = is_pnls - is_pnls.ffill().shift(1)
        # 保留四年时间的增量pnls
        is_rets = is_rets[pd.to_datetime(is_rets.index) > pd.to_datetime(
            is_rets.index).max() - pd.DateOffset(years=4)]
        df_corr = is_rets.corr()
        check = self.get_list(df=df_corr, max_corr=max_corr)[0]
        return df[df["id"].isin(check)]
    def set_color(self, ids: list, color:str):
        set_none = [i for i in self.ids if i not in ids]
        set_c = [i for i in ids if i not in self.ids]
        self.ids = ids
        for i in set_c:
            print(i, color)
            url = f"https://api.worldquantbrain.com/alphas/{i}"
            while True:
                res = self.sess.patch(url, json={"color": color})
                if res.status_code == 429:
                    print(res.json())
                    time.sleep(30)
                    continue
                time.sleep(1)
                break
        for i in set_none:
            print(i, color)
            url = f"https://api.worldquantbrain.com/alphas/{i}"
            while True:
                res = self.sess.patch(url, json={"color": None})
            
                if res.status_code == 429:
                    print(res.json())
                    time.sleep(30)
                    continue
                time.sleep(1)
                break

