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
import model
from model import cfg, save_status, status
from concurrent.futures import ThreadPoolExecutor
import threading
import save_db
from case_db import cfg as dbcfg

lock_alpha_result = threading.Lock()


class quant:
    def __init__(self) -> None:
        self.arr = {}
        self.count = 0
        self.que = queue.Queue()
        self.save_lock = threading.Lock()
        self.sess = requests.Session()

    def login(self,):
        with open(cfg.path + 'brain.txt') as f:
            self.username, self.password = json.load(f)
        self.sess.auth = HTTPBasicAuth(self.username, self.password)
        # self.sess.close()
        response = self.sess.post(
            'https://api.worldquantbrain.com/authentication')
        print(response.status_code)

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
                retries += 1
                simulation_progress = self.sess.get(url)
                if simulation_progress.status_code == 401:
                    self.login()
                    continue
                if simulation_progress.headers.get("Retry-After", 0) == 0 and simulation_progress.status_code < 300 or simulation_progress.status_code == 404:
                    break

                time.sleep(2.5)
            except Exception as e:
                traceback.print_exc()
        return simulation_progress

    def wait_post(self, url: str, data: list, retry: int = 300):
        i = 0
        while i < retry:
            try:
                i += 1
                sim = self.sess.post(url, json=data, timeout=60)
                print("POST STATUS:", sim.status_code)
                if sim.status_code == 401:
                    self.login()
                    continue
                elif sim.status_code == 429:
                    sleep(5)
                    continue
                elif sim.status_code < 299:
                    return sim
                log.log("POST FAIL!!!")
                return None
            except:
                traceback.print_exc()
                time.sleep(5)
                continue

    def deal_requests(self,):
        pass

    def deal_single_alpha_result(self, alpha_id) -> dict:
        '''传入获取的数量和开始的时间参数
        返回df'''
        # # 2025-07-04T22:52:30
        # url = "https://api.worldquantbrain.com/users/self/alphas?limit=100&offset=%d"%(i) \
        #             + "&status=UNSUBMITTED%1FIS_FAIL&" + \
        #     "dateCreated%3E=2026-01-09T4:09:00-05:00&&type=REGULAR&settings.region=USA" #is.sharpe

        url = f"https://api.worldquantbrain.com/alphas/{alpha_id}"
        log.log(url)
        alphas_p = self.wait_get(url).json()
        if not alphas_p:
            return
        result = dict()
        result["id"] = alphas_p["id"]
        # 获取表达式
        result["code"] = alphas_p["regular"]["code"]
        # 检查checks中是否有fail
        result["result"] = False if [i
                                     for i in alphas_p["is"]["checks"] if i.get("result") == "FAIL"] else True
        # is中单keys数据类型的值直接保留
        alpha_is: dict[str, dict] = alphas_p["is"]
        result.update({i: alpha_is[i] for i in alpha_is if isinstance(
            alpha_is[i], (float, str, int))})
        # 保存checks中的详细数据
        # no_check = ["LOW_SHARPE", "LOW_FITNESS",
        #             "HIGH_TURNOVER", "LOW_TURNOVER"]
        checks = {i["name"].split(".")[0]: round(i.get("value", -4) - i.get("limit"), 4) for i in alphas_p["is"]["checks"]
                  if "limit" in i}
        # 暂定2y sharpe保留为原始值
        result.update(checks)
        result["WARNING"] = len(
            [i for i in alphas_p["is"]["checks"] if i.get("result") == "WARNING"])
        # 保存其他项的sharpe和fitness
        _fit = {i+"_fitness": alpha_is[i].get("fitness")
                for i in alpha_is if isinstance(alpha_is[i], dict)}
        _sharpe = {i+"_sharpe": alpha_is[i].get("sharpe")
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

    def prod_corr_check(self, alpha_id):
        url = "https://api.worldquantbrain.com/alphas/" + alpha_id + "/correlations/prod"
        result = self.wait_get(url)
        print(url)
        try:
            result = self.wait_get(url).json()
            # print(result)
            print(alpha_id, result["max"], result["min"])
            return result["max"]
        except Exception as e:
            traceback.print_exc()
            print(e)
            sleep(1)
        return None

    def zero_sharpe_count(self, alpha_list: list[str], sharpe: int = 0, count: int = 1, use_abs: bool = True) -> pd.DataFrame:
        with ThreadPoolExecutor(max_workers=10) as executor:
            #   多线程获取alpha 每年year数据
            results = executor.map(
                lambda alpha_id: self.get_zero_sharpe(alpha_id), alpha_list)
        df = pd.DataFrame(list(results),)
        self.sess.close()
        df = df[df["zeroCount"] < 5]
        return df["id"].values.tolist()

    def get_zero_sharpe(self, alpha_id: str) -> dict:
        """
        传入id值，返回index为年份， col为id的DataFrame
        """
        print(f"download year data:{alpha_id}")
        result = self.wait_get("https://api.worldquantbrain.com/alphas/" +
                               alpha_id + "/recordsets/yearly-stats").json()
        zc = [records[6] for records in result['records']].count(0)
        if [records[6] for records in result['records']][-1] == 0:
            zc = 12
        case_db.update_case_db(alpha_id, {"zeroCount": zc})
        return {"id": alpha_id, "zeroCount": zc}


    def childrens_to_alpha_details(self, children):
        alphaid = children
        # 根据长度判断是alpha id还是child
        if len(children) > 10:
            for i in range(50):
                url = "https://api.worldquantbrain.com/simulations/"
                result = self.wait_get(url + children).json()
                alphaid = result.get("alpha")
                if alphaid:
                    break
                if result.get("status") not in ["FAIL", "ERROR"]:
                    print(i, children, "get alpha id fail")
                    time.sleep(5)
                return
        if not alphaid:
            log.log(f"{children} get alpha rsult fail!!!")
            return None
            # 获取回测结果数据
        detail = self.deal_single_alpha_result(alphaid)
        if not detail["id"]:
            log.log(detail)
            log.log(f"{children} get alpha rsult fail!!!")
            return
        case_db.insert_case([detail])
        if max(abs(detail.get("sharpe", 0)), abs(detail.get("investabilityConstrained_sharpe", 0))) \
            > model.yamldata.para.get("sharpe", 1):
            # 获取yeardata数据
            self.get_zero_sharpe(alphaid)
            # update
            # 下载pnl数据
            self._get_alpha_pnl(alphaid)
            pass
        return detail

    def save_alpha_info(self, s_id: str, childrens: list) -> pd.DataFrame:
        # 根据childrens 的长度 获取alpha id，如果是alpha id时直接获取其值
        # 根据id获取详细数据，保存至case表中
        # 更新状态
        lock_alpha_result.acquire()
        with ThreadPoolExecutor(max_workers=10) as executor:
            details = executor.map(
                lambda x: self.childrens_to_alpha_details(x), childrens)
        case_db.update_simulations_status(s_id, True)
        lock_alpha_result.release()

    def finall_save_db(self, ):
        # 获取未保存的id表
        no_save = case_db.get_simulations_status()[1]
        self.login()
        print(no_save)

        url = "https://api.worldquantbrain.com/simulations/"
        for i in no_save:
            childrens = self.wait_get(
                url + i[0]).json().get("children") or [i[0]]
            self.save_alpha_info(i[0], childrens)
            case_db.update_simulations_status(i[0], True)

    def sub(self, index, df: pd.DataFrame, retry: int = 300):
        '''提交回测'''
        try:
            df = df.copy(deep=True)
            df["regular"] = df["code"]
            df["type"] = model.yamldata.type
            df = df[["type", "regular", "settings"]]
            alpha_list = [df.loc[i].to_dict() for i in df.index]
            log.log(str(alpha_list[0]))
            if len(alpha_list) == 1:
                alpha_list = alpha_list[0]
            # bug提交等待时未保存到数据库
            result = self.wait_post(
                'https://api.worldquantbrain.com/simulations', data=alpha_list)
            print(result)
            if result is None:
                print("当前result为None")
                raise

            url = result.headers['Location']
            log.log(url)
            case_db.insert_simulations_id(url.split("/")[-1])
            save_status(index + model.yamldata.one_slot_number)
            log.log(url + " submit success!!!")
            remaining = float(result.headers.get(
                "X-ratelimit-remaining", 5000))
            reset = result.headers.get("X-ratelimit-reset")
            log.log(
                f'limit: {result.headers.get("X-ratelimit-limit")}, remaining: {remaining}, reset: {reset}')
            reset_time = float(result.headers.get("X-ratelimit-reset", 500))
            if not self.check_sub(url, alplist=alpha_list):
                print("重新回测", index, df)
                self.sub(index, df)
            if remaining < 1:
                log.log("休息时长为"+str(reset_time + 3600))
                time.sleep(reset_time + 3600)
            if remaining > 200 or reset_time < 2000:
                return True
            time.sleep(reset_time - 2000)

        except:
            print("xxxxxxxxxxxxxxxx")
            time.sleep(10)
            traceback.print_exc()

    def check_sub(self, url: str, alplist=[]):
        # 检查回测状态
        try:
            for i in range(10):
                result = self.wait_get(url)
                sts = result.json().get("status")
                if not sts:
                    continue
                if sts == "ERROR":
                    log.log(f"status ERROR: {url}")
                    case_db.update_simulations_status(url.split("/")[-1], True)
                    u = "https://api.worldquantbrain.com/simulations/"
                    errmsg = None
                    for err, alp in zip(result.json().get("children"), alplist):
                        message = self.wait_get(u+err).json().get("message")
                        if message:
                            errmsg = message
                            log.log(message + str(alp))
                    if not errmsg:
                        return False

                print("回测完成", url)
                children = result.json().get("children") or [
                    url.split("/")[-1]]   # 获取alpha id
                self.que.put((url.split("/")[-1], children))
                return True
        except:
            traceback.print_exc()
            return True

    # def subs(self, df: pd.DataFrame):

    #     single_sub = lambda x, df: self.sub(  x, df[x: x + model.yamldata.one_slot_number])
    #     with ThreadPoolExecutor(max_workers=model.yamldata.slots_counts) as extor:
    #         extor.map(single_sub, [i for i in range(
    #         status.current.index,  df.shape[0], model.yamldata.one_slot_number)])

    # def saves(self, sub: threading.Thread):
    #     print(" saves is startings...")
    #     while sub.is_alive() or not self.que.empty():
    #         if self.que.empty():
    #             print("waiting queue, sleep 120s !!!")
    #             sleep(120)
    #             continue
    #         data = self.que.get()
    #         self.save_alpha_info(data[0], data[1])
    #         case_db.update_simulations_status(data[0], True)
    #         log.log(data[0]+ " finished!!!")
    def saveresult(self):
        print(" saves is startings...")

        while self.save_flag or not self.que.empty():
            if self.que.empty():
                print("waiting queue, sleep 120s !!!")
                sleep(120)
                continue
            data = self.que.get(timeout=5)
            self.save_alpha_info(data[0], data[1])
            case_db.update_simulations_status(data[0], True)
            log.log(data[0] + "save finished!!!")

    def mutis(self, df: pd.DataFrame):
        self.save_flag = True
        print("测试总数", df.shape[0])
        self.login()
        saves = threading.Thread(target=self.saveresult)
        saves.start()
        def single_sub(x): return self.sub(
            x, df=df[x: x + model.yamldata.one_slot_number])
        print("当前位置", status.current)
        log.log(f"{model.yamldata.slots_counts}, {model.yamldata.one_slot_number}")
        with ThreadPoolExecutor(max_workers=min(model.yamldata.slots_counts, 2)) as extor:
            extor.map(single_sub, [i for i in range(
                status.current.index,  df.shape[0], model.yamldata.one_slot_number)])
        self.save_flag = False
        saves.join()

    # def muti(self, df: pd.DataFrame):
    #     '''可增加到最大槽位'''
    #     print(df)
    #     self.simed = True
    #     print("测试总数", df.shape[0])
    #     self.login()
    #     sub = threading.Thread(target= self.subs, args=(df, ))
    #     sub.start()
    #     saves = threading.Thread(target= self.saves, args=(sub,))
    #     saves.start()
    #     saves.join()

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
        print(f"Download pnl: {alpha_id}")
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
        # 传入index和col 名均为alpha idde矩阵
        result = []
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

    def corr_check(self, df: pd.DataFrame, max_corr=0.95):
        '''
        传入alpha-ids列表返回，list[df]
        id对应的pnl列
        '''
        alpha_ids = df["id"].values.tolist()
        print(len(alpha_ids))
        alpha_res_func = lambda x: self._get_alpha_pnl(
            x).set_index('Date')
        with ThreadPoolExecutor(max_workers=10) as executor:
            alpha_results = executor.map(alpha_res_func, alpha_ids)
        alpha_results = list(alpha_results)
        is_pnls = pd.concat(alpha_results, axis=1)
        print(is_pnls)
        # 算出增量，后一行减前一行的值
        is_pnls -= is_pnls.ffill().shift(1)
        # 保留四年时间的增量pnls
        is_pnls = is_pnls[pd.to_datetime(is_pnls.index) > pd.to_datetime(
            is_pnls.index).max() - pd.DateOffset(years=4)]
        is_pnls = is_pnls.corr()
        check = self.get_list(df=is_pnls, max_corr=max_corr)
        return df[df["id"].isin(check)]
