import os
import sys
import threading
import yaml
import datetime
import pytz
import pandas as pd
import data
from loger import logs
import loger
from model import cfg, load_status, save_status, yamldata, current_model, status
import case_db
from supert import quant
# import selfcheck
project = sys.argv[1]

#project = "case2"


def load_yaml(project):
    with open(cfg.path + f'case/{project}.yaml', 'r', encoding='utf-8') as f:
        yldata = yaml.load(f.read(), Loader=yaml.Loader)
        cases = {i: cas for i, cas in enumerate(yldata["cases"])}
        yldata["cases"] = cases
    return yldata


def reload_cfg():
    cfg.test_path = os.path.join(cfg.path, "status", yamldata.data_name+"/", )
    print(cfg.test_path, cfg.log_name)
    cfg.log_name = cfg.test_path + cfg.log_name
    cfg.status_name = cfg.test_path + cfg.status_name  # 过程状态文件
    cfg.deal_data = cfg.test_path + cfg.deal_data  # deal之后的文件
    cfg.result = cfg.test_path + cfg.result  # 回测结果文件


def get_time() -> str:
    now = datetime.datetime.now(pytz.timezone('Asia/Shanghai'))
    return now.isoformat().split(".")[0]

def update_status():
    status.current.end_time = get_time()
    status.current.finish = True
    status.total[status.current.case] = status.current.model_dump()
    status.current = current_model()
    save_status(index=0)

def init_status(lens, func):
    status.current.total = lens
    # 开始时间
    if not status.current.start_time:
        status.current.start_time = get_time()
    status.current.name = func
    loger.log.log(str(cfg.status_name))



def init_db():
    # 初始化用例db
    case_db.cfg.case_result_db = yamldata.data_name.replace("-", "_")+"_result"
    case_db.cfg.simulations_db_name = yamldata.data_name.replace(
        "-", "_")+"_simulations"
    print(case_db.create_case())
    print(case_db.create_simulations())


def init_yamldata(project):
    yldata = load_yaml(project)
    yamldata.data_name = yldata["data_name"]
    yamldata.type = yldata["type"]
    yamldata.settings = yldata["settings"]
    yamldata.cases = yldata["cases"]
    yamldata.slots_counts = yldata.get("slots_counts", 8)
    yamldata.one_slot_number = yldata.get("one_slot_number", 10)
    yamldata.para = yldata.get("para", {})





def read_data():
    print(cfg.deal_data)
    if os.path.exists(cfg.deal_data):
        print("存在，续测")
        df = pd.read_json(cfg.deal_data)
        return df
    else:
        df = pd.read_csv(cfg.test_path + "data.csv")
        # df = df[start: end]
        df["code"] = df["code"].apply(lambda x: "  " + x + "  ")
    print(yamldata.settings)
    df["settings"] = df.apply(lambda x:  yamldata.settings)
    arr = []
    print(df)
    for i in df.index:
        js = df.loc[i].to_dict()
        js["settings"] = yamldata.settings
        arr.append(js)
    # print(df)
    return pd.DataFrame(arr)

def one_cases(func, df: pd.DataFrame, para: dict = {}):
    # 组装生成alphas
    print(func)
    arrs: pd.DataFrame = getattr(data, func)(df, para)
    print(len(arrs))
    init_status(arrs.shape[0], func)
    qua = quant()
    #qua.get_all_alpha()
    #exit()
    if  para.get("no_sumi"):
        arrs.to_json(cfg.deal_data)
        update_status() 
        return arrs
    qua.login()
    th = threading.Thread(target=qua.finall_save_db)
    th.start()
    qua.muti(arrs)
    th.join()
    qua.finall_save_db()
    df = data.deal_data(df, sharpe=para.get(
      "sharpe", 0.9),fitness=para.get(
      "fitness", 0.1), save_file=cfg.test_path+func+str(status.current.case)+".csv", case_df=cfg.deal_data)
    # 执行回测
    df.to_csv(cfg.test_path + status.current.name +f"{status.current.case}.csv")
    qua.login()
    if "zero_sharpe_check" in para:
        print(".....zero check.....")
        loger.log.log(f"zero count: {df.shape[0]}")
        zero = qua.zero_sharpe_count(df["id"].to_list())
        df = df[df["id"].isin(zero)]
        print(".....zero check.....")
        print(df)
    df = qua.corr_check(df, max_corr=para.get("max_corr", 0.9))
    df.to_json(cfg.deal_data)
    update_status()
    return df

def init_cfg(project):
    # 加载yaml
    init_yamldata(project)
    print("yaml load finished.")
    # 重赋值cfg
    reload_cfg()
    # if not os.path.exists("{cfg.test_path}{project}.yaml"):
    os.system(f"cp -f {cfg.path }/case/{project}.yaml {cfg.test_path}")
    # 创建路径
    os.makedirs(cfg.test_path, exist_ok=True)
    cfg.project_path = os.path.join(
        cfg.path + yamldata.data_name.split(".")[0])
    # 初始化DB
    init_db()
    # 读取测试状态
    if os.path.exists(cfg.status_name):
        load_status()
    loger.log.log = logs(cfg.log_name)
    # 加载数据
    df = read_data()
    # exit(0)
    for i in yamldata.cases:
        func = yamldata.cases[i]["name"]
        para = yamldata.cases[i]["para"]
        yamldata.para = para
        total = status.total
        print(total)
        if total.get(str(i)):
            continue
        status.current.case = i
        df = one_cases(func, df=df, para=para)
    return df

df = init_cfg(project)

