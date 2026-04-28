import os
import argparse
import threading
import yaml
import datetime
import pytz
import pandas as pd
import data
from loger import logs
import loger
from model import cfg, load_status, save_status, current_model, status, yamldataModel, Super_settings
import model
import case_db
from api import quant
import save_db
import deal_result


def load_yaml(project):
    with open(cfg.path + f'case/{project}.yaml', 'r', encoding='utf-8') as f:
        yldata = yaml.load(f.read(), Loader=yaml.Loader)
        cases = {i: cas for i, cas in enumerate(yldata["cases"])}
        yldata["cases"] = cases
    print(yldata)
    return yldata


def load_test_path_file(data_name):
    cfg.test_path = os.path.join(cfg.path, "status", data_name+"/", )
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
    case_db.cfg.case_result_db = model.yamldata.data_name.replace(
        "-", "_")+"_result"
    case_db.cfg.simulations_db_name = model.yamldata.data_name.replace(
        "-", "_")+"_simulations"
    print(case_db.create_case())
    print(case_db.create_simulations())


def init_yamldata(project: str, expore=False, data_name=""):
    model.yamldata = model.yamldataModel()
    yldata = load_yaml(project)
    model.yamldata = yamldataModel.model_validate(yldata)
    model.yamldata.para = yldata.get("para", {})
    if expore:
        print("当前数据集名称", data_name)
        model.yamldata.data_name = data_name
        model.yamldata.settings.region = data_name.split("-")[0]
        model.yamldata.settings.universe = data_name.split("-")[2]
    if model.yamldata.type == "SUPER":
        model.yamldata.settings = Super_settings().model_validate(yldata["settings"])


def read_data(enhance: str = ""):
    print(cfg.deal_data)
    if enhance:
        deal_data = cfg.deal_data.split(".")
        deal_data.insert(-1, "1")
        cfg.deal_data = ".".join(deal_data)
        print(cfg.deal_data)
        qua = quant()
        qua.login()
        return deal_result.deal_data(pd.DataFrame(qua.deal_single_alpha_result(enhance)))

    if os.path.exists(cfg.deal_data):
        print("存在，续测")
        df = pd.read_json(cfg.deal_data)
        return df
    else:
        df = pd.read_csv(cfg.test_path + "data.csv")
        # df = df[start: end]
        df["code"] = df["code"].apply(lambda x: "  " + x + "  ")
    print(model.yamldata.settings)
    df["settings"] = df.apply(lambda x:  model.yamldata.settings)
    arr = []
    print(model.yamldata.settings)
    for i in df.index:
        js = df.loc[i].to_dict()
        js["settings"] = model.yamldata.settings.model_dump()
        arr.append(js)
    # print(df)
    return pd.DataFrame(arr)


def one_cases(func: str, df: pd.DataFrame, para: dict = {}):
    # 组装生成alphas
    print("当前回测项名称", func)

    arrs = pd.DataFrame()
    for i in func.split("&"):
        arrs = pd.concat([arrs, getattr(data, func)(df, )])
    print("总的回测数量", len(arrs))
    init_status(arrs.shape[0], func)
    qua = quant()
    if para.get("no_sumi"):
        arrs.to_json(cfg.deal_data)
        update_status()
        return arrs
    qua.login()
    # 保存因中断回测而未保存的值
    th = threading.Thread(target=qua.finall_save_db)
    th.start()
    # 多线程回测
    qua.mutis(arrs)
    th.join()
    qua.finall_save_db()
    df = deal_result.deal_data(df, sharpe=para.get("sharpe", 0.9),
                               fitness=para.get("fitness", 0.1))
    qua.login()
    df = qua.corr_check(df, max_corr=para.get("max_corr", 0.95))
    df.to_json(cfg.deal_data)
    update_status()
    return df


def init_cfg(project: str = "USA-s", expore=False, data_name: str = "", enhance=""):
    # 加载yaml
    save_db.inin_database(data_name.split("-")[0]
                          if data_name else project.split("-")[0])
    
    # 初始化配置文件
    init_yamldata(project, expore, data_name=data_name)
    print("yaml load finished.")
    yamldata = model.yamldata
    # 加载配置文件
    load_test_path_file(yamldata.data_name)
    cfg.project_path = os.path.join(
        cfg.path + yamldata.data_name.split(".")[0])
    # 初始化DB，包含状态文件和结果文件
    init_db()
    # 读取测试状态
    if os.path.exists(cfg.status_name):
        load_status()
    loger.log.log = logs(cfg.log_name)
    # 读取数据data.csv
    df = read_data(enhance)
    if enhance:
        yamldata.cases = [{"name": "fine_tune", "para": {}}]
    for i in yamldata.cases:
        func = yamldata.cases[i]["name"]  # case的名称如group_second
        para = yamldata.cases[i]["para"]  # case的参数
        model.yamldata.para = para
        total = status.total
        print(total)
        if total.get(str(i)):
            continue
        status.current.case = i
        df = one_cases(func, df=df, para=para)  # 每一回测项的具体执行逻辑
    return df


def delete():
    # 用于删除正在回测的状态
    qua = quant()
    qua.login()
    import requests
    sess: requests.Session = qua.sess
    ids = '''xxxxxx'''.split("\n")
    for i in ids:
        sess.delete(f"https://api.worldquantbrain.com/simulations/{i}")


if __name__ == "__main__":
    # delete()
    parser = argparse.ArgumentParser(prog="wq", description="参数解析")
    parser.add_argument("-e", "--explore", default=False,
                        type=bool, help="是否启用探索模式")
    parser.add_argument("--region", "-r", type=str, help="地区")
    parser.add_argument("--universe", "-u", type=str, help="universe")
    parser.add_argument("--dataset", "-d", type=str, help="数据集名称")
    parser.add_argument("--delay", type=int, help="delay")
    parser.add_argument("--case", "-c", type=str, help="case名称")
    parser.add_argument("--enhance", type=str, help="增加操作符调优传入alphaid")
    args = parser.parse_args()
    if args.enhance:
        exit(init_cfg(args.case, enhance=args.enhance), )
        pass
    if args.explore:
        # eg: data_name: USA-1-ILLIQUID_MINVOL1M-model165-dh4
        data_name = f"{args.region}-1-{args.universe}-{args.dataset}-a4"
        print(data_name)
        # 不存在则下载
        if not os.path.exists(f"../status/{args.region}-1-{args.universe}-{args.dataset}-a4"):
            os.system(
                f"python load_data.py {args.region} {args.universe} {args.dataset} a4")

        init_cfg(project="expore-s", expore=True, data_name=data_name)
    else:
        project = args.case
        print(project)
        df = init_cfg(project)
