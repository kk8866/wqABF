import pandas as pd
import model
"""
组成aplha的方法
"""


def ff(df: pd.DataFrame):
    return df


def ts_first(df: pd.DataFrame, expore=False) -> pd.DataFrame:
    days = [66]
    ts_ops = ["ts_product"]
    if expore:  # 探索模式的时候走全量搜索
        days = [21, 66]
        ts_ops = ["-ts_std_dev", "ts_rank",  "ts_quantile",
                  "ts_product",  "ts_kurtosis", ]
    arr = []
    for i in df.index:
        for day in days:
            for op in ts_ops:
                arr.append(
                    {"code": f'{op}({df.loc[i]["code"]}, {day})', "settings": df.loc[i]["settings"]})
    return pd.DataFrame(arr)


def ts_m(df: pd.DataFrame, expore=False) -> pd.DataFrame:
    days = [21, 66]
    ts_ops = [
        "ts_mean", "ts_sum", "ts_max",  "ts_min", "ts_median", ]
    if expore:
        days = [21, 63, 126]
        ts_ops = ["ts_mean", "ts_sum", "ts_max",  "ts_min",   "ts_median",]
    arr = []
    for i in df.index:
        for day in days:
            for op in ts_ops:
                arr.append(
                    {"code": f'{op}({df.loc[i]["code"]}, {day})', "settings": df.loc[i]["settings"]})
    return pd.DataFrame(arr)


def group_second(df: pd.DataFrame):
    """group增强信号"""
    para: dict = model.yamldata.para
    group_ops = ["group_rank", "group_zscore",
                 "group_scale", "group_neutralize", "group_normalize",]
    fieds = ["industry", "subindustry",
             "exchange", "market", "sector", "currency"]
    group_ops = para.get("ops") if para.get("ops") else group_ops
    if df.loc[df.index[0], "settings"]["region"] in ["GLB", "EUR", "ASI"]:
        fieds.append("country")
    #     group_datas += ["group_cartesian_product(country, industry)",
    # "group_cartesian_product(country, subindustry)"]
    group_cartesian_product = [
        f"group_cartesian_product(country, {i})" for i in fieds]
    arr = []
    for i in df.index:
        # 判断地区，是欧亚全球时，走国家联合分组
        current_datas = fieds.copy()
        if df.loc[i]["settings"]["region"] in ["GLB", "EUR", "ASI"]:
            pass
            # current_datas += group_cartesian_product
        for op in group_ops:
            for gp in current_datas:
                arr.append({"code": f'{op}({df.loc[i]["code"]}, {gp})',
                            "settings": df.loc[i]["settings"]})

    print("group", len(arr))
    return pd.DataFrame(arr)


def when_third(df: pd.DataFrame, ) -> pd.DataFrame:
    open_events = ['group_rank(ts_std_dev(returns,60),sector)>0.7',
                   'ts_mean(volume,5)>ts_mean(volume,60)',
                   'ts_zscore(returns,60)<0.8',
                   'ts_std_dev(returns, 5)>ts_std_dev(returns, 20)',
                   'returns<0.09',
                   'ts_corr(close,volume,5)>0',
                   'ts_corr(close,volume,5)<0',
                   'returns>-0.09',
                   "abs(returns)<0.10"]
    # open_events=["rank(rp_css_business)>0.8","ts_rank(rp_css_business,22)>0.8",
    # "rank(vec_avg(nws3_scores_posnormscr))>0.8",
    # "ts_rank(vec_avg(nws3_scores_posnormscr),22)>0.8",]
    para: dict = model.yamldata.para
    open_events = para.get("ops") if para.get("ops") else open_events
    arr = []
    for i in df.index:
        for op in open_events:
            arr.append(
                {
                    "code": f'{op}?{df.loc[i]["code"]}:-1',
                    "settings": df.loc[i]["settings"],
                }
            )
    return pd.DataFrame(arr)


def t_decay(df: pd.DataFrame,  s=1, n=11, ) -> pd.DataFrame:
    para: dict = model.yamldata.para
    decays: list = para.get("decays")
    if not decays:
        decays = [2, 5, 10, 20]
    arr = []
    for i in df.index:
        for decay in decays:
            settings = df.loc[i]["settings"].copy()
            settings["decay"] = decay
            arr.append({"code": df.loc[i]["code"], "settings": settings})
    return pd.DataFrame(arr)


def t_neutralization(df: pd.DataFrame, now: str = None, ) -> pd.DataFrame:
    """用于便利各种中性化"""
    arrs = []
    neus = ["REVERSION_AND_MOMENTUM",
            "CROWDING", "MARKET", "SECTOR", "SUBINDUSTRY", "INDUSTRY", "FAST", "SLOW", "SLOW_AND_FAST",]
    if df.loc[df.index[0]]["settings"]["region"] in ["GLB", "EUR", "ASI", "AMR"]:
        neus += ["COUNTRY"]
    if df.loc[df.index[0]]["settings"]["region"] in ["GLB", "EUR", "ASI", "USA"]:
        neus += ["STATISTICAL"]
    now = df.loc[df.index[0]]["settings"]["neutralization"]
    neus = list(set(neus))
    neus = [i for i in neus if i != now]
    para: dict = model.yamldata.para
    print(para)
    neus = para.get("neus") if para.get("neus") else neus
    print(df.shape[0], neus)
    for neu in neus:
        for i in df.index:

            settings = df.loc[i]["settings"].copy()
            # if settings["neutralization"] == neu:
            #     continue
            settings["neutralization"] = neu
            arrs.append({"settings": settings, "code": df.loc[i]["code"]})
    print(len(arrs))
    return pd.DataFrame(arrs)
# 获取表信息
def fine_tune(df: pd.DataFrame):
    ops1 = ["log", "s_log_1p", "quantile", "rank",
            "winsorize", "hump", "abs", "sign", "sqrt", "ceiling", "arc_cos",
            "arc_sin", "arc_tan", "exp", "floor", "fraction", "purify", "round", "sigmoid", "sign", "tanh"]
    ops2 = ['left_tail({}, maximum = 0.98)',
            'right_tail({}, minimum = 0.03)', "signed_power({}, 0.7)", "signed_power({}, 1.5)", "truncate( {},maxPercent=0.01)"]
    arr = []
    for i in df.index:
        for op in ops1:
            arr.append({"code": f'{op}({df.loc[i]["code"]})',
                        "settings": df.loc[i]["settings"]})
        for op2 in ops2:
            print(f'{op2.format(df.loc[i]["code"])}')
            arr.append(
                {"code": f'{op2.format(df.loc[i]["code"])}',  "settings": df.loc[i]["settings"]})
    return pd.DataFrame(arr)

def find_all(df: pd.DataFrame, n=1):
    # 取前n个字段用于探索
    import inspect
    import data
    fname = inspect.getmembers(data, inspect.isfunction)
    fname = [i[0] for i in fname if "ts_" in i[0]]
    print(fname)
    # return
    ds = pd.DataFrame()
    for i in fname:
        ds = pd.concat([ds, getattr(data, i)(df.head(n), expore=True)])
    ds.reset_index(inplace=True, drop=True)
    print(ds.shape[0])
    print(ds["code"].tolist())
    return ds
