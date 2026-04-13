import sys
import requests
import api

qua = api.quant()


def deal_single_alpha_result(alpha_id) -> str:
    """传入获取的数量和开始的时间参数
    返回df"""
    # 2025-07-04T22:52:30

    url = f"https://api.worldquantbrain.com/alphas/{alpha_id}"
    alphas_p = qua.wait_get(url).json()
    if not alphas_p:
        return
    if "regular" in alphas_p:
        print(alphas_p["regular"]["code"])
    alpha_is: dict[str, dict] = alphas_p["is"]
    alpl = []
    for i in alpha_is:
        # alpl = []
        if isinstance(alpha_is[i], list):
            continue
        if isinstance(alpha_is[i], dict):
            subl = [i]
            sub = alpha_is[i]
            for j in sub:
                if j in ["bookSize", "pnl"]:
                    continue
                subl.append(": ".join([j, str(sub[j])]))
            print("  ".join(subl))

            continue
        alpl.append(": ".join([i, str(alpha_is[i])]))
    print("\n")
    print("\n".join(alpl))
    checks = alphas_p["is"]["checks"]
    for i in checks:
        if "pyramids" in i:
            print(i.get("pyramids"))
    _pass = [i for i in checks if i.get("result") == "PASS"]
    for i in _pass:
        name = i.get("name")
        limit = i.get("limit", 0)
        value = i.get("value", 0)
        print(f"\033[32m name:{name}, value: {value}, limit: {limit}.\033[0m")
    _fail = [i for i in checks if i.get("result") == "FAIL"]
    for i in _fail:
        name = i.get("name")
        limit = i.get("limit", 0)
        value = i.get("value", 0)
        print(f"\033[31m name:{name}, value: {value}, limit: {limit}.\033[0m")
    _warning = [i for i in checks if i.get("result") == "WARNING"]
    for i in _warning:
        name = i.get("name")
        limit = i.get("limit", 0)
        value = i.get("value", 0)
        print(f"\033[33m name:{name}, value: {value}, limit: {limit}.\033[0m")
    for i in checks:
        i["result"]
    checks = {
        i["name"].split(".")[0]: round(i.get("value", -4) - i.get("limit"), 4)
        for i in alphas_p["is"]["checks"]
        if "limit" in i
    }
    if "regular" in alphas_p:
        return alphas_p["regular"]["code"]
    return alphas_p["combo"]["code"]


def set_alpha_properties(
    s: requests.Session,
    alpha_id,
    name: str = None,
    color: str = None,
    selection_desc: str = "None",
    combo_desc: str = "None",
    reg_sc = "",
    tags: str = ["ace_tag"],
):
    """
    Function changes alpha's description parameters
    """
    

    params = {
        "color": color,
        "name": name,
        "tags": tags,
        "category": None,
        "regular": {"description": reg_sc},
        "combo": {"description": combo_desc},
        "selection": {"description": selection_desc},
    }
    response = s.patch(
        "https://api.worldquantbrain.com/alphas/" + alpha_id, json=params
    )
    print("设置alpha属性值：",response.status_code)
def get_years_sharpe(alpha_id):
    print(alpha_id)
    result = qua.wait_get("https://api.worldquantbrain.com/alphas/" +
    alpha_id + "/recordsets/yearly-stats").json()
    zc = [records[6] for records in result['records']]
    print(zc)
    return {alpha_id: zc}



def get_all():
    from concurrent.futures import ThreadPoolExecutor
    import pandas as pd
    
    df: pd.DataFrame = pd.read_csv("/data/check/os_alpha_ids.csv")
    
    qua.login()
    with ThreadPoolExecutor(max_workers=10) as executor:
            #   多线程获取alpha 每年year数据
            results = executor.map(
                lambda alpha_id: get_years_sharpe(alpha_id), df["id"].tolist())
    qua.sess.close()
    return list(results)
    
    
def get_code(code):
    import re

    x = re.findall(r"(\w+)\(", code)
    data = re.search(r"[\(| ](\w+)[ |,]", code).group(1)
    op = ",".join(sorted(x))
    return data, op


if __name__ == "__main__":
    alpha_id = sys.argv[1]
    qua.login()
    #code = deal_single_alpha_result(alpha_id)
    #data, op = get_code(code)
    #scr = f"""
     #   Idea: {data}\nRationale for data used: {data}\nRationale for operators used: {op}
    #   #  """
    # if len(sys.argv) > 2 and sys.argv[2] == "1":
    #     set_alpha_properti:.sess, alpha_id, reg_sc= scr)
    if "," in alpha_id:
        alpha_id = alpha_id.split(",")
    else:
        alpha_id = [alpha_id]
    for i in alpha_id:
        get_years_sharpe(i)
    qua.sess.close()

#    # RR3bvMQ1
