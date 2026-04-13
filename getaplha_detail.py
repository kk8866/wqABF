
import sys
import api

qua = api.quant()
def deal_single_alpha_result(alpha_id) -> dict:
    """传入获取的数量和开始的时间参数
    返回df"""
    # 2025-07-04T22:52:30

    url = f"https://api.worldquantbrain.com/alphas/{alpha_id}"
    alphas_p = qua.wait_get(url).json()
    if not alphas_p:
        return
    print(alphas_p["regular"]["code"])
    alpha_is: dict[str, dict] = alphas_p["is"]
    alpl = []
    for i in alpha_is:
        
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
    print(alpl)
    checks = alphas_p["is"]["checks"]
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
if __name__ == "__main__":
    alpha_id = sys.argv[1]
    qua.login()
    deal_single_alpha_result(alpha_id)
    qua.sess.close()

#    # RR3bvMQ1