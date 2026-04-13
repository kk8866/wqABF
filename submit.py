import time
import sys
import json
import requests

# 用于提交alpha


def post(s: requests.Session, alpha_id: str):
    submit_url = f"https://api.worldquantbrain.com/alphas/{alpha_id}/submit"
    print(submit_url)
    res = s.post(submit_url)
    status = res.status_code
    print("post状态值为", status)
    if status == 403:
        print("alpha已提交成功")
        return 1
    if status == 404:
        print("alpha不存在")
        return 1
    if status == 400:
        print("alpha提交中")
    if status < 300:
        print("post成功")
    return 0
#    pass


def submit_alpha(s: requests.Session, alpha_id: str):
    submit_url = f"https://api.worldquantbrain.com/alphas/{alpha_id}/submit"
    t1 = time.time()
    print(submit_url)
    # 提交
    count = 0
#   while count<5:
    res = post(s, alpha_id)
    if res:
        print("本次不检查")
        return 1
    count = 0
    while count < 1000:
        print(int(time.time()-t1))
        res = s.get(submit_url)
        print(res.status_code)
        if res.status_code == 200:
            retry = res.headers.get('Retry-After', 0)
            print(retry)
            time.sleep(5)
            if retry == 0:
                print(res.json())
                print(f"submit alpha: {alpha_id} sucess")
                break
        elif res.status_code == 404:
            if post(s, alpha_id):
                return True
        else:
            break
        count += 1


class cfg:
    with open('../brain.txt') as f:
        username, password = json.load(f)


if __name__ == '__main__':
    s = requests.Session()
    s.auth = (cfg.username, cfg.password)
    response = s.post('https://api.worldquantbrain.com/authentication')
    print(response.status_code)
    submittable_alphas = [sys.argv[1]]
    for alpha_id in submittable_alphas:
        submit_alpha(s, alpha_id)
