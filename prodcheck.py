# https://api.worldquantbrain.com/alphas/7R56JlQ/check
import requests
import logging
import time
import sys
import json


class cfg:
    data_path = "/storage/emulated/0/qua/check/" \
        if sys.platform != "linux"\
        else "/data/check/"
    with open(data_path + '../brain.txt') as f:
        username, password = json.load(f)
    sess: requests.Session = None


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


def prod_corr_check(sess: requests.Session, alpha_id):
    url = "https://api.worldquantbrain.com/alphas/" + alpha_id + "/correlations/prod"
    while True:
        result = sess.get(url)
        if result.headers.get("Retry-After", 0) == 0:
            try:
               # print(result.json())
                return result.json()["max"]

            except Exception as e:
                print(e)
                break
        time.sleep(1)
    return None


cfg.sess = sign_in(cfg.username, cfg.password)
# df = pd.read_csv(cfg.data_path+"20250904-101953-check.csv", index_col=0)
# df = df[df["selfcheck"]<0.5]
# # b0Qxjdr
# # proc = get_check_submission(cfg.sess, "b0kQ716")
# # print(proc)
# print(df)
# arr = ['dM5zPYE', 'rqKVeOo', 'xLA5Vqg', 'OL0j7vv', 'glkkLrK', 'G2rzWGZ', 'YVPo7go', 'Ndxm6Qo', 'obmjzAn', '9eq5vpq', 'NdxXYwe', 'Z8PGK9Q', 'kwq6Knz', '0Vm5Q21', '3Kq5lrg', 'xLOqY1W', 'EjO33YJ', 'PR0m7Gx', 'Z8YzKO1', '9eZ182d', 'Apj166d', 'njpqepw', 'obm0ad5']
# df["pc"] = 1.0
# for i in arr:
#     proc = prod_corr_check( i)
#     print(i, proc)
#     df.at[i, "pc"] = proc
# now = datetime.datetime.now()
# formatted_date = now.strftime("%Y%m%d-%H%M%S")
# df.to_csv(cfg.data_path+f"{formatted_date}-checkpc.csv")
# # cfg.sess.close()
