import sys
import case_db
from supert import quant

def get_super_all_ids(sid:str):
    url = f"https://api.worldquantbrain.com/alphas/{sid}/alphas?limit=50"
    qua = quant()
    qua.login()
    alphas_list = qua.wait_get(url).json().get("results")
    print(len(alphas_list))
    return [ra.get("id") for ra in alphas_list]


if __name__ == "__main__":
    res = get_super_all_ids(sys.argv[1])
    print(res)

