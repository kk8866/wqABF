import re
import json
import pandas as pd


def op_list(field=""):
    df = pd.read_csv("/data/check/os_alpha_ids.csv")
    df["dateSubmitted"] = df["dateSubmitted"].apply(lambda x: x.split("T")[0])
    df["dateSubmitted"] = pd.to_datetime(df["dateSubmitted"])
    df = df[df["type"] == "REGULAR"]
    date1 = pd.Timestamp("2026-04-01")
    df = df[df["dateSubmitted"] >= date1]
    df["code"] = df["code"].apply(lambda x: x.split("#")[0])
    s = "\n".join(df["code"].tolist())
    code = ""
    for i in s.split("\n"):
        code += "," + i.split("#")[0].split("//")[0]
    # code = ",".join(df["code"].tolist())
    code = (
        code.replace("*", " multiply ")
        .replace("+", " add ")
        # .replace("-", " subtract ")
        .replace("/", " divide ")
        .replace(">=", " greater_equal ")
        .replace(">", " greater ")
        .replace("<=", " less_equal ")
        .replace("<", " less ")
        .replace("?", " if_else ")
        .lower()
    )
    result = re.findall("\w+", code)
    result = list(set(result))
    print(field, "是否存在于当前字段中", field in result)
    with open("/data/check/operates.json", "r") as f:
        op = json.load(f)
    k = [i["name"] for i in op if "REGULAR" not in i.get("scope")]
    print(len(k), k)
    regular_op = [i["name"] for i in op if ("REGULAR" in i.get("scope") or "ALL" in i.get("scope"))]
    used_op = sorted([i for i in regular_op if i in result])
    print("="*20, "使用",len(used_op), "="*20)
    for i in range(0, len( used_op), 4):
        print("{:<50}".format("\t".join(used_op[i: i+4])))
    not_used_op = [i for i in regular_op if i not in used_op]
    print("="*20, "未使用",len(not_used_op), "="*20)
    not_used_op.sort()
    for i in range(0, len( not_used_op), 4):
        print("{:<100}".format("\t".join(not_used_op[i: i+4])))


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        op_list(sys.argv[1])
    else:
        op_list()
