
import random, os
import pandas as pd
# 从1到10范围内随机生成5个不重复的整数


def gene_ids(region="USA", delay=1, count=20, total=200) -> list[list]:
    df = pd.read_csv("/data/check/os_alpha_ids.csv")
    df["dateSubmitted"] = pd.to_datetime(
        df["dateSubmitted"].apply(lambda x: x.split("T")[0]))
    df = df[(df["dateSubmitted"] >= pd.Timestamp("2025-07-01"))
            & (df["region"] == region) & (df["delay"] == delay) &(df["type"]=="REGULAR")]
    alpha_ids = df["id"].tolist()
    result = []
    print("总数为：", len(alpha_ids))
    
    for x in range(total):
        # arr = {"code": 'color=="PURPLE"',
        #        "combo": '''stats = generate_stats(alpha);
        #        a = reduce_skewness(self_corr(stats.returns, 250));
        #        b = combo_a(alpha, nlength=250, mode="algo1");
        #        b/ts_std_dev(a, 252)''',
        #         "idlist": ""
        #        }

#         color=="PURPLE"	"stats = generate_stats(alpha);


        numbers = random.sample(range(0, len(alpha_ids)), count)
        
        l1 = [alpha_ids[i] for i in numbers]
        result.append(l1)
    df = pd.DataFrame({"idlist":result})
    selection = 'color=="PURPLE"'
    df["code"] = selection
    df["selection"] = selection
    df["combo"] = ('''stats = generate_stats(alpha);\n'''
               '''a = reduce_skewness(self_corr(stats.returns, 250));\n'''
               '''b = combo_a(alpha, nlength=250, mode="algo1");\n'''
               '''c = if_else(ts_std_dev(stats.returns, 500) > 0, a , -1);\n'''
               '''b/ts_std_dev(c, 252)''')
    print(df)
    if region == "EUR":
        uni = "TOP2500"
    if region == "ASI" or region =="GLB":
        uni = "MINVOL1M"
    if region == "USA":
        uni = "TOP3000"  
    if region == "IND":
        uni = "TOP500"    
    os.system(f"mkdir /data/status/{region}-{delay}-{uni}-SUPER-s2")
    df.to_csv(f"/data/status/{region}-{delay}-{uni}-SUPER-s2/data.csv")
    return df

if __name__ == "__main__":
    gene_ids(region="USA", delay=1, count=18, total=200)
"""
stats = generate_stats(alpha);
a = reduce_avg(self_corr(stats.returns, 250));
b = combo_a(alpha, nlength=250, mode="algo2");
c = if_else(ts_std_dev(stats.returns, 500) > 0, b , -1);
d = scale(b) + scale(c)
d=ts_std_dev(b,750)/ts_std_dev(d, 750);
d
"""
