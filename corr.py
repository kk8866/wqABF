
import os, sys
import pandas as pd
import api

# 计算两个alpha的相关性，用法： python corr.py alpha_id1,alpha_id2
def concat_pnls(arr: list):
    qua = api.quant()
    qua.login()
    df = pd.DataFrame()
    for i in arr:
        path = f"../check/pnls/{i}.pkl"
        if os.path.exists(path):
            dft = pd.read_pickle(path).set_index("Date")
        else:
             dft = qua._get_alpha_pnl(i).set_index("Date")
             
        df = pd.concat([df, dft], axis=1)
    df = df - df.ffill().shift(1)
    # 保留四年时间的增量pnls
    df: pd.DataFrame = df[pd.to_datetime(df.index)
        > pd.to_datetime(df.index).max() - pd.DateOffset(years=4)]
    qua.sess.close()
    return df

if __name__ == "__main__":
    
    ids = sys.argv[1].split(",")
    df = concat_pnls(ids)
    print(df.corr())