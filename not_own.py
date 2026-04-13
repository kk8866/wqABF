arr = []
for i in range(25):
    for j in range(25):
        exp = (f'''not(own)&&( (turnover>{0.01+i/100}&&turnover<{0.02+i/100})  ||( turnover>{0.26+j/100}\n'''
        f''' &&turnover<{0.27+j/100}))&&(operator_count<=7)&&\n'''
        '''(prod_correlation<0.7)/sigmoid(turnover)*sigmoid(prod_correlation)*log(long_count)\n'''
        '''&&self_correlation<0.5 && universe=="MINVOL1M"  && operator_count<7  && operator_count>1''')
        arr.append({"code": exp})
import pandas as pd
df = pd.DataFrame(arr)
df["selection"] = df["code"]
df["idlist"] = "[]"
df["combo"] = '''b = combo_a(alpha, nlength=500, mode="algo1");
signed_power(
b/ts_std_dev(b , 252),0.2)'''
df.to_csv("/data/status/GLB-1-MINVOL1M-SUPER-notown-s1/data.csv")