# import json
# import pandas as pd
# import save_db
# with open("../qua/xxx.json","r") as f:
#   db_cfg = json.load(f)
# df = pd.read_csv("d1-1.csv")
# region = "USA"
# delay = 0
# columns = df.columns
# db_name = f"{region}{delay}"
# # print(df)
# fields = []
# for i in db_cfg[region]:
#   fields.append(f"{i} {db_cfg['keys'][i]}")
# save_db.create_table(db_name, fields=",".join(fields))

# df = df[[i for i in columns if i in db_cfg[region]]]
# for i in df.index:
#   data = df.loc[i].to_dict()
#   save_db.insert_db(db_name, data=data)
