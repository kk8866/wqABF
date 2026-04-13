import pandas as pd
import save_db
import sys
def get_data_from_db(db_name):
    sql = f"SHOW COLUMNS FROM {db_name};"
    columns = save_db.query(sql)[1]
    columns = [i[0] for i in columns]
    # WHERE zeroCount<3
    sql = f"SELECT * FROM {db_name} WHERE zeroCount<3;"
    print(sql)
    data = save_db.query(sql)[1]
    df = pd.DataFrame(data, columns=columns)
    df.to_csv(f"/data/{db_name}.csv")
get_data_from_db(sys.argv[1])
