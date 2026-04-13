import pymysql
import traceback
import threading


class cfg:
    db = pymysql.connect
    # 使用cursor()方法获取操作游标
    cursor = db.cursor


def inin_database(dbname):
    cfg.db = pymysql.connect(host='127.0.0.1',
                             user='root',
                             password='root',
                             database=dbname)
    cfg.cursor = cfg.db.cursor()


def create_table(tablename, fields):
    print("开始创建DB", tablename)
#    print(fields)
    sql = f'''CREATE TABLE IF NOT EXISTS {tablename}
      (
      {fields}
      );'''
    return exe_sql(sql)


def exe_sql(sql):
    for i in range(3):
        try:
            # 执行sql语句
            # print(sql)
            db_lock.acquire()
            cfg.db.begin()
            cfg.cursor.execute(sql)
        # 提交到数据库执行
            cfg.db.commit()
            db_lock.release()
            return True
        except pymysql.err.InterfaceError:
            print(sql)
            inin_database()
            db_lock.release()
        except Exception as e:
            db_lock.release()
            print(sql)
            traceback.print_exc()
            # 如果发生错误则回滚
            cfg.db.rollback()
            return False


def query(sql):
    try:
        cfg.cursor.execute(sql)
        return True, cfg.cursor.fetchall()
    except:
        print(sql)
        cfg.db = pymysql.connect(host='127.0.0.1',
                                 user='root',
                                 password='root',
                                 database='alphas')

        cfg.cursor = cfg.db.cursor()
        return False, ()
#  插入


def insert_db(name, data: dict):
    #    keys = ",".join(list(data.keys()))
    keys = ",".join([i[0] for i in data.items()])
    values = str(list([i[1] for i in data.items()]))[1:-1]
    sql = f'''INSERT INTO {name} ({keys})
   SELECT  {values} FROM DUAL 
   WHERE NOT EXISTS (
   SELECT id FROM {name} WHERE id = '{data["id"]}'
   );'''
    return exe_sql(sql)


def delete_table(table_name):
    sql = f"TRUNCATE TABLE {table_name};"
    return exe_sql(sql)


def close():
    cfg.db.close()


db_lock = threading.Lock()
