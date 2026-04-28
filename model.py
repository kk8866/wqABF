import sys
import json
import datetime
from typing import Optional
from pydantic import BaseModel


# 用于存放一些数据类型

class settingsModel(BaseModel):

    instrumentType: str = "EQUITY"
    region: str = "USA"
    universe: str = "TOP3000"
    delay: int = 0
    decay: int = 1
    neutralization: str = "SECTOR"
    truncation: float = 0.08
    pasteurization: str = "ON"
    unitHandling: str = "VERIFY"
    nanHandling: str = "ON"
    language: str = "FASTEXPR"
    maxTrade: str = "OFF"
    visualization: bool = False


class yamldataModel(BaseModel):
    data_name: str = ""
    type: str = "regular"
    settings: settingsModel = settingsModel()
    cases: dict = {}
    slots_counts: int = 4
    one_slot_number:  int = 1
    para: dict = {}


class cfg:
    #     存放文件类型变量

    path: str = "../" # 路径变量
    project_path = ""
    check_path = path+"check/"
    test_path = path+"test/"
    log_name: str = "test.log"  # 日志变量
    status_name: str = "status.json"  # 过程状态文件
    deal_data: str = "deal.json"  # deal之后的文件
    result: str = "result.csv"  # 回测结果文件


class current_model(BaseModel):
    # 回测状态数据字段
    index: int = 0
    name: str = ""
    data: str = ""
    start_time: Optional[str] = None
    case: int = 0
    total: int = 0
    end_time: Optional[str] = None
    finish: bool = False


class status:
    current: current_model = current_model()
    total: dict = {}


def save_status(index):
    status.current.index = index
    # status.current.data = cfg.status_name.split("/")[-2]
    m = status.current.model_dump()
    m["data"] = cfg.status_name.split("/")[-2]
    print(m)
    with open(cfg.status_name, "w") as f:
        f.write(json.dumps(
            {"current": status.current.model_dump(), "total": status.total}, indent=2))


def load_status():
    with open(cfg.status_name, "r") as f:
        js = json.load(f)
        current = js.get("current", {})
        status.total = js["total"]
        status.current = status.current.model_validate(current)
        print(status.current)


class Super_settings(settingsModel):
    selectionLimit: int = 25
    selectionHandling: str = "POSITIVE"  
global yamldata
yamldata: yamldataModel = None
