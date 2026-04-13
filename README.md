# 用于WQ的自动化回测框架

tips： 主要用于在linux上的回测， windows及其他需要小改动

### 主要由四个文件夹构成

- case：用于存放yaml流程用例

- status：用于保存状态文件和原始datafields

- check：用于存放已有数据如已提交的alpha数据和拉取的pnls

- code：回测代码文件目录

- brain.txt：存放用邮箱和密码，格式["xxx@gamail.com", "WQ_password"]

### 开始

1. 首先需要创建mysql数据库，数据库名称为地区名，如USA，每次开始会自动创建以用例名称为表名的数据表

2. 下载数据库： `python load_data.py USA TOP3000 fundamental6 s202601` (默认下载dleay1，s202601随意取，方便查看使用的模板和回测的时间)

3. 修改测试文件，修改case目录下（非code文件夹下的case）对应的yaml文件

4. 执行回测：如`python main.py -c USA-s`,linux简化为: ./start.py USA

5. 查看回测状态及剩余额度: ./limit.sh USAs.logs返回信息参考虑`[root@instance-mrs18uwm code]# ./limit.sh USAs.log
   ../status/USA-1-TOP3000-fundamental6-s202601/status.json
   
   USA-1-TOP3000-fundamental6-s202601
   {
    "current": {
    "index": 100,
    "name": "ts_first", #回测项的名称
    "data": "",
    "start_time": "2026-04-11T16:46:14",
    "case": 0,
    "total": 1600, #本阶段总的回测数量
    "end_time": null,
    "finish": false
    },
    "total": {}
   }
   [2026-04-11 17:22:41] limit: 5000, remaining: 3489.0, reset: 67039`

6. 检查pc：`python check.py USA-1-TOP3000-fundamental6-s202601`

7. 提交：`python submit.py alpha_id` ppa及Super Alpha需要手动填写描述，再用代码提交

8. 探索模式：`python main.py -r USA -u  TOP3000 -d fumdamental6 -e True` 该模式将该数据集下alpha出货量最大的字段来探索在不同模板下的表现，会将data下以`ts_`开头的方法拼接一起来回测以达到达到探索多个模板。`[root@instance-mrs18uwm code]# cat ../case/expore-s.yaml
   data_name: USA-1-ILLIQUID_MINVOL1M-model165-dh4
   type: regular
   settings:
     "instrumentType": "EQUITY"
     "region": USA
     "universe": ILLIQUID_MINVOL1M
       # universe: TOP3000
     "delay": 1
     "decay": 1
     neutralization: SECTOR
     "truncation": 0.08
     "pasteurization": "ON"
     "unitHandling": "VERIFY"
     "nanHandling": "ON"
     "language": "FASTEXPR"
     "maxTrade": "OFF"
     "visualization": False
   slots_counts: 2
   one_slot_number: 10
   cases:
   
   - name: find_all
     para:
       sharpe: 1`

9. 
