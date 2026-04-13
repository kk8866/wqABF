# !/bin/bash
echo python main.py -r $1 -u $2 -d $3 -e True
nohup python main.py -r $1 -u $2 -d $3 -e True >fall.log &