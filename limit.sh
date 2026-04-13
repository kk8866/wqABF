#/bin/bash

if [ ! -f "$1" ]; then
	echo "不存在 $1"
	exit 
fi
grep -a .json "$1" |awk '{print $(NF)}'|uniq
echo 
p=$(grep -a .json "$1" |tail -n1 |awk -F "/" '{print $(NF-1)}')
echo $p
cat "../status/$p/status.json"
echo 
grep -a limit "$1" |tail -n1
