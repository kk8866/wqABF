#!/bin/bash

# 用于检查多个数据表
result=$(./grep.sh $1 $2)
for i in $result;
do
	python check.py $i
done
