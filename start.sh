#!/bin/bash
region=$(echo "$1"|cut -c1-3 )

echo python main.py $region-s 
nohup python main.py -c $region-s >"$region"s.log &
echo

