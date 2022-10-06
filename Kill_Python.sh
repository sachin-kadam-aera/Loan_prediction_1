#!/bin/sh

echo "Enter script name"
read scriptname

echo "python processes before kill"
ps -ef|grep -i $scriptname|head -5
sleep 5

for i in `ps -ef|grep -i $scriptname|awk '{print $2}'`; do kill -9 $i; done
echo "python processes after kill"
ps -ef|grep -i python|head -5
