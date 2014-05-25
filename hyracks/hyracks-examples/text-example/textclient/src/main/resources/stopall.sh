#!/bin/bash
JPS_PATH="jps"

PID=`$JPS_PATH | grep CCDriver | awk '{print $1}'`

echo "Killing CC thread: "$PID
kill -9 $PID
echo "CC[$PID] is killed."


PID=`$JPS_PATH | grep NCDriver | awk '{print $1}'`

echo "Killing NC thread: "$PID
kill -9 $PID
echo "NC[$PID] is killed."

#PID=`ps -ujarodwen | grep iostat | awk '{print $1}'`

#echo "Killing iostat thread: "$PID
#kill -9 $PID
#echo "iostat[$PID] is killed."

#PID=`ps -ujarodwen | grep free | awk '{print $1}'`

#echo "Killing free thread: "$PID
#kill -9 $PID
#echo "free[$PID] is killed."