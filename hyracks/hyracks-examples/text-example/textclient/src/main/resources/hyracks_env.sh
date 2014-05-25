#!/bin/bash

# setup the source code path
export HYRACKS_HOME=/Volumes/Home/GitPier/hyracks/hyracks
export HVERSION=0.2.11-SNAPSHOT
export HYRACKS_TMP_DIR=/Volumes/Home/hyracks_tmp/io_tmp
export HYRACKS_OUTPUT_DIR=/Volumes/Home/hyracks_tmp/output_tmp
export HYRACKS_LOG_DIR=/Volumes/Home/hyracks_tmp/log_tmp

IFS=','
for i in $HYRACKS_TMP_DIR; do
echo "[INFO] Check tmp dir: " $i;
if [ -d $i ]; then
echo "[INFO] Clean up tmp dir: " $i;
find $i | grep ".waf" | xargs rm;
else
echo "[INFO] Cretae tmp dir: " $i;
mkdir -p $i;
fi
done

if [ ! -d $HYRACKS_OUTPUT_DIR ]; then
echo "[INFO] Create output dir: " $HYRACKS_OUTPUT_DIR;
mkdir -p $HYRACKS_OUTPUT_DIR;
fi

if [ ! -d $HYRACKS_LOG_DIR ]; then
echo "[INFO] Create log dir: " $HYRACKS_LOG_DIR;
mkdir -p $HYRACKS_LOG_DIR;
fi

#if [ -d "/mnt/data/sda/space/jarodwen/tmp" ]; then
#find /mnt/data/sda/space/jarodwen/tmp | grep ".waf" | xargs rm
#else
#mkdir -p /mnt/data/sda/space/jarodwen/tmp;
#fi

# setup the server ip address
export HYRACKS_SERVER_IP=127.0.0.1
export HYRACKS_SERVER_CLIENT_PORT=1098
export HYRACKS_SERVER_CLUSTER_PORT=1099

LOG_DATE=`date +%Y%m%d`
LOG_TIME=`date +%H%M`

# setup the log directory
export HLOG_DIR="$HYRACKS_LOG_DIR/$LOG_DATE/$LOG_TIME"

if [ ! -d $HLOG_DIR ]; then
mkdir -p $HLOG_DIR
fi

export HYRACKS_CC_LOG=$HLOG_DIR/cc_log

if [ ! -d $HYRACKS_CC_LOG ]; then
mkdir -p $HYRACKS_CC_LOG
fi

export HYRACKS_NC_LOG=$HLOG_DIR/nc_log

if [ ! -d $HYRACKS_NC_LOG ]; then
mkdir -p $HYRACKS_NC_LOG
fi

# start profiling
#iostat -d -x sda -k 5 > $HYRACKS_NC_LOG/sda_io.log &
#iostat -d -x sdb -k 5 > $HYRACKS_NC_LOG/sdb_io.log &
#iostat -d -x sdc -k 5 > $HYRACKS_NC_LOG/sdc_io.log &
#iostat -d -x sdd -k 5 > $HYRACKS_NC_LOG/sdd_io.log &
#free -s 5 > $HLOG_DIR/mem.log &

$HYRACKS_HOME/hyracks-examples/text-example/textclient/src/main/resources/startcc.sh

$HYRACKS_HOME/hyracks-examples/text-example/textclient/src/main/resources/startnc.sh

# deploy application
#echo "--- Deploy application"

#echo "connect to \"$HYRACKS_SERVER_IP:$HYRACKS_SERVER_CLIENT_PORT\";" > app.hcli
#echo "create application textapp \"$HYRACKS_HOME/hyracks-examples/text-example/textapp/target/textapp-$HVERSION-app-assembly.zip\";" >> app.hcli
#echo "" >> app.hcli

#$HYRACKS_HOME/hyracks-cli/target/hyracks-cli-"$HVERSION"-binary-assembly/bin/hyrackscli < app.hcli

#rm -rf app.hcli

#echo ""