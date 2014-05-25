#!/bin/bash

# start nc
IPADDR=$HYRACKS_SERVER_IP
NODEID="asterix-004"

export JAVA_OPTS="-Djava.io.tmpdir=$HYRACKS_TMP_DIR -Xmx8G"

echo "Start NC..."
echo $HYRACKS_HOME/hyracks-server/target/hyracks-server-$HVERSION-binary-assembly/bin/hyracksnc -cc-host $HYRACKS_SERVER_IP -cc-port $HYRACKS_SERVER_CLUSTER_PORT -data-ip-address $IPADDR -node-id $NODEID -iodevices $HYRACKS_TMP_DIR -cluster-net-ip-address $IPADDR -result-ip-address $IPADDR

touch $HYRACKS_NC_LOG/$NODEID.log

$HYRACKS_HOME/hyracks-server/target/hyracks-server-$HVERSION-binary-assembly/bin/hyracksnc -cc-host $HYRACKS_SERVER_IP -cc-port $HYRACKS_SERVER_CLUSTER_PORT -data-ip-address $IPADDR -node-id $NODEID -iodevices $HYRACKS_TMP_DIR -cluster-net-ip-address $IPADDR -result-ip-address $IPADDR &> $HYRACKS_NC_LOG/$NODEID.log &

sleep 5
