#!/bin/bash

# setup java environment
export JAVA_OPTS="-Djava.rmi.server.hostname=$HYRACKS_SERVER_IP -Xdebug -Djava.io.tmpdir=$HYRACKS_TMP_DIR -Xrunjdwp:transport=dt_socket,address=7001,server=y,suspend=n -Xmx256m"

# start cc
echo "Start CC..."
echo $HYRACKS_HOME/hyracks-server/target/hyracks-server-$HVERSION-binary-assembly/bin/hyrackscc -client-net-ip-address $HYRACKS_SERVER_IP -cluster-net-ip-address $HYRACKS_SERVER_IP -client-net-port $HYRACKS_SERVER_CLIENT_PORT -cluster-net-port $HYRACKS_SERVER_CLUSTER_PORT -cc-root $HYRACKS_CC_LOG

$HYRACKS_HOME/hyracks-server/target/hyracks-server-$HVERSION-binary-assembly/bin/hyrackscc -client-net-ip-address $HYRACKS_SERVER_IP -cluster-net-ip-address $HYRACKS_SERVER_IP -client-net-port $HYRACKS_SERVER_CLIENT_PORT -cluster-net-port $HYRACKS_SERVER_CLUSTER_PORT -cc-root $HYRACKS_CC_LOG &> $HYRACKS_CC_LOG/asterix-cc.log &

sleep 5
