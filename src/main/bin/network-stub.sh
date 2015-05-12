#!/bin/sh
if [ -z ${JAVA_HOME} ]
then
    JAVA=`which java`
    RMIR=`which rmiregistry`
else
    JAVA=${JAVA_HOME}/bin/java
    RMIR=${JAVA_HOME}/bin/rmiregistry
fi
# Set RMI Registry Port to arg or env var if possible
if [ ! -z ${1} ]
then
    RPORT=${1}
elif [ ! -z ${RMI_REGISTRY_PORT} ]
then
    RPORT=${RMI_REGISTRY_PORT}
fi
CLASSPATH="../lib/*" ${RMIR} ${RPORT} &
PID=$!
trap "kill ${PID}" 2 9 15
sleep 2
${JAVA} -cp '../lib/*' -Djava.security.policy=../etc/security.policy -DPROPERTY_FILE=../etc/benchmark.properties org.dia.benchmark.kafka.RmiAggregatorServer
