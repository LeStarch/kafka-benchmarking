#!/bin/sh
if [ -z ${JAVA_HOME} ]
then
    JAVA=`which java`
else
    JAVA=${JAVA_HOME}/bin/java
fi
${JAVA} -cp '../lib/*' org.dia.benchmark.kafka.RmiAggregatorServer
