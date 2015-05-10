#!/bin/sh
if [ -z ${JAVA_HOME} ]
then
    JAVA=`which java`
else
    JAVA=${JAVA_HOME}/bin/java
fi
${JAVA} -cp '../lib/*' -Djava.security.policy=../etc/security.policy -DPROPERTY_FILE=../etc/benchmark.properties org.dia.benchmark.kafka.controller.BandwidthController
