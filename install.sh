#!/bin/sh
mvn clean; mvn install && tar -xzf target/kafka-benchmark-0.1-dist.tar.gz -C target
