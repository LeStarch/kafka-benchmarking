/*
Licensed to the Apache Software Foundation (ASF) under one or more contributor
license agreements.  See the NOTICE.txt file distributed with this work for
additional information regarding copyright ownership.  The ASF licenses this
file to you under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License.  You may obtain a copy of
the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
License for the specific language governing permissions and limitations under
the License.
 */
package org.dia.benchmark.kafka;

import kafka.consumer.ConsumerConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Class that holds configuration for the Kafka benchmarks.
 *
 * @author starchmd
 */
public class Configuration {
    //Number of consumers
    public int CONSUMER_COUNT = 1;

    //Number of producers
    public int PRODUCER_COUNT = 1;

    //Number of topics
    public int TOPIC_COUNT = 1;
    
    //Kafka config
    public String GROUP_ID = "CosumerGroup";

    //Zookeeper properties
    public String ZOOKEEPER_CONNECT = "http://localhost:8080";
    public int ZOOKEEPER_SYNC_TIME_MS = 200;
    public int ZOOKEEPER_SESSION_TIMEOUT_MS = 400;
    
    //Shared prefix of all topics, topics will look like <TOPIC_PREFIX>#
    public String TOPIC_PREFIX = "TOPIC_";

    /**
     * Produce Kafka consumer properties
     * @return Kafka Consumer Config
     */
    public ConsumerConfig getKafkaConsumerProperties() {
        Properties props = new Properties();
        props.put("zookeeper.connect", ZOOKEEPER_CONNECT);
        props.put("group.id", GROUP_ID);
        props.put("zookeeper.session.timeout.ms", ""+ZOOKEEPER_SESSION_TIMEOUT_MS);
        props.put("zookeeper.sync.time.ms", ""+ZOOKEEPER_SYNC_TIME_MS);
        props.put("auto.commit.interval.ms", "1000");
        return new ConsumerConfig(props);
    }
    /**
     * Get a map of topic to thread count
     * @param topic - number of topics
     * @param threadPreTopic - number of threads per topic
     * @return map of topic name to thread count
     */
    public Map<String,Integer> getTopicThreadCounts(int topic,int threadPreTopic) {
        Map<String,Integer> map = new HashMap<String,Integer>();
        for (int i = 0; i < topic; i++)
            map.put(TOPIC_PREFIX+i, threadPreTopic);
        return map;
    }
}
