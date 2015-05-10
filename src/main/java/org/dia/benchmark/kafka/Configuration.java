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

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
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
    public String ZOOKEEPER_CONNECT = "localhost:2181";
    public int ZOOKEEPER_SYNC_TIME_MS = 200;
    public int ZOOKEEPER_SESSION_TIMEOUT_MS = 400;
    
    //Shared prefix of all topics, topics will look like <TOPIC_PREFIX>#
    public String TOPIC_PREFIX = "TOPIC_";

    //Node configurations
    public String[] CONSUMER_NODES = {"localhost"};
    public String[] PRODUCER_NODES = {"localhost"};

    //How often to report
    public int REPORTING_PERIOD = 1000;
    //Size of messages
    public long MESSAGE_SIZE = 1024*1024;
    
    public int RMI_PORT = 8071;

    //What property to check for location of property file
    public static String PROPERTY_FILE_PROP = "PROPERTY_FILE";
    /**
     * Ctor -- overwrites configuration with properties
     * @param properties - properties used to build config
     * @throws IllegalAccessException - failed to set field
     */
    public Configuration(Properties properties) throws IllegalAccessException {
        override(properties);
    }
    /**
     * Overrides key and value inside this configuration
     * @param properties - properties used to override configuration
     * @throws IllegalAccessException - failed to set field
     */
    public void override(Properties properties) throws IllegalAccessException {
        Field[] fields = this.getClass().getFields();
        for (Field field : fields) {
            if (properties.containsKey(field.getName()) && !Modifier.isStatic(field.getModifiers())) {
                String val = properties.getProperty(field.getName());
                if (field.getType().isArray()) {
                    field.set(this,val.split(":"));
                } else if (field.getType().equals(Integer.TYPE)) {
                    field.setInt(this,Integer.parseInt(val));
                } else if (field.getType().equals(Long.TYPE)) {
                    field.setLong(this, Long.parseLong(val));
                } else {
                    field.set(this, properties.get(field.getName()));
                }
            }
        }
    }

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
    /**
     * Get properties in this order: ENV VARS then Command Line properties then
     * Properties File then Hardcoded Configuration
     * Properties file set in command line property: PROPERTY_FILE
     * @return constructed properties
     * @throws IOException - thrown on failure to read file 
     */
    public static Properties getProperties() throws IOException {
        String file = System.getProperty(PROPERTY_FILE_PROP);
        Properties properties = new Properties();
        properties.load(new FileInputStream(file));
        properties.putAll(System.getProperties());
        properties.putAll(System.getenv());
        return properties;
    }
}
