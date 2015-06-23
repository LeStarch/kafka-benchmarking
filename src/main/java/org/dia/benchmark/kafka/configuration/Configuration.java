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
package org.dia.benchmark.kafka.configuration;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Class that holds configuration for the Kafka benchmarks.
 *
 * @author starchmd
 * @author jwyngaard
 */
public class Configuration implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger log = Logger.getLogger(Configuration.class.getName());
    
    //What property to check for location of property file
    public static String PROPERTY_FILE_PROP = "PROPERTY_FILE";
    
    //Setup properties
    private static Properties props = new Properties();
    {
    	//Benchmarking properties
        props.put("message.size",""+1024*1024*1024);
        props.put("broker.nodes", "localhost:9092");
        props.put("producer.nodes","localhost");
        props.put("consumer.nodes","localhost");
        props.put("use.monitor","true");
        props.put("reporting.period","1000");
        props.put("rmi.registry.port","1099");
        props.put("topic.count","1");
        props.put("topic.index","0");
        props.put("threads.per.topic","1");       
        props.put("topic.prefix","TOPIC_"); //Prefix of topics, topics look like <topic.prefix>#
    	
        //Producer configs
        props.put("metadata.broker.list",props.get("broker.nodes")); //REAUIRED
        props.put("request.required.acks","0");
        props.put("request.timeout.ms","10000");
        props.put("producer.type","sync");
        props.put("serializer.class","org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("key.serializer.class","org.apache.kafka.common.serialization.ByteArraySerializer");
        //props.put("partitioner.class","kafka.producer.DefaultPartitioner");
        //props.put("compression.codec","none");
        //props.put("compressed.topics","null");
        //props.put("message.send.max.retries","3");
        //props.put("retry.backoff.ms","100");
        //props.put("topic.metadata.refresh.interval.ms","600000");
        //props.put("queue.buffering.max.ms","5000");
        //props.put("queue.buffering.max.messages","10000");
        //props.put("queue.enqueue.timeout.ms","-1");
        props.put("batch.num.messages","1");
        props.put("send.buffer.bytes",props.get("message.size"));
        //props.put("client.id","");  //DO NOT USE

        //Consumer config
        props.put("group.id","ConsumerGroup"); //REQUIRED
        props.put("zookeeper.connect","localhost:2181"); //REQUIRED
        //props.put("consumer.id","null"); 
        //props.put("socket.timeout.ms","30 * 1000");
        props.put("socket.receive.buffer.bytes",props.get("message.size"));
        props.put("fetch.message.max.bytes",props.get("message.size"));
        props.put("num.consumer.fetchers","1");
        //props.put("auto.commit.enable","TRUE");
        //props.put("auto.commit.interval.ms","60 * 1000");
        //props.put("queued.max.message.chunks","2");
        //props.put("rebalance.max.retries","4");
        //props.put("fetch.min.bytes","1");
        //props.put("fetch.wait.max.ms","100");
        //props.put("rebalance.backoff.ms","2000");
        //props.put("refresh.leader.backoff.ms","200");
        //props.put("auto.offset.reset","largest");
        //props.put("consumer.timeout.ms","-1");
        //props.put("exclude.internal.topics","TRUE");
        //props.put("partition.assignment.strategy","range");
        //props.put("client.id","group id value");  //DO NOT USE
        //props.put("zookeeper.session.timeout.ms","6000");
        //props.put("zookeeper.connection.timeout.ms","6000");
        //props.put("zookeeper.sync.time.ms","2000");
        //props.put("offsets.storage","zookeeper");
        //props.put("offsets.channel.backoff.ms","1000");
        //props.put("offsets.channel.socket.timeout.ms","10000");
        //props.put("offsets.commit.max.retries","5");
        //props.put("dual.commit.enabled","TRUE");
        //props.put("partition.assignment.strategy","range");
    }

    /**
     * Ctor -- overwrites configuration with properties
     * @param properties - properties used to build config
     * @throws IllegalAccessException - failed to set field
     */
    public Configuration(Properties properties) throws IllegalAccessException {
    }


    /**
     * Generates properties for Kafka configuration based on static class passed in holding the names
     * of all the available kafka configuration parameters.
     * @return properties object
     */
    public Properties getProps() {
    	return props;
    }
    
    public String get(String key) {
    	return props.getProperty(key);
    }
    
    /**
     * Get a map of topic to thread count
     * @param topic - number of topics
     * @return map of topic name to thread count
     */
    public Map<String,Integer> getTopicThreadCounts(int topic) {
        String name = props.getProperty("topic.prefix")+props.getProperty("topic.index");
        log.log(Level.FINE,String.format("Setting up topic: %s allowing %d.",name, props.getProperty("thread.per.topic")));
        Map<String,Integer> map = new HashMap<String,Integer>();
        map.put(name, Integer.parseInt(props.getProperty("thread.per.topic")));
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
        Properties properties = props;
        properties.load(new FileInputStream(file));
        properties.putAll(System.getProperties());
        Map<String,String> envs = System.getenv();
        for (String key : envs.keySet()) {
            envs.put(key.toLowerCase().replace("_","."),envs.get(key));
            envs.remove(key);
        }
        properties.putAll(envs);
        return properties;
    }
}
