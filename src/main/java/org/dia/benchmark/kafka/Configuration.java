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
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang3.StringUtils;

/**
 * Class that holds configuration for the Kafka benchmarks.
 *
 * @author starchmd
 */
public class Configuration implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger log = Logger.getLogger(Configuration.class.getName());

    //Number of consumers
    public int CONSUMER_COUNT = 1;

    //Number of producers
    public int PRODUCER_COUNT = 1;

    public String USE_MONITOR = "true";
    //Number of topics
    public int TOPIC_COUNT = 1;
    public int TOPIC_INDEX = 0;
    public int THREADS_PER_TOPIC = 1;
    //Kafka config
    public String GROUP_ID = "ConsumerGroup";

    //Zookeeper properties
    public String ZOOKEEPER_CONNECT = "localhost:2181";
    public int ZOOKEEPER_SYNC_TIME_MS = 2000;
    public int ZOOKEEPER_SESSION_TIMEOUT_MS = 4000;
    
    //Shared prefix of all topics, topics will look like <TOPIC_PREFIX>#
    public String TOPIC_PREFIX = "TOPIC_";

    //Node configurations
    public String[] CONSUMER_NODES = {"localhost"};
    public String[] PRODUCER_NODES = {"localhost"};
    private String[] BROKER_NODES = {"localhost:9092","localhost:9093"};

    //ProducerConfig
<<<<<<< HEAD
    public int RECEIVE_BUFFER_CONFIG = 1610612736;  //Size of TCP receive buffer to use when receiving
    public int SEND_BUFFER_CONFIG = 1610612736;    //Size of TCP send buffer to use when sending
    public int MAX_REQUEST_SIZE_CONFIG = 1610612736;  //Max size of a request and record size.
    public long BUFFER_MEMORY_CONFIG = 1610612736;   //Total memory in bytes the producer can use to buffer records


=======
>>>>>>> 4bbfff28c56bb9e9b51a8e987a62a01ee8f63de0
    public String BOOTSTRAP_SERVERS_CONFIG = StringUtils.join(BROKER_NODES,",");
    public int BATCH_SIZE_CONFIG = 0;  //Batch size in bytes, default 16384
    public String ACKS_CONFIG = "0";   // 0 = none, 1 = write locally no waiting, all = full sync
    public int TIMEOUT_CONFIG = 30000; // Wait for ACK time in ms
<<<<<<< HEAD
    public String COMPRESSION_TYPE_CONFIG = "none";  //gzip, snappy also valid for batch compression
    public int RETRIES_CONFIG = 0; //Can affect ordering if >0
    public String VALUE_SERIALIZER_CLASS_CONFIG = "org.apache.kafka.common.serialization.ByteArraySerializer";
    public String KEY_SERIALIZER_CLASS_CONFIG = "org.apache.kafka.common.serialization.ByteArraySerializer";
    public Boolean BLOCK_ON_BUFFER_FULL_CONFIG = true;  //Setting to false will cause full memory to throw an error rather than block records
=======
    public long BUFFER_MEMORY_CONFIG = 33554432;   //Total memory in bytes the producer can use to buffer records
    public String COMPRESSION_TYPE_CONFIG = "none";  //gzip, snappy also valid for batch compression
    public int RETRIES_CONFIG = 0; //Can affect ordering if >0
    public String VALUE_SERIALIZER_CLASS_CONFIG = "org.apache.kafka.common.serialization.ByteArraySerializer";
    public String KEY_SERIALIZER_CLASS_CONFIG = "org.apache.kafka.common.serialization.StringSerializer";
    public Boolean BLOCK_ON_BUFFER_FULL_CONFIG = true;  //Setting to false will cause full memory to throw an error rather than block records
    public int RECEIVE_BUFFER_CONFIG = 32768;  //Size of TCP receive buffer to use when receiving
    public int SEND_BUFFER_CONFIG = 131072;    //Size of TCP send buffer to use when sending
    public int MAX_REQUEST_SIZE_CONFIG = 1048576;  //Max size of a request and record size.
>>>>>>> 4bbfff28c56bb9e9b51a8e987a62a01ee8f63de0
    public long LINGER_MS_CONFIG = 0;  //Delay in ms, to impose a spreading out of arriving records
    public String CLIENT_ID_CONFIG = "RequestSourceName";   //Unused now, string passed to server when making requests, for debugging
    public long RECONNECT_BACKOFF_MS_CONFIG = 10;  //ms delay before attempting to reconnect to a given host
    public long RETRY_BACKOFF_MS_CONFIG = 100; //ms delay before retrying a produce request to a given topic partition
    public long METRICS_SAMPLE_WINDOW_MS_CONFIG = 30000;   //ms metric collection window before overwrite
    public int METRICS_NUM_SAMPLES_CONFIG = 2; //Number of samples maintained to compute metrics
    public long METADATA_MAX_AGE_CONFIG = 300000;   //ms delay between forced metadata refreshes to discover new leaders/brokers/partitions
    public long METADATA_FETCH_TIMEOUT_CONFIG = 60000; //ms Delay prior to throwing an exception when no metadata is received
<<<<<<< HEAD

    //ConsumerConfig
    public String SOCKET_RECEIVE_BUFFER_BYTES = "1610612736"; //Socket recieve buffer for network requests
    public long SOCKET_TIMEOUT_MS = 30000;  //The socket timeout for network requests, see documentation for details
    public int FETCH_MESSAGE_MAX_BYTES = 1610612736;    //Number of bytes of messages to try fetch for each topic partition per fetch request
    public int NUM_CONSUMER_FETCHERS = 1;   //Number of fetch threads used to fetch data
    public boolean AUTO_COMMIT_ENABLE = true;   //Periodically commit to ZK the offset of messages already fetched
    public String AUTO_COMMITS_INTERVAL_MS = "60000";   //Frequency in ms that ofsets are committed to ZK
    public int QUEUED_MAX_MESSAGE_CHUNKS = 2;   //Max number of message chunks buffered.  Each chunk can be up to FETCH.MESSAGES.MAX.BYTES
    public int FETCH_MIN_BYTES = 805306368;    //Default is 1, currently 750MB.  Min amount of data to be fetched on a request - waits for data if not available
    public int FETCH_WAIT_MAX_MS = 1000;    //Longest server will block if min data is not available
    public String PARTITION_ASSIGNMENT_STRATEGY = "range";  // "range" or "roundrobbin" --Strategy for partition assignment to consumer streams

=======
>>>>>>> 4bbfff28c56bb9e9b51a8e987a62a01ee8f63de0

    //How often to report
    public int REPORTING_PERIOD = 1000;
    //Size of messages
    //public int MESSAGE_SIZE = 4096 * 1024*1024;
    public int MESSAGE_SIZE = 104857;//6

    public int RMI_REGISTRY_PORT = 1099;

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
                    log.log(Level.FINE,String.format("Overriding %s as array with %s",field.getName(),val));
                    field.set(this,val.split(":"));
                } else if (field.getType().equals(Integer.TYPE)) {
                    log.log(Level.FINE,String.format("Overriding %s as int with %d",field.getName(),Integer.parseInt(val)));
                    field.setInt(this,Integer.parseInt(val));
                } else if (field.getType().equals(Long.TYPE)) {
                    log.log(Level.FINE,String.format("Overriding %s as long with %d",field.getName(),Long.parseLong(val)));
                    field.setLong(this, Long.parseLong(val));
                } else {
                    log.log(Level.FINE,String.format("Overriding %s as string with %s",field.getName(),val));
                    field.set(this, val);
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
        props.put("auto.commit.interval.ms", AUTO_COMMITS_INTERVAL_MS);
        //props.put("auto.commit.interval.ms", "1000");
        props.put("socket.receive.buffer.bytes",SOCKET_RECEIVE_BUFFER_BYTES);
        return new ConsumerConfig(props);
    }
    /**
     * Get a map of topic to thread count
     * @param topic - number of topics
     * @return map of topic name to thread count
     */
    public Map<String,Integer> getTopicThreadCounts(int topic) {
        String name = TOPIC_PREFIX+TOPIC_INDEX;
        log.log(Level.FINE,String.format("Setting up topic: %s allowing %d.",name, THREADS_PER_TOPIC));
        Map<String,Integer> map = new HashMap<String,Integer>();
        map.put(name, THREADS_PER_TOPIC);
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
