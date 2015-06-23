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
package org.dia.benchmark.kafka.consumer;

  

import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import org.dia.benchmark.kafka.BandwidthAggregator;
import org.dia.benchmark.kafka.configuration.Configuration;

/**
 * This consumer measures bandwidth as it consumes messages.
 *
 * @author starchmd
 */
public class BandwidthConsumer extends BandwidthAggregator {

    private static final Logger log = Logger.getLogger(BandwidthConsumer.class.getName());
    private ConsumerIterator<byte[], byte[]> iterator = null;

    @Override
    public void setup(Configuration config) {
        String name = config.get("topic.prefix")+config.get("topic.index");
        log.log(Level.INFO, String.format("Setting up consumer on topic %s with %d threads",name,config.get("threads.pre.topic")));
        ConsumerConfig conf = new ConsumerConfig(config.getProps());
        ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(conf);
        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumer.createMessageStreams(config.getTopicThreadCounts(Integer.parseInt(config.get("threads.per.topic"))));
        iterator = messageStreams.get(name).get(0).iterator();
    }
    /**
     * Consume a message, and return the count consumed.
     * @return count of messages consumed
     */
    public void  act() {
        log.log(Level.INFO,String.format("Thread(%s) consuming message",Thread.currentThread().getName()));
        if (iterator.hasNext()) {
            byte[] message = iterator.next().message();
            log.log(Level.FINE, String.format("Length of message recievd:"+message.length));
            synchronized (this) {
                count++;
            }
        }
    }
}
