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

import kafka.javaapi.consumer.ConsumerConnector;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.dia.benchmark.kafka.Aggregator;
import org.dia.benchmark.kafka.Configuration;

/**
 * This consumer measures bandwidth as it consumes messages.
 *
 * @author starchmd
 */
public class BandwidthConsumer implements Aggregator {

    Child[] children = null;
    Executor executor;

    @Override
    public void setup(Configuration config) {
        
        children = new Child[config.CONSUMER_COUNT];
        executor = Executors.newFixedThreadPool(children.length);
        
        ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config.getKafkaConsumerProperties());
        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumer.createMessageStreams(config.getTopicThreadCounts(config.TOPIC_COUNT, config.CONSUMER_COUNT/config.TOPIC_COUNT));
        for (int i = 0; i < children.length; i++) {
            children[i] = new Child();
            children[i].setup(messageStreams.get(config.TOPIC_PREFIX+(i/config.TOPIC_COUNT)).get(i%config.TOPIC_COUNT));
        }
    }

    @Override
    public void start() {
        for (int i = 0; i < children.length; i++) {
            executor.execute(children[i]);
        }
    }

    @Override
    public long stop() {
        long total = 0;
        for (int i = 0; i < children.length; i++) {
            children[i].stop();
            total += children[i].getMessageCount();
        }
        return total;
    }
    /**
     * A child consumer, allows for multi-threaded consumption.
     * @author starchmd
     */
    private class Child implements Runnable {
        long messages = -1;
        boolean stop = false;
        
        ConsumerIterator<byte[], byte[]> iterator = null;

        @Override
        public void run() {
            //Set termination flag safely
            boolean terminate = false;
            synchronized (this) {
                terminate = stop;
            }
            while (!terminate) {
                long count = consume();
                synchronized (this) {
                    terminate = stop;
                    messages += count;
                }
            }
        }
        /**
         * Setup this child.
         * @param stream - KafkaStream to read from
         */
        public void setup(KafkaStream<byte[], byte[]> stream) {
            iterator = stream.iterator();
        }
        /**
         * Return the count of messages
         * @return number of messages read by this child
         */
        public synchronized long getMessageCount() {
            return messages;
        }

        /**
         * Stop this consuming child
         */
        public synchronized void stop() {
            stop = true;
        }
        /**
         * Consume a message, and return the count consumed.
         * @return count of messages cosumed
         */
        private long consume() {
            if (iterator.hasNext()) {
                iterator.next();
                return 1;
            }
            return 0;
        }
    }
}
