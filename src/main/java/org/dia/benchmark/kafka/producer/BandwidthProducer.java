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
package org.dia.benchmark.kafka.producer;

//kafka imports
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
//kafka-benchmarking imports
import org.dia.benchmark.kafka.BandwidthAggregator;


import org.dia.benchmark.kafka.configuration.Configuration;

//General imports
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.Exception;
import java.lang.String;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.ConcurrentLinkedQueue;
/**
  * This producer creates a stream of messages according to parameters set in Configuration
  *
  * @author jwyngaard
 */
public class BandwidthProducer extends BandwidthAggregator {

    private static final Logger log = Logger.getLogger(BandwidthProducer.class.getName());
    Configuration config;

    private KafkaProducer<byte[], byte[]> producer = null;
    private ConcurrentLinkedQueue<Exception> sendExceptions = new ConcurrentLinkedQueue<Exception>();

    @Override
    public void setup(Configuration config) {
        String name = config.get("topic.prefix") + config.get("topic.index");
        log.log(Level.INFO, String.format("\nSetting up producer on topic %s with %s ###### threads", name, config.get("threads.per.topic")));
        producer = new KafkaProducer<byte[], byte[]>(config.getProps());
        this.config = config;
    }
    @Override
    public long stop() {
        long temp = super.stop();
        log.log(Level.FINE,"Stopping producer having read:"+temp);
        producer.close();
        return temp;
    }
    /**
     * Generate and send a message, close producer a message, and return 1 .
     */
    public  void act() {
        int length = Integer.parseInt(config.get("message.size"));
        byte[] key = {1};
        log.log(Level.INFO, String.format("Thread(%s) producing message of size: %d", Thread.currentThread().getName(),length));
        //Produce and send messages
        byte[] bytes = fillMessage(new byte[length]);
        ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<byte[], byte[]>(config.get("topic.prefix") + config.get("topic.index"), key, bytes);
        log.log(Level.FINE, "Actual produced message size:"+producerRecord.value().length);
        producer.send(producerRecord,new HasSent());
        logBackgroundExceptions();
    }
    /**
     * Reads and log all exceptions detected on background thread.
     */
    private void logBackgroundExceptions() {
        //Log all exceptions
        while(true) {
            try {
                detectException();
                break;
            } catch (Exception e) {
                log.log(Level.WARNING, "Exception thrown on 'sending' background thread.", e);
            }
        }
    }
    /**
     * Fills a message with bytes
     * @param message - message to fill bytes
     */
    private byte[] fillMessage(byte[] message) {
        InputStream is = null;
        try {
            //Read a message from /dev/urandom
            is = new BufferedInputStream(new FileInputStream("/dev/urandom"));
            is.read(message, 0, message.length);
        } catch (IOException e) {
            log.log(Level.WARNING, "Exception thrown while reading message from /dev/urandom", e);
        } finally {
            try {is.close();} catch (Exception ignore) {}
        }
        return message;
    }
    /**
     * Detects stored exceptions in the callback thread
     * @throws IOException - io exception detected on background thread
     */
    private synchronized void detectException() throws IOException {
        Exception e = this.sendExceptions.poll();
        if (e != null)
            throw new IOException(e);
    }
    /**
     * This class implements the callback for the Kafka Producer send message.
     * 
     * @author jwyngaard
     * @author starchmd - exception queuing
     */
    public class HasSent implements org.apache.kafka.clients.producer.Callback {
        @Override
        public void onCompletion(RecordMetadata metadata, Exception e) {
            if (e != null) {
                BandwidthProducer.this.sendExceptions.add(e);
                return;
            }
            synchronized (BandwidthProducer.this) {
                count++;
            }
        }
    }
}