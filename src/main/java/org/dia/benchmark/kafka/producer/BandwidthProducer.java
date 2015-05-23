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
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

//kafka-benchmarking imports
import org.dia.benchmark.kafka.Configuration;
import org.dia.benchmark.kafka.BandwidthAggregator;

//General imports
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.Exception;
import java.lang.String;
import java.util.Properties;
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
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.BOOTSTRAP_SERVERS_CONFIG);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, config.BATCH_SIZE_CONFIG);
        props.put(ProducerConfig.ACKS_CONFIG, config.ACKS_CONFIG);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, config.BUFFER_MEMORY_CONFIG);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, config.COMPRESSION_TYPE_CONFIG);
        props.put(ProducerConfig.RETRIES_CONFIG, config.RETRIES_CONFIG);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, config.VALUE_SERIALIZER_CLASS_CONFIG);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, config.KEY_SERIALIZER_CLASS_CONFIG);
        props.put(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG, config.BLOCK_ON_BUFFER_FULL_CONFIG);

		/*
		//Settings that are available but not currently used/defaults are fine
		props.put(ProducerConfig,TIMEOUT_CONFIG,config.TIMEOUT_CONFIG);
		props.put(ProducerConfig.RECEIVE_BUFFER_CONFIG,config.RECEIVE_BUFFER_CONFIG);
		props.put(ProducerConfig.SEND_BUFFER_CONFIG,131072);
		props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG,1048576);
		props.put(ProducerConfig.LINGER_MS_CONFIG, config.LINGER_MS_CONFIG);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, config.CLIENT_ID_CONFIG);
		props.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, config.RECONNECT_BACKOFF_MS_CONFIG);
		props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, config.RETRY_BACKOFF_MS_CONFIG);
		props.put(ProducerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG, config.METRICS_SAMPLE_WINDOW_MS_CONFIG);
		props.put(ProducerConfig.METRICS_NUM_SAMPLES_CONFIG, config.METRICS_NUM_SAMPLES_CONFIG);
		props.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, config.METADATA_MAX_AGE_CONFIG);
		props.put(ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG,config.METADATA_FETCH_TIMEOUT_CONFIG);
		*/

        String name = config.TOPIC_PREFIX + config.TOPIC_INDEX;
        log.log(Level.INFO, String.format("\nSetting up producer on topic %s with %d ###### threads", name, config.THREADS_PER_TOPIC));
        producer = new KafkaProducer<byte[], byte[]>(props);
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
     *
     * @return count of messages produced
     */
    public  void act() {
        int length = config.MESSAGE_SIZE;
        byte[] key = {1};
        log.log(Level.INFO, String.format("Thread(%s) producing message of size: %d", Thread.currentThread().getName(),length));
        //Produce and send messages
        byte[] bytes = fillMessage(new byte[length]);
        ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<byte[], byte[]>(config.TOPIC_PREFIX + config.TOPIC_INDEX, key, bytes);
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