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

//kafka-benchmarking imports
import org.dia.benchmark.kafka.Configuration;
import org.dia.benchmark.kafka.BandwidthAggregator;

//General imports
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
  * This producer creates a stream of messeges according to parameters set in Configuration
  *
  * @author jwyngaard
 */
public class BandwidthProducer extends BandwidthAggregator {

	private static final Logger log = Logger.getLogger(BandwidthProducer.class.getName());

	@Override
	public void setup(Configuration config){
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.BOOTSTRAP_SERVERS_CONFIG);
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, config.BATCH_SIZE_CONFIG);
		props.put(ProducerConfig.ACKS_CONFIG, config.ACKS_CONFIG);
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG,config.BUFFER_MEMORY_CONFIG);
		props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, config.COMPRESSION_TYPE_CONFIG);
		props.put(ProducerConfig.RETRIES_CONFIG, config.RETRIES_CONFIG);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,config.VALUE_SERIALIZER_CLASS_CONFIG);
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

		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        //Generate message
        int length = config.MESSAGE_SIZE;
        byte[] bytes = new byte[length];
        String bmessage = "";
        InputStream is = null;

        for (int i =0; i<config.NUM_MESSAGES; i++) {
            try {
                is = new BufferedInputStream(new FileInputStream("/dev/urandom"));
                int off = 0;
                is.read(bytes, off, length);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (is != null) {
                    try {
                        is.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }

            try {
                bmessage = new String(bytes, "UTF-8");
            } catch (Exception e) {
                e.printStackTrace();
            }

            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(config.TOPIC_PREFIX + config.TOPIC_INDEX, Integer.toString(i), bmessage);
            producer.send(producerRecord);
        }
        producer.close();
	}
    /**
     * Consume a message, and return the count produced.
     * @return count of messages produced
     */
    public long act() {
        log.log(Level.INFO,String.format("Thread(%s) producing message",Thread.currentThread().getName()));
            return 1;
        }
}
