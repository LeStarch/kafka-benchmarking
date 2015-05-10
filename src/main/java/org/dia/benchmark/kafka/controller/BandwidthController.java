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
package org.dia.benchmark.kafka.controller;

import java.io.IOException;

import org.apache.commons.lang3.ArrayUtils;
import org.dia.benchmark.kafka.Aggregator;
import org.dia.benchmark.kafka.Configuration;
import org.dia.benchmark.kafka.consumer.BandwidthConsumer;
//import org.dia.benchmark.kafka.producer.BandwidthProducer;
import org.dia.benchmark.kafka.NetworkAggregator;

/**
 * This controller that runs the bandwidth-test.
 *
 * @author starchmd
 */
public class BandwidthController implements Aggregator {

    Aggregator[] consumers;
    Aggregator[] producers;
    long start = 0;
    Configuration config;
    //Checkpoint vars
    long lastTime = 0;
    long lastSent = 0;
    long lastRecv = 0;

    @Override
    public void setup(Configuration config) {
        this.config = config;
        consumers = new Aggregator[config.CONSUMER_NODES.length];
        producers = new Aggregator[config.PRODUCER_NODES.length];
        Aggregator[][] aggregators = { consumers, producers };
        //Setup consumers and producers over the network
        String[] nodes = config.CONSUMER_NODES;
        Class<?> clazz = BandwidthConsumer.class;
        for (Aggregator[] array : aggregators) {
            for (int i = 0; i < array.length; i++) {
            	try {
            		array[i] = new NetworkAggregator(config,clazz,nodes[i]);
            		array[i].setup(config);
            	} catch (Exception e) {
            		System.err.println("Error starting network aggregator: "+e);
            	}
            }
            nodes = config.PRODUCER_NODES;
//            clazz = BandwidthProducer.class;
        }
    }

    @Override
    public void start() {
        start = System.nanoTime();
        for (Aggregator agg : ArrayUtils.addAll(consumers,producers)) {
        	try {
        		agg.start();
        	} catch (IOException e) {
        		System.err.println("Error starting: "+e);
        	}
        }
    }

    @Override
    public long stop() {
        long sent = 0, recv = 0;
        for (Aggregator producer : producers) {
        	try {
        		sent += producer.stop();
        	} catch (IOException e) {
        		System.err.println("Error stopping: "+e);
        	}
        }
        for (Aggregator consumer : consumers) {
        	try {
        		recv += consumer.stop();
        	} catch (IOException e) {
        		System.err.println("Error stopping: "+e);
        	}
        }
        long end = System.nanoTime();
        long time = this.start - end;
        printCriticalData(time,this.config.MESSAGE_SIZE,sent,recv);
        return 0;
    }
    @Override
    public long count() {
        long sent = 0, recv = 0;
        for (Aggregator producer : producers) {
        	try {
        		sent += producer.count();
        	} catch (IOException e) {
        		System.err.println("Error retreiving counts: "+e);
        	}
        }
        for (Aggregator consumer : consumers) {
        	try {
        		recv += consumer.count();
        	} catch (IOException e) {
        		System.err.println("Error retreiving counts: "+e);
        	}      		
        }
        long end = System.nanoTime();
        long time = this.lastTime - end;
        printCriticalData(time,this.config.MESSAGE_SIZE,sent-lastSent,recv-lastRecv);
        return 0;
    }
    /**
     * Prints the desired test results to the screen.
     * @param time - length (in nano seconds) of time window
     * @param size - message size
     * @param sent - number of messages sent in time window
     * @param recv - number of messages received in time window
     */
    private static void printCriticalData(long time, long size, long sent, long recv) {
        System.out.println(String.format("Total time: %f Sent: %d Received: %d",time/1000000000,sent,recv));
        long lost = sent-recv;
        System.out.println(String.format("Size: %dB Lost: %d(%f%%)",size,lost,(double)lost/(double)recv));
        System.out.println("----------------------------------------------------");
        System.out.println(String.format("Bandwidth(sent): %fB/s Bandwidth(received): %fB/s",
                           (double)(size*sent)/(double)time,
                           (double)(size*recv)/(double)time));
    }
    /**
     * Test main program.
     * @param args - command line arguments
     */
    public static void main(String[] args) {
        final BandwidthController bc = new BandwidthController();
        final Configuration config = new Configuration();
        bc.setup(config);
        bc.start();
        //Catch CTRL-C
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                bc.stop();
            }
        });
        //Loop printing count every second
        while(true) {
            bc.count();
            try {
                Thread.sleep(config.REPORTING_PERIOD);
            } catch (InterruptedException e) {} 
        }
    }
}
