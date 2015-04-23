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

    //Shared prefix of all topics, topics will look like <TOPIC_PREFIX>#
    public String TOPIC_PREFIX = "TOPIC_";
}
