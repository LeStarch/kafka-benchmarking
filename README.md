Kafka Benchmarking
==================

Ongoing work on the Streaming-OODT project (http://oodt.apache.org/ , https://issues.apache.org/jira/browse/OODT-698) has seen a need to test Kafka at a large scale. The goal of this work is to get 10Gb/s flowing through a Kafka system.

This has lead to the need for a Kafka benchmarking suite. That work is captured here.


Code Base
---------

Java is choosen as the language of choice for this project because OODT's primary language is Java and thus Java should be used to ensure compatibility with OODT.

To Build
--------
This project uses maven and pom.xml files to build. It compiles the Java source code, constructs a jar, and packages all dependencies into a .tar.gz archive.

- System configuration may be edited post build (configurations will be dynamically overridden if needed) but for ease of use edit src/main/resources/benchmark.properties before building.

- Then build:
```
./install
```

To Deploy
---------

- If not done prior to building, configure the system by extracting the generated archive (found in /target/ and edit the configuration file kafka-benchmarking-<version>/etc/benchmark.properties

- Copy the generated archive or extracted instance to all machines that will serve as producers or consumers

```
tar -xzvf target/*.tar.gz
```

To Run
---------
- Start the kafka broker (cluster or single node, including zookeeper)
- start a network stub on each producer and consumer
```
cd bin/
./network-stub.sh
```
- Either start a producer and consumer manually on each machine:
```
cd bin/
./consumer.sh 
```
or
```
cd bin/
./producer.sh 
```
Or initiate a controller which will start both as configured
```
cd bin/
./controller.sh 
```

License
-------
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


