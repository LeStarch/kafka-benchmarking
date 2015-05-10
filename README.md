Kafka Benchmarking
==================

Ongoing work on the Streaming-OODT project (http://oodt.apache.org/ , https://issues.apache.org/jira/browse/OODT-698) has seen a need to test Kafka at a large scale. The goal of this work is to get 10Gb/s flowing through a Kafka system.

This has lead to the need for a Kafka benchmarking suite. That work is captured here.


Code Base
---------

Java is choosen as the language of choice for this project because OODT's primary language is Java and thus java should be used to ensure compatibility with OODT.

To Build
--------

This project uses maven and pom.xml files to build. It compiles the java source code, constructs a jar, and packages all dependencies into a .tar.gz archive.

```
mvn install
```

To Deploy
---------

To deploy, untar the archive and run within the extracted directory.

```
tar -xzf target/*.tar.gz
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


