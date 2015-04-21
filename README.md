Kafka Benchmarking
==================

Ongoing work on the Streaming-OODT project (http://oodt.apache.org/ , https://issues.apache.org/jira/browse/OODT-698) has seen a need to test Kafka at a large scale. The goal of this work is to get 10Gb/s flowing through a Kafka system.

This has lead to the need for a Kafka benchmarking suite. That work is captured here.


Code Base
---------

Java is choosen as the language of choice for this project because OODT's primary language is Java and thus java should be used to ensure compatibility with OODT.
