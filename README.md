# streaming-comparison

The streaming comparision between Databricks ( Spark Streaming ), Apache Flink and Quix streaming.

## Description

Codebase to the attached article:


## Howto build generators

Have the librdkafka (with the c++ bindings) installed on your system.


Run the command in both of the cpp-generator-plaintext and cpp-generator-json directories:
```
g++ -c ./producer.cpp &&  g++ producer.o -lrdkafka -lrdkafka++
```