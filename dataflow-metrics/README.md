# Dataflow KafkaToBeamSQLToKafka

Produce Kafka messages in Avro format. Dataflow pipeline reads from Kafka topic run BeamSQL and write it back to another Kafka topic.


## KafkaAvroBeamSQLToKafka Pipeline

[KafkaAvroBeamSQLToKafka](src/main/java/com/google/cloud/pso/pipeline/KafkaAvroBeamSQLToKafka.java) -


## Getting Started

### Requirements

* Java 8
* Maven 3

### Building the Project

Build the entire project using the maven compile command.
```sh
mvn clean && mvn compile
```
