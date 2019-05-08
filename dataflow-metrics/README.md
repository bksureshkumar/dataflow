# Dataflow KafkaToBeamSQLToKafka

Produce Kafka messages in Avro format. Dataflow pipeline reads from Kafka topic run BeamSQL and write it back to another Kafka topic.


## KafkaAvroBeamSQLToKafka Pipeline

[KafkaAvroBeamSQLToKafka](https://github.com/bksureshkumar/dataflow/blob/master/dataflow-metrics/src/main/java/org/metrics/pipeline/KafkaAvroBeamSQLToKafka.java) -Kafka to BeamSQL to Kafka pipeline
</br>
[KafkaAvroProducer](https://github.com/bksureshkumar/dataflow/blob/master/dataflow-metrics/src/main/java/org/metrics/pipeline/KafkaAvroProducer.java) - Kafka Avro producer
## Getting Started

### Requirements

* Java 8
* Maven 3

### Building the Project

Build the entire project using the maven compile command.
```sh
mvn clean && mvn compile
```
