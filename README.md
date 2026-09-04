# Kafka Integration with Spring Boot

A complete Kafka producer and consumer implementation with Spring Boot.

## Features

-  Multi-broker producer (3 brokers)
-  Custom partitioner (`key % partitions`)
-  Single-thread high-level consumer
-  Multi-threaded consumer (one thread per partition)
-  Programmatic topic creation
-  Interactive console producer

## Requirements

- Java 22
- Kafka 4.3.1
- Maven

## Configuration
Broker ports: `localhost:9092, localhost:9094, localhost:9096`
this configuration is just for test now but with time and moving on to deeper concept 
the architecture and configuration would be more pro. 

## Running the Application

```bash
mvn spring-boot:run
