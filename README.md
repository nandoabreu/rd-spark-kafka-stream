# Kafka-Based CPU Temperature Streaming

This project demonstrates a complete data streaming pipeline that reads real-time CPU temperature data from three devices—a Lenovo laptop, a Dell laptop, and a wireless router—using a built-in streaming service developed by Fernando Abreu.

The streaming data is processed using **Apache Kafka** and **Apache Spark Streaming**. The pipeline writes the processed data to **PostgreSQL**, which is then visualized using **Grafana** for real-time dashboard insights.

## Project Overview

### Data Source:
The data comes from a custom streaming service that captures CPU temperature metrics from three devices. These temperature readings are continuously streamed and published to Kafka topics.

### Data Ingestion:
**Apache Kafka** is used to handle the real-time ingestion of CPU temperature data. Each device streams its temperature records to Kafka topics, which **Spark Streaming** subscribes to for further processing.

### Data Processing:
**Apache Spark Streaming** reads the raw temperature data from Kafka, transforms it by parsing JSON records, and cleans the data for further analysis. During the transformation phase, device-specific fields such as CPU temperature and timestamps are extracted.

### Data Storage:
After processing, the transformed data is written into a **PostgreSQL** database for persistence.

### Visualization:
Finally, **Grafana** is used to create a real-time dashboard that visualizes the CPU temperature data, offering insights into the performance of the devices over time. Grafana pulls the data directly from PostgreSQL to generate charts and graphs for better readability.

## Process Flow

1. **Extract**: Kafka streams CPU temperature records from the devices.
2. **Transform**: Spark Streaming processes and cleans the incoming data.
3. **Load**: The transformed data is stored in PostgreSQL.
4. **Visualize**: Grafana generates real-time visual dashboards from the stored data.
