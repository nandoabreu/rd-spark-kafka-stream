# Kafka-Based CPU Temperature Streaming

This project demonstrates a complete data streaming pipeline that reads real-time CPU temperature data from three devices—a Lenovo laptop, a Dell laptop, and a wireless router—using a built-in streaming service developed by [Fernando Abreu](https://github.com/nandoabreu).

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
In this project, **Grafana** is utilized to create a dynamic dashboard that visualizes the CPU temperature data, providing valuable insights into the performance of the devices over time. The dashboard pulls data directly from PostgreSQL, enabling the generation of informative charts and graphs that enhance readability and interpretation of the data.

To give you a glimpse of the dashboard’s layout and functionality, a snapshot link is available [here](https://snapshots.raintank.io/dashboard/snapshot/7g2rBArUuEgBcEynMROOmekywUPuCiqJ). Please note that this snapshot does not reflect live data due to privacy constraints associated with the database connection. However, it effectively showcases the current configuration and design of the dashboard, demonstrating how the CPU temperature metrics are visualized.

## Process Flow

1. **Extract**: Kafka streams CPU temperature records from the devices.
2. **Transform**: Spark Streaming processes and cleans the incoming data.
3. **Load**: The transformed data is stored in PostgreSQL.
4. **Visualize**: Grafana generates real-time visual dashboards from the stored data.
