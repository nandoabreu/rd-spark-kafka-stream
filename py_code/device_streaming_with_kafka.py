import json

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


def load_config(config_file: str) -> dict:
    with open(config_file) as f:
        return json.load(f)


def normalize_network_data(raw_data: (dict, str)) -> str:
    # PySpark UDFs cannot directly reference instance methods or class attributes that rely on the SparkContext
    normalized = {}

    if not isinstance(raw_data, dict):
        raw_data = json.loads(raw_data)

    # print(f'{raw_data=} ({type(raw_data)})')  # todo: debug-log
    for device, metrics in raw_data.items():
        if not isinstance(metrics, dict):
            # Unknown data - todo: log this decision
            continue

        inbound = metrics.get('in', 0.0)
        outbound = metrics.get('out', 0.0)

        if not (inbound or outbound):
            continue

        if any([device.startswith(prefix) for prefix in ('eth', 'enp')]):
            normalized['cxn'] = 'eth'

        elif any([device.startswith(prefix) for prefix in ('wlan', 'wlp')]):
            normalized['cxn'] = 'wlan'

        else:
            # Do not set if not eth or wlan
            continue

        if len(normalized) == 1:
            # cnx was set, but not the values until this loop
            normalized['in'] = inbound
            normalized['out'] = outbound

    return json.dumps(normalized or raw_data)  # Return raw if not normalized


class Streaming_ETL:
    def __init__(self, config: dict):
        self.spark_session_info = config['spark_session_info']
        self.kafka_information = config['kafka_information']
        self.database_info = config['database_info']

        # Initialize SparkSession
        self.spark = SparkSession.builder \
            .appName("Streaming_Devices_Data") \
            .config("spark.jars", self.spark_session_info['postgres_jars_path']) \
            .config("spark.jars.packages", self.spark_session_info['jars_package']) \
            .getOrCreate()


    def extract(self) -> DataFrame:
        # Read Kafka Stream and extract data schema
        input_df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_information['host']) \
            .option("subscribe", self.kafka_information['topic']) \
            .load()

        return input_df

    def process(self, input_df: DataFrame) -> DataFrame:
        # Select json values to further processing and normalization
        processed_df = input_df.select(
            F.col("key").cast("string").alias("key"),
            F.col("value").cast("string").alias("json_value")
        )

        return processed_df.fillna(0)

    def transform(self, processed_df: DataFrame) -> DataFrame:
        transformed = processed_df.select(
            'key',
            F.expr("get_json_object(json_value, '$.device')").cast('string').alias('device'),
            F.expr("get_json_object(json_value, '$.collected_at')").cast('timestamp').alias('collected_at'),

            # Extract valid CPU temperatures
            F.from_json(
                F.expr("get_json_object(json_value, '$.cpu.thermal_zones')"), 'array<float>'
            ).alias('cpu_zones'),
            F.expr("get_json_object(json_value, '$.cpu.sensors.CPU')").cast('float').alias('cpu_sensor'),
            F.from_json(
                F.expr("get_json_object(json_value, '$.cpu.sensors')"), 'map<string,float>'
            ).alias('cpu_any'),

            # Extract valid GPU temperatures
            F.expr("get_json_object(json_value, '$.gpu.nvidia')").cast('float').alias('gpu_nvidia'),
            F.expr("get_json_object(json_value, '$.gpu.sensors.edge')").cast('float').alias('gpu_edge'),
            F.from_json(
                F.expr("get_json_object(json_value, '$.gpu.sensors')"), 'map<string,float>'
            ).alias('gpu_any'),

            # Add not-yet-normalized network data
            F.expr("get_json_object(json_value, '$.net')").alias('net_data'),
        )

        # Fetch first valid CPU temperature
        transformed = transformed.withColumn('cpu_value', F.coalesce(
            F.when(
                F.col('cpu_zones').isNotNull(),
                F.expr('aggregate(cpu_zones, cast(0.0 as double), (acc, x) -> acc + x) / size(cpu_zones)')
            ),
            F.when(F.col('cpu_sensor').isNotNull(), F.col('cpu_sensor')),
            F.when(F.col('cpu_any').isNotNull(), F.expr('element_at(map_values(cpu_any), 1)')),
        ))

        # Fetch first valid GPU temperature
        transformed = transformed.withColumn('gpu_value', F.coalesce(
            F.when(F.col('gpu_nvidia').isNotNull(), F.col('gpu_nvidia')),
            F.when(F.col('gpu_edge').isNotNull(), F.col('gpu_edge')),
            F.when(F.col('gpu_any').isNotNull(), F.expr('element_at(map_values(gpu_any), 1)')),
        ))

        return transformed.select(
            F.col('key'),
            F.col('device'),
            F.col('collected_at'),
            F.col('cpu_value').alias('cpu_temp'),
            F.col('gpu_value').alias('gpu_temp'),
        ) \
            .withColumn('cpu_temp', F.round(F.col('cpu_temp'), 1)) \
            .withColumn('gpu_temp', F.round(F.col('gpu_temp'), 1))

    def write_to_postgres(self, batch_df, _):
        # Set config to write transformed incoming data
        batch_df.write \
            .format("jdbc") \
            .option("url", self.database_info['db_url']) \
            .option("dbtable", self.database_info['table']) \
            .option("user", self.database_info['user']) \
            .option("password", self.database_info['password']) \
            .option("driver", self.database_info['driver']) \
            .mode(self.database_info['mode']) \
            .save()

    def start_streaming(self):
        input_df = self.extract()
        processed_df = self.process(input_df)
        merged_df = self.transform(processed_df)

        # Write transformed dataframe (df_final) to Postgres
        streaming_query = merged_df.writeStream \
            .foreachBatch(self.write_to_postgres) \
            .outputMode("append") \
            .start()

        streaming_query.awaitTermination()


if __name__ == "__main__":
    config_filepath = 'config.json'

    config = load_config(config_file=config_filepath)
    if not config:
        raise ValueError(f'Config from {config_filepath} not fetched or parsed')

    process_data = Streaming_ETL(config=config)
    process_data.start_streaming()
